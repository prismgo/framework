package routine

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/prismgo/framework/container"
	goexception "github.com/prismgo/framework/exception"
)

type reportedException struct {
	err    error
	fields map[string]any
}

func captureReports(t *testing.T) <-chan reportedException {
	t.Helper()
	registry := container.NewContainer()
	container.SetProvider(func() *container.Container { return registry })
	t.Cleanup(func() { container.SetProvider(nil) })

	reports := make(chan reportedException, 4)
	handler := goexception.New(goexception.WithPanicStack(false))
	handler.Reporters = append(handler.Reporters, func(_ any, err error, fields map[string]any) {
		var copied map[string]any
		if fields != nil {
			copied = make(map[string]any, len(fields))
			for key, value := range fields {
				copied[key] = value
			}
		}
		reports <- reportedException{err: err, fields: copied}
	})
	if err := registry.Instance("exception.handler", handler, container.WithCloseGroup(container.CloseGroupReporting)); err != nil {
		t.Fatalf("bind exception handler: %v", err)
	}
	return reports
}

func waitReport(t *testing.T, reports <-chan reportedException) reportedException {
	t.Helper()
	select {
	case report := <-reports:
		return report
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for exception report")
		return reportedException{}
	}
}

func TestTaskReportsReturnedErrorAndCallsOnError(t *testing.T) {
	reports := captureReports(t)
	runErr := errors.New("run failed")
	callbacks := make(chan error, 1)

	Task(context.Background(), func(context.Context) error {
		return runErr
	}).
		Fields(map[string]any{"component": "field-component", "job": "sync"}).
		Component("routine-component").
		Name("sync").
		OnError(func(err error) {
			callbacks <- err
		}).
		Go()

	report := waitReport(t, reports)
	if !errors.Is(report.err, runErr) {
		t.Fatalf("reported err = %v, want %v", report.err, runErr)
	}
	if report.fields["component"] != "routine-component" {
		t.Fatalf("component = %v, want routine-component", report.fields["component"])
	}
	if report.fields["routine"] != "sync" {
		t.Fatalf("routine = %v, want sync", report.fields["routine"])
	}
	if report.fields["job"] != "sync" {
		t.Fatalf("job = %v, want sync", report.fields["job"])
	}

	select {
	case got := <-callbacks:
		if !errors.Is(got, runErr) {
			t.Fatalf("callback err = %v, want %v", got, runErr)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for OnError callback")
	}
}

func TestTaskRecoversPanicReportsAndCallsOnPanic(t *testing.T) {
	reports := captureReports(t)
	callbacks := make(chan error, 1)

	Task(context.Background(), func(context.Context) error {
		panic("boom")
	}).
		Component("horizon").
		Name("supervisor.loop").
		OnPanic(func(err error) {
			callbacks <- err
		}).
		Go()

	report := waitReport(t, reports)
	if report.err == nil || report.err.Error() != "boom" {
		t.Fatalf("reported err = %v, want original panic value", report.err)
	}
	if report.fields["component"] != "horizon" {
		t.Fatalf("component = %v, want horizon", report.fields["component"])
	}
	if report.fields["routine"] != "supervisor.loop" {
		t.Fatalf("routine = %v, want supervisor.loop", report.fields["routine"])
	}

	select {
	case got := <-callbacks:
		if got == nil || !strings.Contains(got.Error(), "boom") {
			t.Fatalf("callback err = %v, want boom", got)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for OnPanic callback")
	}
}

func TestGoUsesDefaultReportingWithoutFields(t *testing.T) {
	reports := captureReports(t)
	runErr := errors.New("quick failure")

	Go(context.Background(), func(context.Context) error {
		return runErr
	})

	report := waitReport(t, reports)
	if !errors.Is(report.err, runErr) {
		t.Fatalf("reported err = %v, want %v", report.err, runErr)
	}
	if len(report.fields) != 0 {
		t.Fatalf("fields = %#v, want empty", report.fields)
	}
}

func TestNilTaskReportsError(t *testing.T) {
	reports := captureReports(t)

	Task(context.Background(), nil).Go()

	report := waitReport(t, reports)
	if !errors.Is(report.err, errNilTask) {
		t.Fatalf("reported err = %v, want %v", report.err, errNilTask)
	}
}
