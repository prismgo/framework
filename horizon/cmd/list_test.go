package cmd

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/prismgo/framework/console"
)

func TestListCommandDisplaysRunningMasterMachines(t *testing.T) {
	// 需求背景：batch bulk dispatch contract 将 horizon:list 收口为 Laravel 对齐的运行中 master machines 视图，
	// 不再展示静态配置、trim 配置或 queue targets。
	runtime := &listRuntime{masters: []MasterState{{
		ID:              "master-1",
		PID:             1234,
		SupervisorCount: 2,
		Status:          "running",
	}}}
	command := NewListCommand(func(context.Context) (Runtime, error) {
		return runtime, nil
	})
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	ctx := console.NewCommandContext(
		context.Background(),
		command,
		console.CloneDefinition(*command.Definition()),
		nil,
		console.NewIO(strings.NewReader(""), &stdout, &stderr),
		nil,
		nil,
	)

	if err := command.Handle(ctx); err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	output := stdout.String()
	for _, want := range []string{"Name", "PID", "Supervisors", "Status", "master-1", "1234", "running"} {
		if !strings.Contains(output, want) {
			t.Fatalf("output missing %q:\n%s", want, output)
		}
	}
	for _, forbidden := range []string{"Environment:", "Store:", "Trim:", "redis:default"} {
		if strings.Contains(output, forbidden) {
			t.Fatalf("horizon:list must not display static config %q:\n%s", forbidden, output)
		}
	}
}

func TestListCommandRequiresLoader(t *testing.T) {
	// 设计原因：命令必须显式注入 Runtime loader；缺失 loader 时返回稳定错误，避免静默使用全局资源。
	command := NewListCommand(nil)
	var stdout bytes.Buffer
	ctx := console.NewCommandContext(
		context.Background(),
		command,
		console.CloneDefinition(*command.Definition()),
		nil,
		console.NewIO(strings.NewReader(""), &stdout, &stdout),
		nil,
		nil,
	)
	if err := command.Handle(ctx); !errors.Is(err, ErrRuntimeNotConfigured) {
		t.Fatalf("expected missing loader error, got %v", err)
	}
}

func TestListCommandReportsNoRunningMachines(t *testing.T) {
	// 测试目的：没有运行中的 master heartbeat 时输出稳定文本，便于脚本和文档断言。
	command := NewListCommand(func(context.Context) (Runtime, error) {
		return &listRuntime{}, nil
	})
	var stdout bytes.Buffer
	ctx := console.NewCommandContext(context.Background(), command, console.CloneDefinition(*command.Definition()), nil, console.NewIO(strings.NewReader(""), &stdout, &stdout), nil, nil)

	if err := command.Handle(ctx); err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if !strings.Contains(stdout.String(), "No machines are running.") {
		t.Fatalf("expected no machines output, got %q", stdout.String())
	}
}

func TestListCommandReturnsErrRuntimeNotConfiguredForNilRuntime(t *testing.T) {
	command := NewListCommand(func(context.Context) (Runtime, error) {
		return nil, nil
	})
	ctx := console.NewCommandContext(context.Background(), command, console.CloneDefinition(*command.Definition()), nil, console.NewIO(strings.NewReader(""), &bytes.Buffer{}, &bytes.Buffer{}), nil, nil)

	if err := command.Handle(ctx); !errors.Is(err, ErrRuntimeNotConfigured) {
		t.Fatalf("expected nil runtime error, got %v", err)
	}
}

type listRuntime struct {
	surfaceRuntime
	masters []MasterState
	memory  bool
}

func (r *listRuntime) UsesMemoryStore() bool { return r.memory }
func (r *listRuntime) Masters(context.Context, time.Time) ([]MasterState, error) {
	return append([]MasterState(nil), r.masters...), nil
}
