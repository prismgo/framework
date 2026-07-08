package console

import (
	"bytes"
	"strings"
	"testing"

	"github.com/mgutz/ansi"
)

func TestTerminalIOLaravelStyleMessages(t *testing.T) {
	t.Setenv("FORCE_COLOR", "1")
	t.Setenv("NO_COLOR", "")
	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	io := NewIO(strings.NewReader(""), stdout, stderr)

	io.Info("info")
	io.Comment("comment")
	io.Question("question")
	io.Success("success")
	io.Line("plain")
	io.Line("warning", "warning")
	io.Line("unknown", "missing")
	io.Warn("warn")
	io.Error("error")

	wantStdout := ansi.Color("info", "green") + "\n" +
		ansi.Color("comment", "yellow") + "\n" +
		ansi.Color("question", "black:cyan") + "\n" +
		ansi.Color("success", "white:green") + "\n" +
		"plain\n" +
		ansi.Color("warning", "yellow") + "\n" +
		"unknown\n"
	if got := stdout.String(); got != wantStdout {
		t.Fatalf("stdout = %q, want %q", got, wantStdout)
	}

	wantStderr := ansi.Color("warn", "yellow") + "\n" +
		ansi.Color("error", "white:red") + "\n"
	if got := stderr.String(); got != wantStderr {
		t.Fatalf("stderr = %q, want %q", got, wantStderr)
	}
}

func TestTerminalIOAlertUsesCommentBlock(t *testing.T) {
	t.Setenv("FORCE_COLOR", "1")
	t.Setenv("NO_COLOR", "")
	stdout := &bytes.Buffer{}
	io := NewIO(strings.NewReader(""), stdout, &bytes.Buffer{})

	io.Alert("careful")

	border := strings.Repeat("*", len("careful")+12)
	want := ansi.Color(border, "yellow") + "\n" +
		ansi.Color("*     careful     *", "yellow") + "\n" +
		ansi.Color(border, "yellow") + "\n"
	if got := stdout.String(); got != want {
		t.Fatalf("stdout = %q, want %q", got, want)
	}
}

func TestTerminalIOAutoDisablesDecorationForPlainBuffers(t *testing.T) {
	t.Setenv("FORCE_COLOR", "")
	t.Setenv("NO_COLOR", "")
	t.Setenv("TERM", "")
	stdout := &bytes.Buffer{}
	io := NewIO(strings.NewReader(""), stdout, &bytes.Buffer{})

	io.Info("info")

	if got := stdout.String(); got != "info\n" {
		t.Fatalf("stdout = %q, want undecorated info", got)
	}
}

func TestTerminalIOAskConfirmChoiceAndSecret(t *testing.T) {
	stdin := strings.NewReader("hello\ny\n2\nsecret\n")
	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	io := NewIO(stdin, stdout, stderr)

	answer, err := io.Ask("name")
	if err != nil {
		t.Fatalf("Ask returned error: %v", err)
	}
	if answer != "hello" {
		t.Fatalf("Ask answer = %q, want hello", answer)
	}

	confirmed, err := io.Confirm("continue", false)
	if err != nil {
		t.Fatalf("Confirm returned error: %v", err)
	}
	if !confirmed {
		t.Fatal("expected Confirm to return true")
	}

	choice, err := io.Choice("pick", []string{"first", "second"})
	if err != nil {
		t.Fatalf("Choice returned error: %v", err)
	}
	if choice != "second" {
		t.Fatalf("Choice result = %q, want second", choice)
	}

	secret, err := io.Secret("token")
	if err != nil {
		t.Fatalf("Secret returned error: %v", err)
	}
	if secret != "secret" {
		t.Fatalf("Secret result = %q, want secret", secret)
	}
}

func TestTerminalIONewLineChoiceWithOptionsAndAnticipateFallback(t *testing.T) {
	stdin := strings.NewReader("1,3\nwrong\n2\nanticipated\n")
	stdout := &bytes.Buffer{}
	ioo := NewIO(stdin, stdout, &bytes.Buffer{})

	ioo.NewLine(3)
	choices, err := ioo.ChoiceWithOptions("pick many", []string{"first", "second", "third"}, ChoiceOptions{Multiple: true})
	if err != nil {
		t.Fatalf("ChoiceWithOptions multiple returned error: %v", err)
	}
	if strings.Join(choices, ",") != "first,third" {
		t.Fatalf("choices = %v, want first,third", choices)
	}

	choice, err := ioo.ChoiceWithOptions("pick once", []string{"first", "second"}, ChoiceOptions{Attempts: 2})
	if err != nil {
		t.Fatalf("ChoiceWithOptions retry returned error: %v", err)
	}
	if len(choice) != 1 || choice[0] != "second" {
		t.Fatalf("choice = %v, want second", choice)
	}

	anticipated, err := ioo.Anticipate("name", []string{"anticipated"})
	if err != nil {
		t.Fatalf("Anticipate returned error: %v", err)
	}
	if anticipated != "anticipated" {
		t.Fatalf("Anticipate = %q, want anticipated", anticipated)
	}
	if !strings.HasPrefix(stdout.String(), "\n\n\n") {
		t.Fatalf("NewLine output = %q, want three newlines", stdout.String())
	}
}

func TestTerminalIOChoiceWithOptionsAttemptsExhausted(t *testing.T) {
	ioo := NewIO(strings.NewReader("bad\n"), &bytes.Buffer{}, &bytes.Buffer{})
	if _, err := ioo.ChoiceWithOptions("pick", []string{"first"}, ChoiceOptions{}); err == nil || !strings.Contains(err.Error(), "invalid choice") {
		t.Fatalf("ChoiceWithOptions error = %v, want invalid choice", err)
	}
}

func TestChoiceWithOptionsMultipleRetrySuccess(t *testing.T) {
	stdin := strings.NewReader("wrong1\nwrong2\nfirst\n")
	ioo := NewIO(stdin, &bytes.Buffer{}, &bytes.Buffer{})

	choice, err := ioo.ChoiceWithOptions("pick", []string{"first", "second"}, ChoiceOptions{Attempts: 3})
	if err != nil {
		t.Fatalf("ChoiceWithOptions returned error: %v", err)
	}
	if len(choice) != 1 || choice[0] != "first" {
		t.Fatalf("choice = %v, want first", choice)
	}
}

func TestTerminalIOTableAndProgress(t *testing.T) {
	stdout := &bytes.Buffer{}
	io := NewIO(strings.NewReader(""), stdout, stdout)
	if err := io.Table([]string{"Command", "Description"}, [][]string{{"serve", "Start HTTP API server"}}); err != nil {
		t.Fatalf("Table returned error: %v", err)
	}
	progress := io.Progress(2)
	progress.Advance(1)
	progress.Advance(1)
	progress.Finish()

	output := stdout.String()
	if !strings.Contains(output, "Command") || !strings.Contains(output, "serve") {
		t.Fatalf("expected table output, got %q", output)
	}
	if !strings.Contains(output, "[2/2]") {
		t.Fatalf("expected progress output, got %q", output)
	}
}
