package cmd

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"

	queuecommand "github.com/prismgo/framework/cmd/queue"
	"github.com/prismgo/framework/console"
	"github.com/spf13/cobra"
)

func TestCommandSignatureAndDescriptionMethods(t *testing.T) {
	queue := queuecommand.NewWorkCommand()
	cron := NewCronCommand(nil, nil)
	serve := NewServeCommand(fakeHTTPServerFactory(&fakeHTTPRegistrars{}))

	definitions := []struct {
		name       string
		definition *console.Definition
	}{
		{"queue", queue.Definition()},
		{"cron", cron.Definition()},
		{"serve", serve.Definition()},
	}
	for _, testCase := range definitions {
		if testCase.definition.Name == "" || testCase.definition.Description == "" {
			t.Fatalf("expected %s definition to be non-empty", testCase.name)
		}
	}
}

func TestListCommandShowsVisibleDefinitionsAndAliases(t *testing.T) {
	cmd := NewListCommand(func() []console.Definition {
		return []console.Definition{
			{Name: "serve", Description: "Start server", Aliases: []string{"s"}},
			{Name: "hidden", Description: "Hidden command", Hidden: true},
		}
	})
	stdout := &bytes.Buffer{}
	ctx := console.NewCommandContext(context.Background(), cmd, *cmd.Definition(), fakeInput{}, console.NewIO(strings.NewReader(""), stdout, io.Discard), nil, &cobra.Command{Use: "list"})

	if err := cmd.Handle(ctx); err != nil {
		t.Fatalf("Handle returned error: %v", err)
	}
	output := stdout.String()
	if !strings.Contains(output, "serve") || !strings.Contains(output, "[s] Start server") {
		t.Fatalf("list output missing visible command alias summary: %q", output)
	}
	if strings.Contains(output, "hidden") {
		t.Fatalf("list output included hidden command: %q", output)
	}
}

func TestListCommandWritesToCustomIOOutputWriter(t *testing.T) {
	cmd := NewListCommand(func() []console.Definition {
		return []console.Definition{{Name: "serve", Description: "Start server"}}
	})
	stdout := &bytes.Buffer{}
	ioo := &writerBackedIO{
		IO:  console.NewIO(strings.NewReader(""), io.Discard, io.Discard),
		out: stdout,
	}
	ctx := console.NewCommandContext(context.Background(), cmd, *cmd.Definition(), fakeInput{
		options: map[string]string{"format": "json"},
	}, ioo, nil, &cobra.Command{Use: "list"})

	if err := cmd.Handle(ctx); err != nil {
		t.Fatalf("Handle returned error: %v", err)
	}
	if !strings.Contains(stdout.String(), `"name": "serve"`) {
		t.Fatalf("custom IO output writer did not receive list output: %q", stdout.String())
	}
}

type writerBackedIO struct {
	console.IO
	out io.Writer
}

func (ioo *writerBackedIO) Output() io.Writer {
	return ioo.out
}

func TestListCommandHandlesNilDefinitions(t *testing.T) {
	cmd := NewListCommand(nil)
	if definitions := cmd.commandDefinitions(); definitions != nil {
		t.Fatalf("commandDefinitions = %v, want nil", definitions)
	}
}

func TestBridgeConstructorsReturnCommands(t *testing.T) {
	if factories := MakeCommandFactories(); len(factories) == 0 {
		t.Fatal("expected generator command factories")
	}

	commands := []console.Command{
		NewMigrateInstallCommand(),
		NewMigrateStatusCommand(),
		NewMigrateCommand(),
		NewMigrateRollbackCommand(),
		NewMigrateResetCommand(),
		NewMigrateRefreshCommand(),
		NewMigrateFreshCommand(),
		NewDBSeedCommand(),
	}
	for _, cmd := range commands {
		if cmd == nil || cmd.Definition().Name == "" {
			t.Fatalf("expected bridge constructor to return command with definition, got %#v", cmd)
		}
	}
}
