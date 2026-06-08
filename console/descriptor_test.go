package console

import (
	"bytes"
	"encoding/json"
	"io"
	"strings"
	"testing"
)

func TestRenderCommandListFormatsAndFiltersDefinitions(t *testing.T) {
	definitions := []Definition{
		{Name: "serve", Description: "Start HTTP API server"},
		{Name: "queue:work", Description: "Run worker", Aliases: []string{"work"}},
		{Name: "queue:restart", Description: "Restart workers"},
		{Name: "hidden:debug", Description: "Hidden", Hidden: true},
	}

	var txt bytes.Buffer
	if err := RenderCommandList(&txt, definitions, CommandListOptions{
		Description: "Prismgo commands",
		Output:      OutputOptions{ANSI: true},
	}); err != nil {
		t.Fatalf("RenderCommandList txt returned error: %v", err)
	}
	txtOutput := txt.String()
	for _, want := range []string{"Prismgo commands", "Usage:", "Available Commands:", "queue", "queue:work", "[work] Run worker", "serve"} {
		if !strings.Contains(txtOutput, want) {
			t.Fatalf("txt output missing %q: %q", want, txtOutput)
		}
	}
	if strings.Contains(txtOutput, "hidden:debug") {
		t.Fatalf("txt output included hidden command: %q", txtOutput)
	}

	var raw bytes.Buffer
	if err := RenderCommandList(&raw, definitions, CommandListOptions{Raw: true}); err != nil {
		t.Fatalf("RenderCommandList raw returned error: %v", err)
	}
	if got := raw.String(); !strings.Contains(got, "queue:restart Restart workers") {
		t.Fatalf("raw output = %q", got)
	}

	var short bytes.Buffer
	if err := RenderCommandList(&short, definitions, CommandListOptions{Short: true, Namespace: "queue"}); err != nil {
		t.Fatalf("RenderCommandList short returned error: %v", err)
	}
	if got := short.String(); got != "queue:restart\nqueue:work\n" {
		t.Fatalf("short output = %q", got)
	}
}

func TestRenderCommandListJSONMarkdownAndUnsupportedFormats(t *testing.T) {
	definitions := []Definition{
		{Name: "queue:work", Description: "Run worker", Aliases: []string{"work"}},
		{Name: "serve", Description: "Start HTTP API server"},
	}

	var jsonOut bytes.Buffer
	if err := RenderCommandList(&jsonOut, definitions, CommandListOptions{Format: "json", Namespace: "queue"}); err != nil {
		t.Fatalf("RenderCommandList json returned error: %v", err)
	}
	var payload struct {
		Namespace string              `json:"namespace"`
		Commands  []CommandDescriptor `json:"commands"`
	}
	if err := json.Unmarshal(jsonOut.Bytes(), &payload); err != nil {
		t.Fatalf("json output did not decode: %v", err)
	}
	if payload.Namespace != "queue" || len(payload.Commands) != 1 || payload.Commands[0].Name != "queue:work" {
		t.Fatalf("unexpected json payload: %+v", payload)
	}

	var markdown bytes.Buffer
	if err := RenderCommandList(&markdown, definitions, CommandListOptions{Format: "markdown", AppName: "Prismgo"}); err != nil {
		t.Fatalf("RenderCommandList markdown returned error: %v", err)
	}
	if got := markdown.String(); !strings.Contains(got, "# Prismgo") || !strings.Contains(got, "_(aliases: work)_") {
		t.Fatalf("markdown output = %q", got)
	}

	if err := RenderCommandList(io.Discard, definitions, CommandListOptions{Format: "xml"}); err == nil {
		t.Fatal("expected unsupported format error")
	}

	var quiet bytes.Buffer
	if err := RenderCommandList(&quiet, definitions, CommandListOptions{Output: OutputOptions{Quiet: true}}); err != nil {
		t.Fatalf("RenderCommandList quiet returned error: %v", err)
	}
	if quiet.Len() != 0 {
		t.Fatalf("quiet output = %q", quiet.String())
	}
}

func TestStyledAndIOWriterHelpers(t *testing.T) {
	if got := Styled("value", StyleInfo, OutputOptions{}); got != "value" {
		t.Fatalf("Styled without ANSI = %q", got)
	}
	if got := Styled("", StyleInfo, OutputOptions{ANSI: true}); got != "" {
		t.Fatalf("Styled empty = %q", got)
	}
	if got := Styled("value", "missing", OutputOptions{ANSI: true}); got != "value" {
		t.Fatalf("Styled unknown = %q", got)
	}
	if got := Styled("value", StyleInfo, OutputOptions{ANSI: true}); got != "\x1b[32mvalue\x1b[0m" {
		t.Fatalf("Styled ANSI = %q", got)
	}

	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	ioo := NewIOWithOutputOptions(strings.NewReader(""), stdout, stderr, OutputOptions{ANSI: true})
	if OutputWriter(ioo) != stdout {
		t.Fatal("expected OutputWriter to expose stdout")
	}
	if ErrorOutputWriter(ioo) != stderr {
		t.Fatal("expected ErrorOutputWriter to expose stderr")
	}
	if !OutputOptionsForIO(ioo).ANSI {
		t.Fatal("expected OutputOptionsForIO to expose configured options")
	}
	if OutputWriter(nil) != io.Discard {
		t.Fatal("expected nil OutputWriter to fall back to io.Discard")
	}
	if ErrorOutputWriter(nil) != io.Discard {
		t.Fatal("expected nil ErrorOutputWriter to fall back to io.Discard")
	}
}
