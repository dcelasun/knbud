package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func TestHelp(t *testing.T) {
	var output bytes.Buffer
	command := newCommand(strings.NewReader(""), &output, &output)
	if err := command.Run(context.Background(), []string{"knbud", "--help"}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "discover") || !strings.Contains(output.String(), "plan") {
		t.Fatalf("unexpected help output: %s", output.String())
	}
}

func TestRejectsInvalidOutputBeforeLoadingConfig(t *testing.T) {
	var output bytes.Buffer
	command := newCommand(strings.NewReader(""), &output, &output)
	err := command.Run(context.Background(), []string{"knbud", "discover", "--output", "xml"})
	if err == nil || !strings.Contains(err.Error(), "unsupported output format") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRejectsUnexpectedArguments(t *testing.T) {
	var output bytes.Buffer
	command := newCommand(strings.NewReader(""), &output, &output)
	err := command.Run(context.Background(), []string{"knbud", "discover", "extra"})
	if err == nil || !strings.Contains(err.Error(), "unexpected arguments") {
		t.Fatalf("unexpected error: %v", err)
	}
}
