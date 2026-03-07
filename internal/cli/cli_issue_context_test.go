package cli

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"memori/internal/store"
)

type issueEnvelope struct {
	Command string `json:"command"`
	Data    struct {
		Issue store.Issue `json:"issue"`
	} `json:"data"`
}

func TestIssueCreateAndShowExposeRichContextFields(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "memori-cli-issue-context.db")
	if _, stderr, err := runMemoriForTest("init", "--db", dbPath, "--issue-prefix", "mem", "--json"); err != nil {
		t.Fatalf("init db: %v\nstderr: %s", err, stderr)
	}

	stdout, stderr, err := runMemoriForTest(
		"issue", "create",
		"--db", dbPath,
		"--key", "mem-2222aaa",
		"--type", "task",
		"--title", "Richer context",
		"--description", "A detailed description",
		"--acceptance-criteria", "Readable in issue show",
		"--reference", "https://example.com/spec",
		"--reference", "notes.md",
		"--actor", "test",
		"--command-id", "cmd-cli-rich-create-1",
		"--json",
	)
	if err != nil {
		t.Fatalf("issue create with rich context: %v\nstderr: %s", err, stderr)
	}
	var created issueEnvelope
	if err := json.Unmarshal([]byte(stdout), &created); err != nil {
		t.Fatalf("decode issue create json: %v\nstdout: %s", err, stdout)
	}
	if created.Data.Issue.Description != "A detailed description" {
		t.Fatalf("unexpected description: %q", created.Data.Issue.Description)
	}
	if created.Data.Issue.Acceptance != "Readable in issue show" {
		t.Fatalf("unexpected acceptance criteria: %q", created.Data.Issue.Acceptance)
	}
	if len(created.Data.Issue.References) != 2 {
		t.Fatalf("expected 2 references, got %#v", created.Data.Issue.References)
	}

	stdout, stderr, err = runMemoriForTest("issue", "show", "--db", dbPath, "--key", "mem-2222aaa", "--json")
	if err != nil {
		t.Fatalf("issue show json: %v\nstderr: %s", err, stderr)
	}
	var shown issueEnvelope
	if err := json.Unmarshal([]byte(stdout), &shown); err != nil {
		t.Fatalf("decode issue show json: %v\nstdout: %s", err, stdout)
	}
	if shown.Data.Issue.Description != "A detailed description" {
		t.Fatalf("expected show description, got %q", shown.Data.Issue.Description)
	}
	if shown.Data.Issue.Acceptance != "Readable in issue show" {
		t.Fatalf("expected show acceptance criteria, got %q", shown.Data.Issue.Acceptance)
	}
}

func TestIssueUpdateSupportsContextOnlyMutation(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "memori-cli-issue-update-context.db")
	if _, stderr, err := runMemoriForTest("init", "--db", dbPath, "--issue-prefix", "mem", "--json"); err != nil {
		t.Fatalf("init db: %v\nstderr: %s", err, stderr)
	}
	if _, stderr, err := runMemoriForTest(
		"issue", "create",
		"--db", dbPath,
		"--key", "mem-3333bbb",
		"--type", "task",
		"--title", "Context update",
		"--actor", "test",
		"--command-id", "cmd-cli-rich-update-create-1",
		"--json",
	); err != nil {
		t.Fatalf("issue create: %v\nstderr: %s", err, stderr)
	}

	stdout, stderr, err := runMemoriForTest(
		"issue", "update",
		"--db", dbPath,
		"--key", "mem-3333bbb",
		"--description", "Context-only update",
		"--acceptance-criteria", "No status required for context edit",
		"--reference", "https://example.com/context",
		"--actor", "test",
		"--command-id", "cmd-cli-rich-update-1",
		"--json",
	)
	if err != nil {
		t.Fatalf("issue update context-only: %v\nstderr: %s", err, stderr)
	}
	var updated issueEnvelope
	if err := json.Unmarshal([]byte(stdout), &updated); err != nil {
		t.Fatalf("decode issue update json: %v\nstdout: %s", err, stdout)
	}
	if updated.Data.Issue.Status != "Todo" {
		t.Fatalf("status should remain Todo on context-only update, got %q", updated.Data.Issue.Status)
	}
	if updated.Data.Issue.Description != "Context-only update" {
		t.Fatalf("unexpected description after update: %q", updated.Data.Issue.Description)
	}
}

func TestIssueUpdateRequiresAtLeastOneMutationField(t *testing.T) {
	t.Parallel()

	_, _, err := runMemoriForTest(
		"issue", "update",
		"--key", "mem-4444ccc",
		"--command-id", "cmd-cli-rich-update-empty-1",
	)
	if err == nil || !strings.Contains(err.Error(), "one of --status, --priority, --label, --description, --acceptance-criteria, or --reference is required") {
		t.Fatalf("expected mutation field validation error, got: %v", err)
	}
}
