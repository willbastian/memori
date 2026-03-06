package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"memori/internal/store"
)

type backlogCommandJSON struct {
	Command string `json:"command"`
	Data    struct {
		Count  int           `json:"count"`
		Issues []store.Issue `json:"issues"`
	} `json:"data"`
}

func TestBacklogCommandJSONFiltersByTypeAndStatus(t *testing.T) {
	t.Parallel()

	dbPath := seedBacklogTestDB(t)

	if err := runIssueUpdateForTest(dbPath, "mem-c111111", "inprogress", "cmd-backlog-cli-update-1"); err != nil {
		t.Fatalf("update mem-c111111 status: %v", err)
	}

	resp := runBacklogJSONForTest(t, "--db", dbPath, "--type", "task", "--status", "inprogress")
	if resp.Command != "backlog" {
		t.Fatalf("expected command backlog, got %q", resp.Command)
	}
	if resp.Data.Count != 1 {
		t.Fatalf("expected 1 filtered issue, got %d", resp.Data.Count)
	}
	if len(resp.Data.Issues) != 1 || resp.Data.Issues[0].ID != "mem-c111111" {
		t.Fatalf("expected only mem-c111111 in filtered result, got %#v", resp.Data.Issues)
	}
}

func TestBacklogCommandHumanOutputShowsParentWhenFilteredParentExcluded(t *testing.T) {
	t.Parallel()

	dbPath := seedBacklogTestDB(t)

	stdout, stderr, err := runMemoriForTest("backlog", "--db", dbPath, "--type", "task")
	if err != nil {
		t.Fatalf("run backlog command: %v\nstderr: %s", err, stderr)
	}

	if !strings.Contains(stdout, "mem-c111111 [Task/Todo] Child task (parent: mem-b111111)") {
		t.Fatalf("expected parent annotation when parent filtered out, got:\n%s", stdout)
	}
	if strings.Contains(stdout, "mem-b111111 [Story/") {
		t.Fatalf("did not expect story row in task-only filtered output, got:\n%s", stdout)
	}
}

func seedBacklogTestDB(t *testing.T) string {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), "memori-cli-backlog.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer s.Close()

	ctx := context.Background()
	if err := s.Initialize(ctx, store.InitializeParams{IssueKeyPrefix: "mem"}); err != nil {
		t.Fatalf("initialize store: %v", err)
	}

	create := func(id, typ, title, parent, cmd string) {
		t.Helper()
		if _, _, _, err := s.CreateIssue(ctx, store.CreateIssueParams{
			IssueID:   id,
			Type:      typ,
			Title:     title,
			ParentID:  parent,
			Actor:     "test",
			CommandID: cmd,
		}); err != nil {
			t.Fatalf("create issue %s: %v", id, err)
		}
	}

	create("mem-a111111", "epic", "Root epic", "", "cmd-backlog-cli-create-1")
	create("mem-b111111", "story", "Parent story", "mem-a111111", "cmd-backlog-cli-create-2")
	create("mem-c111111", "task", "Child task", "mem-b111111", "cmd-backlog-cli-create-3")
	create("mem-d111111", "bug", "Child bug", "mem-b111111", "cmd-backlog-cli-create-4")

	return dbPath
}

func runIssueUpdateForTest(dbPath, issueKey, status, commandID string) error {
	_, _, err := runMemoriForTest("issue", "update", "--db", dbPath, "--key", issueKey, "--status", status, "--actor", "test", "--command-id", commandID)
	return err
}

func runBacklogJSONForTest(t *testing.T, extraArgs ...string) backlogCommandJSON {
	t.Helper()

	args := append([]string{"backlog", "--json"}, extraArgs...)
	stdout, stderr, err := runMemoriForTest(args...)
	if err != nil {
		t.Fatalf("run backlog json command: %v\nstderr: %s", err, stderr)
	}

	var resp backlogCommandJSON
	if err := json.Unmarshal([]byte(stdout), &resp); err != nil {
		t.Fatalf("decode backlog json output: %v\nstdout: %s", err, stdout)
	}
	return resp
}

func runMemoriForTest(args ...string) (string, string, error) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := Run(args, &stdout, &stderr)
	return stdout.String(), stderr.String(), err
}
