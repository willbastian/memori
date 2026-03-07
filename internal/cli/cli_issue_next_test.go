package cli

import (
	"encoding/json"
	"path/filepath"
	"testing"
)

type issueNextEnvelope struct {
	Command string `json:"command"`
	Data    struct {
		Next struct {
			Considered int `json:"considered"`
			Candidate  struct {
				Issue struct {
					ID     string `json:"id"`
					Status string `json:"status"`
				} `json:"issue"`
				Reasons []string `json:"reasons"`
			} `json:"candidate"`
		} `json:"next"`
	} `json:"data"`
}

func TestIssueNextReturnsDeterministicCandidateWithReasons(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "memori-cli-issue-next.db")
	if _, stderr, err := runMemoriForTest("init", "--db", dbPath, "--issue-prefix", "mem", "--json"); err != nil {
		t.Fatalf("init db: %v\nstderr: %s", err, stderr)
	}

	create := func(key, issueType, title, commandID string) {
		t.Helper()
		if _, stderr, err := runMemoriForTest(
			"issue", "create",
			"--db", dbPath,
			"--key", key,
			"--type", issueType,
			"--title", title,
			"--command-id", commandID,
			"--json",
		); err != nil {
			t.Fatalf("issue create %s: %v\nstderr: %s", key, err, stderr)
		}
	}

	create("mem-a111111", "task", "Todo task", "cmd-next-create-1")
	create("mem-b222222", "bug", "Todo bug", "cmd-next-create-2")
	create("mem-c333333", "task", "InProgress task", "cmd-next-create-3")

	if _, stderr, err := runMemoriForTest(
		"issue", "update",
		"--db", dbPath,
		"--key", "mem-c333333",
		"--status", "inprogress",
		"--command-id", "cmd-next-progress-3",
		"--json",
	); err != nil {
		t.Fatalf("issue update inprogress: %v\nstderr: %s", err, stderr)
	}

	stdout, stderr, err := runMemoriForTest("issue", "next", "--db", dbPath, "--agent", "agent-next-1", "--json")
	if err != nil {
		t.Fatalf("issue next: %v\nstderr: %s", err, stderr)
	}

	var resp issueNextEnvelope
	if err := json.Unmarshal([]byte(stdout), &resp); err != nil {
		t.Fatalf("decode issue next json: %v\nstdout: %s", err, stdout)
	}
	if resp.Command != "issue next" {
		t.Fatalf("expected issue next command, got %q", resp.Command)
	}
	if resp.Data.Next.Considered != 3 {
		t.Fatalf("expected 3 considered issues, got %d", resp.Data.Next.Considered)
	}
	if resp.Data.Next.Candidate.Issue.ID != "mem-c333333" {
		t.Fatalf("expected in-progress task selected first, got %q", resp.Data.Next.Candidate.Issue.ID)
	}
	if len(resp.Data.Next.Candidate.Reasons) == 0 {
		t.Fatalf("expected reason strings in issue next response")
	}
}
