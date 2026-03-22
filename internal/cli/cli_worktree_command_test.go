package cli

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type worktreeEnvelope struct {
	Command string `json:"command"`
	Data    struct {
		Worktree struct {
			WorktreeID string `json:"worktree_id"`
			Path       string `json:"path"`
			RepoRoot   string `json:"repo_root"`
			Branch     string `json:"branch"`
			HeadOID    string `json:"head_oid"`
			IssueID    string `json:"issue_id"`
			Status     string `json:"status"`
		} `json:"worktree"`
	} `json:"data"`
}

type worktreeListEnvelope struct {
	Command string `json:"command"`
	Data    struct {
		Count     int `json:"count"`
		Worktrees []struct {
			WorktreeID string `json:"worktree_id"`
			IssueID    string `json:"issue_id"`
			Status     string `json:"status"`
			Path       string `json:"path"`
		} `json:"worktrees"`
	} `json:"data"`
}

func TestWorktreeCommandRegisterAttachShowDetachArchiveAndList(t *testing.T) {
	t.Parallel()

	dbPath := seedWorktreeCommandTestDB(t)
	worktreePath := filepath.Join(t.TempDir(), "feature-a")
	repoRoot := filepath.Dir(worktreePath)

	stdout, stderr, err := runMemoriForTest(
		"worktree", "register",
		"--db", dbPath,
		"--path", worktreePath,
		"--repo-root", repoRoot,
		"--branch", "feature/a",
		"--head", "abc1234",
		"--command-id", "cmd-worktree-cli-register-1",
		"--json",
	)
	if err != nil {
		t.Fatalf("worktree register: %v\nstderr: %s", err, stderr)
	}
	var registered worktreeEnvelope
	if err := json.Unmarshal([]byte(stdout), &registered); err != nil {
		t.Fatalf("decode worktree register json: %v\nstdout: %s", err, stdout)
	}
	if registered.Command != "worktree register" || registered.Data.Worktree.Status != "Active" {
		t.Fatalf("unexpected register response: %+v", registered)
	}

	worktreeID := registered.Data.Worktree.WorktreeID
	if worktreeID == "" {
		t.Fatal("expected generated worktree id")
	}

	if _, stderr, err := runMemoriForTest(
		"worktree", "attach",
		"--db", dbPath,
		"--worktree", worktreeID,
		"--issue", "mem-a111111",
		"--command-id", "cmd-worktree-cli-attach-1",
		"--json",
	); err != nil {
		t.Fatalf("worktree attach: %v\nstderr: %s", err, stderr)
	}

	stdout, stderr, err = runMemoriForTest(
		"worktree", "show",
		"--db", dbPath,
		"--worktree", worktreeID,
		"--json",
	)
	if err != nil {
		t.Fatalf("worktree show: %v\nstderr: %s", err, stderr)
	}
	var shown worktreeEnvelope
	if err := json.Unmarshal([]byte(stdout), &shown); err != nil {
		t.Fatalf("decode worktree show json: %v\nstdout: %s", err, stdout)
	}
	if shown.Data.Worktree.IssueID != "mem-a111111" || shown.Data.Worktree.Path != worktreePath {
		t.Fatalf("unexpected shown worktree: %+v", shown)
	}

	stdout, stderr, err = runMemoriForTest(
		"worktree", "list",
		"--db", dbPath,
		"--issue", "mem-a111111",
		"--json",
	)
	if err != nil {
		t.Fatalf("worktree list: %v\nstderr: %s", err, stderr)
	}
	var listed worktreeListEnvelope
	if err := json.Unmarshal([]byte(stdout), &listed); err != nil {
		t.Fatalf("decode worktree list json: %v\nstdout: %s", err, stdout)
	}
	if listed.Command != "worktree list" || listed.Data.Count != 1 || listed.Data.Worktrees[0].WorktreeID != worktreeID {
		t.Fatalf("unexpected worktree list response: %+v", listed)
	}

	if _, stderr, err := runMemoriForTest(
		"worktree", "detach",
		"--db", dbPath,
		"--worktree", worktreeID,
		"--command-id", "cmd-worktree-cli-detach-1",
	); err != nil {
		t.Fatalf("worktree detach: %v\nstderr: %s", err, stderr)
	}

	stdout, stderr, err = runMemoriForTest(
		"worktree", "archive",
		"--db", dbPath,
		"--worktree", worktreeID,
		"--command-id", "cmd-worktree-cli-archive-1",
	)
	if err != nil {
		t.Fatalf("worktree archive: %v\nstderr: %s", err, stderr)
	}
	for _, want := range []string{
		"Archived worktree",
		"Worktree: " + worktreeID,
		"Status: Archived",
	} {
		if !strings.Contains(stdout, want) {
			t.Fatalf("expected archive output to contain %q, got:\n%s", want, stdout)
		}
	}
}

func TestWorktreeCommandAdoptCWDFindsRepoRoot(t *testing.T) {
	dbPath := seedWorktreeCommandTestDB(t)
	repoRoot := filepath.Join(t.TempDir(), "repo")
	cwd := filepath.Join(repoRoot, "trees", "feature-b")
	if err := os.MkdirAll(filepath.Join(repoRoot, ".git"), 0o755); err != nil {
		t.Fatalf("mkdir .git: %v", err)
	}
	if err := os.MkdirAll(cwd, 0o755); err != nil {
		t.Fatalf("mkdir cwd: %v", err)
	}

	previousWD, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	defer func() {
		_ = os.Chdir(previousWD)
	}()
	if err := os.Chdir(cwd); err != nil {
		t.Fatalf("chdir cwd: %v", err)
	}
	expectedCWD, err := os.Getwd()
	if err != nil {
		t.Fatalf("get canonical cwd: %v", err)
	}
	expectedRepoRoot, err := inferRepoRootFromPath(expectedCWD)
	if err != nil {
		t.Fatalf("infer expected repo root: %v", err)
	}

	stdout, stderr, err := runMemoriForTest(
		"worktree", "adopt-cwd",
		"--db", dbPath,
		"--branch", "feature/b",
		"--command-id", "cmd-worktree-cli-adopt-1",
		"--json",
	)
	if err != nil {
		t.Fatalf("worktree adopt-cwd: %v\nstderr: %s", err, stderr)
	}
	var adopted worktreeEnvelope
	if err := json.Unmarshal([]byte(stdout), &adopted); err != nil {
		t.Fatalf("decode worktree adopt json: %v\nstdout: %s", err, stdout)
	}
	if adopted.Data.Worktree.Path != expectedCWD || adopted.Data.Worktree.RepoRoot != expectedRepoRoot || adopted.Data.Worktree.Branch != "feature/b" {
		t.Fatalf("unexpected adopted worktree: %+v", adopted)
	}
}

func TestRunWorktreeRejectsUnknownSubcommands(t *testing.T) {
	t.Parallel()

	if err := runWorktree(nil, nil); err == nil || !strings.Contains(err.Error(), "worktree subcommand required") {
		t.Fatalf("expected missing worktree subcommand error, got %v", err)
	}
	if err := runWorktree([]string{"switch"}, nil); err == nil || !strings.Contains(err.Error(), `unknown worktree subcommand "switch"`) {
		t.Fatalf("expected unknown worktree subcommand error, got %v", err)
	}
}

func seedWorktreeCommandTestDB(t *testing.T) string {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), "memori-cli-worktree.db")
	if _, stderr, err := runMemoriForTest("init", "--db", dbPath, "--issue-prefix", "mem", "--json"); err != nil {
		t.Fatalf("init db: %v\nstderr: %s", err, stderr)
	}
	if _, stderr, err := runMemoriForTest(
		"issue", "create",
		"--db", dbPath,
		"--key", "mem-a111111",
		"--type", "task",
		"--title", "Worktree target issue",
		"--command-id", "cmd-worktree-cli-issue-create-1",
		"--json",
	); err != nil {
		t.Fatalf("issue create: %v\nstderr: %s", err, stderr)
	}
	return dbPath
}
