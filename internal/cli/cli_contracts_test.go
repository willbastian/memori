package cli

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestIssueCreateRequiresCommandID(t *testing.T) {
	t.Parallel()

	_, _, err := runMemoriForTest("issue", "create", "--type", "task", "--title", "missing command id")
	if err == nil || !strings.Contains(err.Error(), "--command-id is required") {
		t.Fatalf("expected missing command-id error, got: %v", err)
	}
}

func TestIssueUpdateRequiresCommandID(t *testing.T) {
	t.Parallel()

	_, _, err := runMemoriForTest("issue", "update", "--key", "mem-a1b2c3d", "--status", "inprogress")
	if err == nil || !strings.Contains(err.Error(), "--command-id is required") {
		t.Fatalf("expected missing command-id error, got: %v", err)
	}
}

func TestIssueLinkRequiresCommandID(t *testing.T) {
	t.Parallel()

	_, _, err := runMemoriForTest("issue", "link", "--child", "mem-a1b2c3d", "--parent", "mem-b2c3d4e")
	if err == nil || !strings.Contains(err.Error(), "--command-id is required") {
		t.Fatalf("expected missing command-id error, got: %v", err)
	}
}

func TestGateEvaluateRequiresCommandID(t *testing.T) {
	t.Parallel()

	_, _, err := runMemoriForTest(
		"gate", "evaluate",
		"--issue", "mem-a1b2c3d",
		"--gate", "build",
		"--result", "PASS",
		"--evidence", "ci://run/1",
	)
	if err == nil || !strings.Contains(err.Error(), "--command-id is required") {
		t.Fatalf("expected missing command-id error, got: %v", err)
	}
}

func TestGateTemplateCreateRequiresFile(t *testing.T) {
	t.Parallel()

	_, _, err := runMemoriForTest(
		"gate", "template", "create",
		"--id", "quality",
		"--version", "1",
		"--applies-to", "task",
	)
	if err == nil || !strings.Contains(err.Error(), "--file is required") {
		t.Fatalf("expected missing --file error, got: %v", err)
	}
}

func TestIssueDoneRequiresLockedGateSet(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "memori-cli-done-requires-gates.db")
	if _, stderr, err := runMemoriForTest("init", "--db", dbPath, "--issue-prefix", "mem", "--json"); err != nil {
		t.Fatalf("init db: %v\nstderr: %s", err, stderr)
	}
	if _, stderr, err := runMemoriForTest(
		"issue", "create",
		"--db", dbPath,
		"--key", "mem-a1a1a1a",
		"--type", "task",
		"--title", "Done gate requirement test",
		"--command-id", "cmd-cli-done-gates-create-1",
		"--json",
	); err != nil {
		t.Fatalf("issue create: %v\nstderr: %s", err, stderr)
	}
	if _, stderr, err := runMemoriForTest(
		"issue", "update",
		"--db", dbPath,
		"--key", "mem-a1a1a1a",
		"--status", "inprogress",
		"--command-id", "cmd-cli-done-gates-progress-1",
		"--json",
	); err != nil {
		t.Fatalf("issue update inprogress: %v\nstderr: %s", err, stderr)
	}

	_, _, err := runMemoriForTest(
		"issue", "update",
		"--db", dbPath,
		"--key", "mem-a1a1a1a",
		"--status", "done",
		"--command-id", "cmd-cli-done-gates-done-1",
		"--json",
	)
	if err == nil || !strings.Contains(err.Error(), "no locked gate set for current cycle") {
		t.Fatalf("expected done gate-set requirement error, got: %v", err)
	}
}

func TestIssueDoneRequiresChildIssuesClosed(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "memori-cli-done-requires-children.db")
	if _, stderr, err := runMemoriForTest("init", "--db", dbPath, "--issue-prefix", "mem", "--json"); err != nil {
		t.Fatalf("init db: %v\nstderr: %s", err, stderr)
	}
	if _, stderr, err := runMemoriForTest(
		"issue", "create",
		"--db", dbPath,
		"--key", "mem-a111111",
		"--type", "story",
		"--title", "Parent story",
		"--command-id", "cmd-cli-done-children-parent-create-1",
		"--json",
	); err != nil {
		t.Fatalf("parent issue create: %v\nstderr: %s", err, stderr)
	}
	if _, stderr, err := runMemoriForTest(
		"issue", "create",
		"--db", dbPath,
		"--key", "mem-b111111",
		"--type", "task",
		"--parent", "mem-a111111",
		"--title", "Child task",
		"--command-id", "cmd-cli-done-children-child-create-1",
		"--json",
	); err != nil {
		t.Fatalf("child issue create: %v\nstderr: %s", err, stderr)
	}
	if _, stderr, err := runMemoriForTest(
		"issue", "update",
		"--db", dbPath,
		"--key", "mem-a111111",
		"--status", "inprogress",
		"--command-id", "cmd-cli-done-children-parent-progress-1",
		"--json",
	); err != nil {
		t.Fatalf("parent issue update inprogress: %v\nstderr: %s", err, stderr)
	}

	gateDefPath := filepath.Join(t.TempDir(), "story-close-gates.json")
	if err := os.WriteFile(gateDefPath, []byte(`{"gates":[{"id":"review","kind":"check","required":true,"criteria":{"command":"echo ok"}}]}`), 0o644); err != nil {
		t.Fatalf("write gate definition: %v", err)
	}
	if _, stderr, err := runMemoriForTest(
		"gate", "template", "create",
		"--db", dbPath,
		"--id", "story-close",
		"--version", "1",
		"--applies-to", "story",
		"--file", gateDefPath,
		"--json",
	); err != nil {
		t.Fatalf("gate template create: %v\nstderr: %s", err, stderr)
	}
	if _, stderr, err := runMemoriForTest(
		"gate", "set", "instantiate",
		"--db", dbPath,
		"--issue", "mem-a111111",
		"--template", "story-close@1",
		"--json",
	); err != nil {
		t.Fatalf("gate set instantiate: %v\nstderr: %s", err, stderr)
	}
	if _, stderr, err := runMemoriForTest(
		"gate", "set", "lock",
		"--db", dbPath,
		"--issue", "mem-a111111",
		"--json",
	); err != nil {
		t.Fatalf("gate set lock: %v\nstderr: %s", err, stderr)
	}
	if _, stderr, err := runMemoriForTest(
		"gate", "verify",
		"--db", dbPath,
		"--issue", "mem-a111111",
		"--gate", "review",
		"--command-id", "cmd-cli-done-children-parent-verify-1",
		"--json",
	); err != nil {
		t.Fatalf("gate verify: %v\nstderr: %s", err, stderr)
	}

	_, _, err := runMemoriForTest(
		"issue", "update",
		"--db", dbPath,
		"--key", "mem-a111111",
		"--status", "done",
		"--command-id", "cmd-cli-done-children-parent-done-1",
		"--json",
	)
	if err == nil || !strings.Contains(err.Error(), "child issues must be Done first: mem-b111111=Todo") {
		t.Fatalf("expected child-close requirement error, got: %v", err)
	}
}

func TestEventLogRejectsUnknownEntityType(t *testing.T) {
	t.Parallel()

	_, _, err := runMemoriForTest("event", "log", "--entity", "session:abc123")
	if err == nil || !strings.Contains(err.Error(), "invalid entity type") {
		t.Fatalf("expected invalid entity type error, got: %v", err)
	}
}

func TestIssueUpdateSupportsPriorityAndLabels(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "memori-cli-priority-labels.db")
	if _, stderr, err := runMemoriForTest("init", "--db", dbPath, "--issue-prefix", "mem", "--json"); err != nil {
		t.Fatalf("init db: %v\nstderr: %s", err, stderr)
	}
	if _, stderr, err := runMemoriForTest(
		"issue", "create",
		"--db", dbPath,
		"--key", "mem-a1b1c1d",
		"--type", "task",
		"--title", "Priority and labels test",
		"--command-id", "cmd-cli-priority-label-create-1",
		"--json",
	); err != nil {
		t.Fatalf("issue create: %v\nstderr: %s", err, stderr)
	}

	if _, stderr, err := runMemoriForTest(
		"issue", "update",
		"--db", dbPath,
		"--key", "mem-a1b1c1d",
		"--priority", "p1",
		"--label", "backend",
		"--label", "urgent",
		"--command-id", "cmd-cli-priority-label-update-1",
		"--json",
	); err != nil {
		t.Fatalf("issue update priority/labels: %v\nstderr: %s", err, stderr)
	}

	stdout, stderr, err := runMemoriForTest(
		"issue", "show",
		"--db", dbPath,
		"--key", "mem-a1b1c1d",
		"--json",
	)
	if err != nil {
		t.Fatalf("issue show: %v\nstderr: %s", err, stderr)
	}
	var showResp struct {
		Data struct {
			Issue struct {
				Priority string   `json:"priority"`
				Labels   []string `json:"labels"`
			} `json:"issue"`
		} `json:"data"`
	}
	if err := json.Unmarshal([]byte(stdout), &showResp); err != nil {
		t.Fatalf("decode issue show json: %v\nstdout: %s", err, stdout)
	}
	if showResp.Data.Issue.Priority != "P1" {
		t.Fatalf("expected normalized priority P1, got %q", showResp.Data.Issue.Priority)
	}
	if len(showResp.Data.Issue.Labels) != 2 || showResp.Data.Issue.Labels[0] != "backend" || showResp.Data.Issue.Labels[1] != "urgent" {
		t.Fatalf("expected labels [backend urgent], got %#v", showResp.Data.Issue.Labels)
	}
}
