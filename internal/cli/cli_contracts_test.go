package cli

import (
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

func TestEventLogRejectsUnknownEntityType(t *testing.T) {
	t.Parallel()

	_, _, err := runMemoriForTest("event", "log", "--entity", "session:abc123")
	if err == nil || !strings.Contains(err.Error(), "invalid entity type") {
		t.Fatalf("expected invalid entity type error, got: %v", err)
	}
}
