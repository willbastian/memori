package cli

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"github.com/willbastian/memori/internal/store"
)

type mutationEventEnvelope struct {
	ResponseSchemaVersion int    `json:"response_schema_version"`
	DBSchemaVersion       int    `json:"db_schema_version"`
	Command               string `json:"command"`
	Data                  struct {
		Event store.Event `json:"event"`
	} `json:"data"`
}

func TestIssueCreateGeneratesCommandIDWhenOmitted(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "memori-cli-generate-create.db")
	if _, stderr, err := runMemoriForTest("init", "--db", dbPath, "--issue-prefix", "mem", "--json"); err != nil {
		t.Fatalf("init db: %v\nstderr: %s", err, stderr)
	}

	stdout, stderr, err := runMemoriForTest(
		"issue", "create",
		"--db", dbPath,
		"--key", "mem-a1b2c3d",
		"--type", "task",
		"--title", "generated command id",
		"--json",
	)
	if err != nil {
		t.Fatalf("issue create: %v\nstderr: %s", err, stderr)
	}

	var resp mutationEventEnvelope
	if err := json.Unmarshal([]byte(stdout), &resp); err != nil {
		t.Fatalf("decode issue create json: %v\nstdout: %s", err, stdout)
	}
	assertEnvelopeMetadata(t, resp.ResponseSchemaVersion, resp.DBSchemaVersion)
	if !strings.HasPrefix(resp.Data.Event.CommandID, "cmdv1-issue-create-") {
		t.Fatalf("expected generated command id, got %q", resp.Data.Event.CommandID)
	}
}

func TestIssueUpdateGeneratesCommandIDWhenOmitted(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "memori-cli-generate-update.db")
	if _, stderr, err := runMemoriForTest("init", "--db", dbPath, "--issue-prefix", "mem", "--json"); err != nil {
		t.Fatalf("init db: %v\nstderr: %s", err, stderr)
	}
	if _, stderr, err := runMemoriForTest(
		"issue", "create",
		"--db", dbPath,
		"--key", "mem-a1b2c3d",
		"--type", "task",
		"--title", "update target",
		"--json",
	); err != nil {
		t.Fatalf("issue create: %v\nstderr: %s", err, stderr)
	}

	stdout, stderr, err := runMemoriForTest(
		"issue", "update",
		"--db", dbPath,
		"--key", "mem-a1b2c3d",
		"--status", "inprogress",
		"--json",
	)
	if err != nil {
		t.Fatalf("issue update: %v\nstderr: %s", err, stderr)
	}

	var resp mutationEventEnvelope
	if err := json.Unmarshal([]byte(stdout), &resp); err != nil {
		t.Fatalf("decode issue update json: %v\nstdout: %s", err, stdout)
	}
	assertEnvelopeMetadata(t, resp.ResponseSchemaVersion, resp.DBSchemaVersion)
	if !strings.HasPrefix(resp.Data.Event.CommandID, "cmdv1-issue-update-") {
		t.Fatalf("expected generated command id, got %q", resp.Data.Event.CommandID)
	}
}

func TestIssueLinkGeneratesCommandIDWhenOmitted(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "memori-cli-generate-link.db")
	if _, stderr, err := runMemoriForTest("init", "--db", dbPath, "--issue-prefix", "mem", "--json"); err != nil {
		t.Fatalf("init db: %v\nstderr: %s", err, stderr)
	}
	if _, stderr, err := runMemoriForTest(
		"issue", "create",
		"--db", dbPath,
		"--key", "mem-b2c3d4e",
		"--type", "story",
		"--title", "parent story",
		"--json",
	); err != nil {
		t.Fatalf("parent create: %v\nstderr: %s", err, stderr)
	}
	if _, stderr, err := runMemoriForTest(
		"issue", "create",
		"--db", dbPath,
		"--key", "mem-a1b2c3d",
		"--type", "task",
		"--title", "child task",
		"--json",
	); err != nil {
		t.Fatalf("child create: %v\nstderr: %s", err, stderr)
	}

	stdout, stderr, err := runMemoriForTest(
		"issue", "link",
		"--db", dbPath,
		"--child", "mem-a1b2c3d",
		"--parent", "mem-b2c3d4e",
		"--json",
	)
	if err != nil {
		t.Fatalf("issue link: %v\nstderr: %s", err, stderr)
	}

	var resp mutationEventEnvelope
	if err := json.Unmarshal([]byte(stdout), &resp); err != nil {
		t.Fatalf("decode issue link json: %v\nstdout: %s", err, stdout)
	}
	assertEnvelopeMetadata(t, resp.ResponseSchemaVersion, resp.DBSchemaVersion)
	if !strings.HasPrefix(resp.Data.Event.CommandID, "cmdv1-issue-link-") {
		t.Fatalf("expected generated command id, got %q", resp.Data.Event.CommandID)
	}
}

func TestGateEvaluateGeneratesCommandIDWhenOmitted(t *testing.T) {
	t.Parallel()

	dbPath := seedGateCommandTestDB(t)

	stdout, stderr, err := runMemoriForTest(
		"gate", "evaluate",
		"--db", dbPath,
		"--issue", "mem-c111111",
		"--gate", "build",
		"--result", "FAIL",
		"--evidence", "ci://run/1",
		"--json",
	)
	if err != nil {
		t.Fatalf("gate evaluate: %v\nstderr: %s", err, stderr)
	}

	var resp mutationEventEnvelope
	if err := json.Unmarshal([]byte(stdout), &resp); err != nil {
		t.Fatalf("decode gate evaluate json: %v\nstdout: %s", err, stdout)
	}
	assertEnvelopeMetadata(t, resp.ResponseSchemaVersion, resp.DBSchemaVersion)
	if !strings.HasPrefix(resp.Data.Event.CommandID, "cmdv1-gate-evaluate-") {
		t.Fatalf("expected generated command id, got %q", resp.Data.Event.CommandID)
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

	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if _, _, err := s.CreateGateTemplate(context.Background(), store.CreateGateTemplateParams{
		TemplateID:     "story-close",
		Version:        1,
		AppliesTo:      []string{"story"},
		DefinitionJSON: `{"gates":[{"id":"review","kind":"check","required":true,"criteria":{"command":"echo verified"}}]}`,
		Actor:          "human:alice",
		CommandID:      "cmd-cli-done-children-template-1",
	}); err != nil {
		t.Fatalf("create gate template via store: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close store: %v", err)
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
		"--command-id", "cmd-cli-done-children-parent-eval-1",
		"--json",
	); err != nil {
		t.Fatalf("gate verify: %v\nstderr: %s", err, stderr)
	}

	_, _, err = runMemoriForTest(
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

func TestDefaultContinuityLoopHumanContract(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "memori-cli-default-loop-human.db")
	if _, stderr, err := runMemoriForTest("init", "--db", dbPath, "--issue-prefix", "mem", "--json"); err != nil {
		t.Fatalf("init db: %v\nstderr: %s", err, stderr)
	}
	if _, stderr, err := runMemoriForTest(
		"issue", "create",
		"--db", dbPath,
		"--key", "mem-c111111",
		"--type", "task",
		"--title", "Default continuity loop",
		"--command-id", "cmd-cli-default-loop-human-create-1",
		"--json",
	); err != nil {
		t.Fatalf("issue create: %v\nstderr: %s", err, stderr)
	}

	stdout, stderr, err := runMemoriForTest(
		"issue", "update",
		"--db", dbPath,
		"--key", "mem-c111111",
		"--status", "inprogress",
		"--agent", "agent-loop-1",
		"--command-id", "cmd-cli-default-loop-human-progress-1",
	)
	if err != nil {
		t.Fatalf("issue update inprogress: %v\nstderr: %s", err, stderr)
	}
	mustContain(t, stdout, "Continuity Started:")
	mustContain(t, stdout, "Captured session ")
	mustContain(t, stdout, "Refreshed issue packet ")
	mustContain(t, stdout, "Updated agent agent-loop-1 focus to mem-c111111 via packet ")

	stdout, stderr, err = runMemoriForTest("issue", "show", "--db", dbPath, "--key", "mem-c111111")
	if err != nil {
		t.Fatalf("issue show active: %v\nstderr: %s", err, stderr)
	}
	mustContain(t, stdout, "Continuity State:")
	mustContain(t, stdout, "Continuity Pressure:")
	mustContain(t, stdout, "Open session sess_")
	mustContain(t, stdout, "Resume:")
	mustContain(t, stdout, "memori context resume")

	stdout, stderr, err = runMemoriForTest(
		"issue", "update",
		"--db", dbPath,
		"--key", "mem-c111111",
		"--status", "blocked",
		"--note", "waiting on review",
		"--command-id", "cmd-cli-default-loop-human-blocked-1",
	)
	if err != nil {
		t.Fatalf("issue update blocked: %v\nstderr: %s", err, stderr)
	}
	mustContain(t, stdout, "Continuity Saved:")
	mustContain(t, stdout, "Summarized session ")
	mustContain(t, stdout, "Saved session packet ")

	stdout, stderr, err = runMemoriForTest("issue", "show", "--db", dbPath, "--key", "mem-c111111")
	if err != nil {
		t.Fatalf("issue show blocked: %v\nstderr: %s", err, stderr)
	}
	mustContain(t, stdout, "Continuity State:")
	mustContain(t, stdout, "Continuity Pressure:")
	mustContain(t, stdout, "mem-c111111 is blocked and its saved issue packet is stale; rebuild it before the next handoff.")
	mustContain(t, stdout, "Resume:")
	mustContain(t, stdout, "memori context resume")

	stdout, stderr, err = runMemoriForTest(
		"context", "resume",
		"--db", dbPath,
		"--agent", "agent-loop-1",
		"--command-id", "cmd-cli-default-loop-human-resume-1",
	)
	if err != nil {
		t.Fatalf("context resume: %v\nstderr: %s", err, stderr)
	}
	mustContain(t, stdout, "OK Resumed session ")
	mustContain(t, stdout, "via packet and updated focus for agent-loop-1")

	stdout, stderr, err = runMemoriForTest(
		"issue", "update",
		"--db", dbPath,
		"--key", "mem-c111111",
		"--status", "inprogress",
		"--agent", "agent-loop-1",
		"--command-id", "cmd-cli-default-loop-human-progress-2",
	)
	if err != nil {
		t.Fatalf("issue update back to inprogress: %v\nstderr: %s", err, stderr)
	}
	mustContain(t, stdout, "Continuity Started:")

	seedManualCloseProofForContractTest(t, dbPath, "mem-c111111", "cmd-cli-default-loop-human-closeproof")

	stdout, stderr, err = runMemoriForTest(
		"issue", "update",
		"--db", dbPath,
		"--key", "mem-c111111",
		"--status", "done",
		"--note", "completed implementation",
		"--reason", "ready for close",
		"--command-id", "cmd-cli-default-loop-human-done-1",
	)
	if err != nil {
		t.Fatalf("issue update done: %v\nstderr: %s", err, stderr)
	}
	mustContain(t, stdout, "Continuity Saved:")
	mustContain(t, stdout, "Closed session ")
	mustContain(t, stdout, "Saved session packet ")
}

func TestDefaultContinuityLoopJSONContract(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "memori-cli-default-loop-json.db")
	if _, stderr, err := runMemoriForTest("init", "--db", dbPath, "--issue-prefix", "mem", "--json"); err != nil {
		t.Fatalf("init db: %v\nstderr: %s", err, stderr)
	}
	if _, stderr, err := runMemoriForTest(
		"issue", "create",
		"--db", dbPath,
		"--key", "mem-c111111",
		"--type", "task",
		"--title", "Default continuity loop json",
		"--command-id", "cmd-cli-default-loop-json-create-1",
		"--json",
	); err != nil {
		t.Fatalf("issue create: %v\nstderr: %s", err, stderr)
	}

	stdout, stderr, err := runMemoriForTest(
		"issue", "update",
		"--db", dbPath,
		"--key", "mem-c111111",
		"--status", "inprogress",
		"--agent", "agent-loop-json-1",
		"--command-id", "cmd-cli-default-loop-json-progress-1",
		"--json",
	)
	if err != nil {
		t.Fatalf("issue update inprogress: %v\nstderr: %s", err, stderr)
	}
	var updated issueEnvelope
	if err := json.Unmarshal([]byte(stdout), &updated); err != nil {
		t.Fatalf("decode issue update json: %v\nstdout: %s", err, stdout)
	}
	if updated.Command != "issue update" || updated.Data.Issue.Status != "InProgress" {
		t.Fatalf("unexpected inprogress update response: %+v", updated)
	}

	stdout, stderr, err = runMemoriForTest(
		"issue", "show",
		"--db", dbPath,
		"--key", "mem-c111111",
		"--json",
	)
	if err != nil {
		t.Fatalf("issue show json: %v\nstderr: %s", err, stderr)
	}
	var shown issueEnvelope
	if err := json.Unmarshal([]byte(stdout), &shown); err != nil {
		t.Fatalf("decode issue show json: %v\nstdout: %s", err, stdout)
	}
	if shown.Command != "issue show" || shown.Data.Issue.Status != "InProgress" {
		t.Fatalf("unexpected issue show response: %+v", shown)
	}

	stdout, stderr, err = runMemoriForTest(
		"issue", "update",
		"--db", dbPath,
		"--key", "mem-c111111",
		"--status", "blocked",
		"--note", "waiting on review",
		"--command-id", "cmd-cli-default-loop-json-blocked-1",
		"--json",
	)
	if err != nil {
		t.Fatalf("issue update blocked: %v\nstderr: %s", err, stderr)
	}
	if err := json.Unmarshal([]byte(stdout), &updated); err != nil {
		t.Fatalf("decode blocked update json: %v\nstdout: %s", err, stdout)
	}
	if updated.Data.Issue.Status != "Blocked" {
		t.Fatalf("expected blocked issue status, got %+v", updated)
	}

	stdout, stderr, err = runMemoriForTest(
		"context", "resume",
		"--db", dbPath,
		"--agent", "agent-loop-json-1",
		"--command-id", "cmd-cli-default-loop-json-resume-1",
		"--json",
	)
	if err != nil {
		t.Fatalf("context resume: %v\nstderr: %s", err, stderr)
	}
	var resumed contextResumeEnvelope
	if err := json.Unmarshal([]byte(stdout), &resumed); err != nil {
		t.Fatalf("decode context resume json: %v\nstdout: %s", err, stdout)
	}
	if resumed.Command != "context resume" || resumed.Data.Source != "packet" || !resumed.Data.FocusUsed {
		t.Fatalf("unexpected context resume response: %+v", resumed)
	}
	if resumed.Data.Focus.AgentID != "agent-loop-json-1" || resumed.Data.Focus.LastPacketID == "" {
		t.Fatalf("expected focused resume packet for agent-loop-json-1, got %+v", resumed)
	}

	stdout, stderr, err = runMemoriForTest(
		"issue", "update",
		"--db", dbPath,
		"--key", "mem-c111111",
		"--status", "inprogress",
		"--agent", "agent-loop-json-1",
		"--command-id", "cmd-cli-default-loop-json-progress-2",
		"--json",
	)
	if err != nil {
		t.Fatalf("issue update back to inprogress: %v\nstderr: %s", err, stderr)
	}
	if err := json.Unmarshal([]byte(stdout), &updated); err != nil {
		t.Fatalf("decode second inprogress update json: %v\nstdout: %s", err, stdout)
	}
	if updated.Data.Issue.Status != "InProgress" {
		t.Fatalf("expected issue to return to InProgress, got %+v", updated)
	}

	seedManualCloseProofForContractTest(t, dbPath, "mem-c111111", "cmd-cli-default-loop-json-closeproof")

	stdout, stderr, err = runMemoriForTest(
		"issue", "update",
		"--db", dbPath,
		"--key", "mem-c111111",
		"--status", "done",
		"--note", "completed implementation",
		"--reason", "ready for close",
		"--command-id", "cmd-cli-default-loop-json-done-1",
		"--json",
	)
	if err != nil {
		t.Fatalf("issue update done: %v\nstderr: %s", err, stderr)
	}
	if err := json.Unmarshal([]byte(stdout), &updated); err != nil {
		t.Fatalf("decode done update json: %v\nstdout: %s", err, stdout)
	}
	if updated.Data.Issue.Status != "Done" {
		t.Fatalf("expected done issue status, got %+v", updated)
	}

	stdout, stderr, err = runMemoriForTest(
		"context", "resume",
		"--db", dbPath,
		"--session", resumed.Data.SessionID,
		"--command-id", "cmd-cli-default-loop-json-resume-2",
		"--json",
	)
	if err != nil {
		t.Fatalf("context resume after done: %v\nstderr: %s", err, stderr)
	}
	if err := json.Unmarshal([]byte(stdout), &resumed); err != nil {
		t.Fatalf("decode post-done context resume json: %v\nstdout: %s", err, stdout)
	}
	if resumed.Data.Source != "packet" || resumed.Data.SessionID == "" {
		t.Fatalf("expected packet-first resume contract after done, got %+v", resumed)
	}
}

func TestEventLogRejectsUnknownEntityType(t *testing.T) {
	t.Parallel()

	_, _, err := runMemoriForTest("event", "log", "--entity", "checkpoint:abc123")
	if err == nil || !strings.Contains(err.Error(), "invalid entity type") {
		t.Fatalf("expected invalid entity type error, got: %v", err)
	}
}

func TestEventLogBareEntityDefaultsToIssue(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "memori-cli-event-log-default-issue.db")
	if _, stderr, err := runMemoriForTest("init", "--db", dbPath, "--issue-prefix", "mem", "--json"); err != nil {
		t.Fatalf("init db: %v\nstderr: %s", err, stderr)
	}
	if _, stderr, err := runMemoriForTest(
		"issue", "create",
		"--db", dbPath,
		"--key", "mem-a1b2c3d",
		"--type", "task",
		"--title", "Event log default entity test",
		"--command-id", "cmd-cli-event-log-default-1",
		"--json",
	); err != nil {
		t.Fatalf("issue create: %v\nstderr: %s", err, stderr)
	}

	stdout, stderr, err := runMemoriForTest(
		"event", "log",
		"--db", dbPath,
		"--entity", "mem-a1b2c3d",
		"--json",
	)
	if err != nil {
		t.Fatalf("event log issue default: %v\nstderr: %s", err, stderr)
	}

	var resp eventLogEnvelope
	if err := json.Unmarshal([]byte(stdout), &resp); err != nil {
		t.Fatalf("decode event log issue default json: %v\nstdout: %s", err, stdout)
	}
	if resp.Command != "event log" || resp.Data.EntityType != "issue" || resp.Data.EntityID != "mem-a1b2c3d" {
		t.Fatalf("unexpected event log issue default response: %+v", resp)
	}
	if len(resp.Data.Events) != 1 || resp.Data.Events[0].EventType != "issue.created" {
		t.Fatalf("unexpected issue event log contents: %+v", resp)
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

func assertEnvelopeMetadata(t *testing.T, gotResponseSchemaVersion, gotDBSchemaVersion int) {
	t.Helper()

	if gotResponseSchemaVersion != responseSchemaVersion {
		t.Fatalf("expected response_schema_version %d, got %d", responseSchemaVersion, gotResponseSchemaVersion)
	}
	if gotDBSchemaVersion < 0 {
		t.Fatalf("expected non-negative db_schema_version, got %d", gotDBSchemaVersion)
	}
}

func seedManualCloseProofForContractTest(t *testing.T, dbPath, issueID, commandPrefix string) {
	t.Helper()

	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer s.Close()

	ctx := context.Background()
	if _, _, err := s.CreateGateTemplate(ctx, store.CreateGateTemplateParams{
		TemplateID:     strings.ReplaceAll(issueID, "-", "") + "-close",
		Version:        1,
		AppliesTo:      []string{"task"},
		DefinitionJSON: `{"gates":[{"id":"build","kind":"check","required":true,"criteria":{"ref":"manual-validation"}}]}`,
		Actor:          "human:alice",
		CommandID:      commandPrefix + "-template-1",
	}); err != nil {
		t.Fatalf("create gate template: %v", err)
	}
	if _, _, err := s.InstantiateGateSet(ctx, store.InstantiateGateSetParams{
		IssueID:      issueID,
		TemplateRefs: []string{strings.ReplaceAll(issueID, "-", "") + "-close@1"},
		Actor:        "test",
		CommandID:    commandPrefix + "-instantiate-1",
	}); err != nil {
		t.Fatalf("instantiate gate set: %v", err)
	}
	if _, _, err := s.LockGateSet(ctx, store.LockGateSetParams{
		IssueID:   issueID,
		Actor:     "test",
		CommandID: commandPrefix + "-lock-1",
	}); err != nil {
		t.Fatalf("lock gate set: %v", err)
	}
	if _, _, _, err := s.EvaluateGate(ctx, store.EvaluateGateParams{
		IssueID: issueID,
		GateID:  "build",
		Result:  "PASS",
		EvidenceRefs: []string{
			"test://default-continuity-loop",
		},
		Actor:     "test",
		CommandID: commandPrefix + "-evaluate-1",
	}); err != nil {
		t.Fatalf("evaluate gate: %v", err)
	}
}
