package cli

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestGateTemplateCreateListInstantiateAndLockFlow(t *testing.T) {
	t.Parallel()

	fixture := newGateSetCommandFixture(t, "memori-cli-gate-template-set.db")
	fixture.createIssue("mem-a111111", "task", "Gate set flow issue", "cmd-cli-gset-create-1")

	defPath := fixture.writeTemplateDefinition(
		"quality-gates.json",
		`{"gates":[{"id":"build","kind":"check","required":false,"criteria":{"ref":"manual-build"}},{"id":"lint","kind":"check","required":false,"criteria":{"ref":"manual-lint"}}]}`,
	)

	stdout, stderr, err := runMemoriForTest(
		"gate", "template", "create",
		"--db", fixture.dbPath,
		"--id", "quality",
		"--version", "1",
		"--applies-to", "task",
		"--file", defPath,
		"--command-id", "cmd-cli-gtemplate-create-1",
		"--json",
	)
	if err != nil {
		t.Fatalf("gate template create: %v\nstderr: %s", err, stderr)
	}
	var created gateTemplateCreateEnvelope
	if err := json.Unmarshal([]byte(stdout), &created); err != nil {
		t.Fatalf("decode gate template create json: %v\nstdout: %s", err, stdout)
	}
	if created.Command != "gate template create" {
		t.Fatalf("expected gate template create command, got %q", created.Command)
	}
	if created.Data.Idempotent {
		t.Fatalf("expected first template create to be non-idempotent")
	}

	stdout, stderr, err = runMemoriForTest("gate", "template", "list", "--db", fixture.dbPath, "--type", "task", "--json")
	if err != nil {
		t.Fatalf("gate template list: %v\nstderr: %s", err, stderr)
	}
	var listed gateTemplateListEnvelope
	if err := json.Unmarshal([]byte(stdout), &listed); err != nil {
		t.Fatalf("decode gate template list json: %v\nstdout: %s", err, stdout)
	}
	if listed.Command != "gate template list" {
		t.Fatalf("expected gate template list command, got %q", listed.Command)
	}
	if listed.Data.Count != 1 || len(listed.Data.Templates) != 1 {
		t.Fatalf("expected one template in list response, got count=%d templates=%d", listed.Data.Count, len(listed.Data.Templates))
	}

	stdout, stderr, err = runMemoriForTest(
		"gate", "set", "instantiate",
		"--db", fixture.dbPath,
		"--issue", "mem-a111111",
		"--template", "quality@1",
		"--command-id", "cmd-cli-gset-instantiate-1",
		"--json",
	)
	if err != nil {
		t.Fatalf("gate set instantiate: %v\nstderr: %s", err, stderr)
	}
	var instantiated gateSetInstantiateEnvelope
	if err := json.Unmarshal([]byte(stdout), &instantiated); err != nil {
		t.Fatalf("decode gate set instantiate json: %v\nstdout: %s", err, stdout)
	}
	if instantiated.Command != "gate set instantiate" {
		t.Fatalf("expected gate set instantiate command, got %q", instantiated.Command)
	}
	if instantiated.Data.Idempotent {
		t.Fatalf("expected first gate set instantiate to be non-idempotent")
	}
	if len(instantiated.Data.GateSet.Items) != 2 {
		t.Fatalf("expected 2 instantiated gate items, got %d", len(instantiated.Data.GateSet.Items))
	}

	stdout, stderr, err = runMemoriForTest(
		"gate", "set", "lock",
		"--db", fixture.dbPath,
		"--issue", "mem-a111111",
		"--command-id", "cmd-cli-gset-lock-1",
		"--json",
	)
	if err != nil {
		t.Fatalf("gate set lock: %v\nstderr: %s", err, stderr)
	}
	var locked gateSetLockEnvelope
	if err := json.Unmarshal([]byte(stdout), &locked); err != nil {
		t.Fatalf("decode gate set lock json: %v\nstdout: %s", err, stdout)
	}
	if locked.Command != "gate set lock" {
		t.Fatalf("expected gate set lock command, got %q", locked.Command)
	}
	if !locked.Data.LockedNow {
		t.Fatalf("expected first gate set lock call to lock now")
	}
	if strings.TrimSpace(locked.Data.GateSet.LockedAt) == "" {
		t.Fatalf("expected locked_at to be set")
	}

	stdout, stderr, err = runMemoriForTest(
		"gate", "set", "lock",
		"--db", fixture.dbPath,
		"--issue", "mem-a111111",
		"--command-id", "cmd-cli-gset-lock-1",
		"--json",
	)
	if err != nil {
		t.Fatalf("second gate set lock: %v\nstderr: %s", err, stderr)
	}
	var relock gateSetLockEnvelope
	if err := json.Unmarshal([]byte(stdout), &relock); err != nil {
		t.Fatalf("decode second gate set lock json: %v\nstdout: %s", err, stdout)
	}
	if relock.Data.LockedNow {
		t.Fatalf("expected second lock call to report already locked")
	}

	stdout, stderr, err = runMemoriForTest(
		"event", "log",
		"--db", fixture.dbPath,
		"--entity", "gate-template:quality@1",
		"--json",
	)
	if err != nil {
		t.Fatalf("event log gate template: %v\nstderr: %s", err, stderr)
	}
	var templateEvents eventLogEnvelope
	if err := json.Unmarshal([]byte(stdout), &templateEvents); err != nil {
		t.Fatalf("decode gate template event log json: %v\nstdout: %s", err, stdout)
	}
	if templateEvents.Data.EntityType != "gate_template" || len(templateEvents.Data.Events) != 1 || templateEvents.Data.Events[0].EventType != "gate_template.created" {
		t.Fatalf("unexpected gate template event log: %+v", templateEvents)
	}

	stdout, stderr, err = runMemoriForTest(
		"event", "log",
		"--db", fixture.dbPath,
		"--entity", "gate-set:"+instantiated.Data.GateSet.GateSetID,
		"--json",
	)
	if err != nil {
		t.Fatalf("event log gate set: %v\nstderr: %s", err, stderr)
	}
	var gateSetEvents eventLogEnvelope
	if err := json.Unmarshal([]byte(stdout), &gateSetEvents); err != nil {
		t.Fatalf("decode gate set event log json: %v\nstdout: %s", err, stdout)
	}
	if gateSetEvents.Data.EntityType != "gate_set" || len(gateSetEvents.Data.Events) != 2 {
		t.Fatalf("unexpected gate set event log: %+v", gateSetEvents)
	}
	if gateSetEvents.Data.Events[0].EventType != "gate_set.instantiated" || gateSetEvents.Data.Events[1].EventType != "gate_set.locked" {
		t.Fatalf("unexpected gate set event types: %+v", gateSetEvents.Data.Events)
	}
}

func TestGateSetInstantiateAndLockHumanOutputCoversTextBranches(t *testing.T) {
	t.Parallel()

	fixture := newGateSetCommandFixture(t, "memori-cli-gate-template-set-text.db")
	fixture.createIssue("mem-a919191", "task", "Gate set text flow issue", "cmd-cli-gset-text-issue-1")

	defPath := fixture.writeTemplateDefinition(
		"quality-text.json",
		`{"gates":[{"id":"build","kind":"check","required":true,"criteria":{"command":"echo build"}}]}`,
	)
	fixture.createTemplateCLI("quality-text", 1, "task", defPath, "cmd-cli-gset-text-template-1")
	fixture.approveTemplateStore("quality-text", 1, "cmd-cli-gset-text-approve-1")

	stdout, stderr, err := runMemoriForTest(
		"gate", "set", "instantiate",
		"--db", fixture.dbPath,
		"--issue", "mem-a919191",
		"--template", "quality-text@1",
		"--command-id", "cmd-cli-gset-text-instantiate-1",
	)
	if err != nil {
		t.Fatalf("gate set instantiate text: %v\nstderr: %s", err, stderr)
	}
	for _, want := range []string{
		"Instantiated gate set ",
		"Templates: quality-text@1",
		"Gate Set Hash:",
	} {
		if !strings.Contains(stdout, want) {
			t.Fatalf("expected instantiate text output to contain %q, got:\n%s", want, stdout)
		}
	}

	stdout, stderr, err = runMemoriForTest(
		"gate", "set", "lock",
		"--db", fixture.dbPath,
		"--issue", "mem-a919191",
		"--command-id", "cmd-cli-gset-text-lock-1",
	)
	if err != nil {
		t.Fatalf("gate set lock text: %v\nstderr: %s", err, stderr)
	}
	if !strings.Contains(stdout, "Locked gate set ") {
		t.Fatalf("expected lock text output, got:\n%s", stdout)
	}

	stdout, stderr, err = runMemoriForTest(
		"gate", "set", "lock",
		"--db", fixture.dbPath,
		"--issue", "mem-a919191",
		"--command-id", "cmd-cli-gset-text-lock-1",
	)
	if err != nil {
		t.Fatalf("gate set relock text: %v\nstderr: %s", err, stderr)
	}
	if !strings.Contains(stdout, "is already locked at") {
		t.Fatalf("expected already-locked text output, got:\n%s", stdout)
	}
}
