package cli

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/willbastian/memori/internal/store"
)

type gateTemplateCreateEnvelope struct {
	Command string `json:"command"`
	Data    struct {
		Template   store.GateTemplate `json:"template"`
		Idempotent bool               `json:"idempotent"`
	} `json:"data"`
}

type gateTemplateListEnvelope struct {
	Command string `json:"command"`
	Data    struct {
		Count     int                  `json:"count"`
		Templates []store.GateTemplate `json:"templates"`
	} `json:"data"`
}

type gateTemplateApproveEnvelope struct {
	Command string `json:"command"`
	Data    struct {
		Template   store.GateTemplate `json:"template"`
		Idempotent bool               `json:"idempotent"`
	} `json:"data"`
}

type gateSetInstantiateEnvelope struct {
	Command string `json:"command"`
	Data    struct {
		GateSet      store.GateSet `json:"gate_set"`
		Idempotent   bool          `json:"idempotent"`
		AutoSelected bool          `json:"auto_selected"`
	} `json:"data"`
}

type gateSetLockEnvelope struct {
	Command string `json:"command"`
	Data    struct {
		GateSet   store.GateSet `json:"gate_set"`
		LockedNow bool          `json:"locked_now"`
	} `json:"data"`
}

func TestGateTemplateCreateListInstantiateAndLockFlow(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "memori-cli-gate-template-set.db")
	if _, stderr, err := runMemoriForTest("init", "--db", dbPath, "--issue-prefix", "mem", "--json"); err != nil {
		t.Fatalf("init db: %v\nstderr: %s", err, stderr)
	}
	if _, stderr, err := runMemoriForTest(
		"issue", "create",
		"--db", dbPath,
		"--key", "mem-a111111",
		"--type", "task",
		"--title", "Gate set flow issue",
		"--command-id", "cmd-cli-gset-create-1",
		"--json",
	); err != nil {
		t.Fatalf("create issue: %v\nstderr: %s", err, stderr)
	}

	defPath := filepath.Join(t.TempDir(), "quality-gates.json")
	definition := `{"gates":[{"id":"build","kind":"check","required":false,"criteria":{"ref":"manual-build"}},{"id":"lint","kind":"check","required":false,"criteria":{"ref":"manual-lint"}}]}`
	if err := os.WriteFile(defPath, []byte(definition), 0o644); err != nil {
		t.Fatalf("write template definition file: %v", err)
	}

	stdout, stderr, err := runMemoriForTest(
		"gate", "template", "create",
		"--db", dbPath,
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

	stdout, stderr, err = runMemoriForTest("gate", "template", "list", "--db", dbPath, "--type", "task", "--json")
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
		"--db", dbPath,
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
		"--db", dbPath,
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
		"--db", dbPath,
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
		"--db", dbPath,
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
		"--db", dbPath,
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

func TestGateTemplateApproveEnablesExecutableTemplateInstantiation(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "memori-cli-gate-template-approve.db")
	if _, stderr, err := runMemoriForTest("init", "--db", dbPath, "--issue-prefix", "mem", "--json"); err != nil {
		t.Fatalf("init db: %v\nstderr: %s", err, stderr)
	}
	if _, stderr, err := runMemoriForTest(
		"issue", "create",
		"--db", dbPath,
		"--key", "mem-a212121",
		"--type", "task",
		"--title", "Approval workflow issue",
		"--command-id", "cmd-cli-gtemplate-approve-issue-1",
		"--json",
	); err != nil {
		t.Fatalf("create issue: %v\nstderr: %s", err, stderr)
	}

	defPath := filepath.Join(t.TempDir(), "approval-gates.json")
	definition := `{"gates":[{"id":"build","kind":"check","required":true,"criteria":{"command":"go test ./..."}}]}`
	if err := os.WriteFile(defPath, []byte(definition), 0o644); err != nil {
		t.Fatalf("write template definition file: %v", err)
	}

	stdout, stderr, err := runMemoriForTest(
		"gate", "template", "create",
		"--db", dbPath,
		"--id", "approval",
		"--version", "1",
		"--applies-to", "task",
		"--file", defPath,
		"--command-id", "cmd-cli-gtemplate-approve-create-1",
		"--json",
	)
	if err != nil {
		t.Fatalf("gate template create: %v\nstderr: %s", err, stderr)
	}
	var created gateTemplateCreateEnvelope
	if err := json.Unmarshal([]byte(stdout), &created); err != nil {
		t.Fatalf("decode gate template create json: %v\nstdout: %s", err, stdout)
	}
	if !created.Data.Template.Executable {
		t.Fatalf("expected executable template in create response")
	}
	if created.Data.Template.ApprovedBy != "" {
		t.Fatalf("expected LLM-authored executable template to start unapproved, got approved_by=%q", created.Data.Template.ApprovedBy)
	}

	stdout, stderr, err = runMemoriForTest("gate", "template", "pending", "--db", dbPath, "--json")
	if err != nil {
		t.Fatalf("gate template pending before approval: %v\nstderr: %s", err, stderr)
	}
	var pendingBefore gateTemplateListEnvelope
	if err := json.Unmarshal([]byte(stdout), &pendingBefore); err != nil {
		t.Fatalf("decode gate template pending before approval json: %v\nstdout: %s", err, stdout)
	}
	if pendingBefore.Data.Count != 1 || len(pendingBefore.Data.Templates) != 1 {
		t.Fatalf("expected one pending template before approval, got count=%d templates=%d", pendingBefore.Data.Count, len(pendingBefore.Data.Templates))
	}
	if pendingBefore.Data.Templates[0].TemplateID != "approval" {
		t.Fatalf("expected approval template in pending queue before approval, got %+v", pendingBefore.Data.Templates[0])
	}

	_, _, err = runMemoriForTest(
		"gate", "set", "instantiate",
		"--db", dbPath,
		"--issue", "mem-a212121",
		"--template", "approval@1",
		"--command-id", "cmd-cli-gtemplate-approve-instantiate-pre-1",
		"--json",
	)
	if err == nil || !strings.Contains(err.Error(), "pending human approval") {
		t.Fatalf("expected approval gate-set rejection, got: %v", err)
	}

	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	seedCLIHumanCredential(t, s, "correct horse battery")
	if err := s.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	t.Setenv("MEMORI_PRINCIPAL", "human")
	originalPrompter := passwordPrompter
	passwordPrompter = func(string) (string, error) {
		return "correct horse battery", nil
	}
	defer func() {
		passwordPrompter = originalPrompter
	}()

	stdout, stderr, err = runMemoriForTest(
		"gate", "template", "approve",
		"--db", dbPath,
		"--id", "approval",
		"--version", "1",
		"--command-id", "cmd-cli-gtemplate-approve-human-1",
		"--json",
	)
	if err != nil {
		t.Fatalf("gate template approve: %v\nstderr: %s", err, stderr)
	}
	var approved gateTemplateApproveEnvelope
	if err := json.Unmarshal([]byte(stdout), &approved); err != nil {
		t.Fatalf("decode gate template approve json: %v\nstdout: %s", err, stdout)
	}
	if approved.Command != "gate template approve" {
		t.Fatalf("expected gate template approve command, got %q", approved.Command)
	}
	if !strings.HasPrefix(approved.Data.Template.ApprovedBy, "human:") {
		t.Fatalf("expected human approval actor, got %q", approved.Data.Template.ApprovedBy)
	}

	stdout, stderr, err = runMemoriForTest("gate", "template", "pending", "--db", dbPath, "--json")
	if err != nil {
		t.Fatalf("gate template pending after approval json: %v\nstderr: %s", err, stderr)
	}
	var pendingAfter gateTemplateListEnvelope
	if err := json.Unmarshal([]byte(stdout), &pendingAfter); err != nil {
		t.Fatalf("decode gate template pending after approval json: %v\nstdout: %s", err, stdout)
	}
	if pendingAfter.Data.Count != 0 || len(pendingAfter.Data.Templates) != 0 {
		t.Fatalf("expected no pending templates after approval, got count=%d templates=%d", pendingAfter.Data.Count, len(pendingAfter.Data.Templates))
	}

	stdout, stderr, err = runMemoriForTest("gate", "template", "pending", "--db", dbPath)
	if err != nil {
		t.Fatalf("gate template pending after approval text: %v\nstderr: %s", err, stderr)
	}
	if !strings.Contains(stdout, "No pending executable gate templates.") {
		t.Fatalf("expected empty pending queue message after approval, got %q", stdout)
	}

	t.Setenv("MEMORI_PRINCIPAL", "llm")
	stdout, stderr, err = runMemoriForTest(
		"gate", "set", "instantiate",
		"--db", dbPath,
		"--issue", "mem-a212121",
		"--template", "approval@1",
		"--command-id", "cmd-cli-gtemplate-approve-instantiate-post-1",
		"--json",
	)
	if err != nil {
		t.Fatalf("instantiate approved gate set: %v\nstderr: %s", err, stderr)
	}
	var instantiated gateSetInstantiateEnvelope
	if err := json.Unmarshal([]byte(stdout), &instantiated); err != nil {
		t.Fatalf("decode gate set instantiate json: %v\nstdout: %s", err, stdout)
	}
	if instantiated.Data.GateSet.GateSetID == "" {
		t.Fatalf("expected instantiated gate set id after approval")
	}
}

func TestGateTemplatePendingListsPendingExecutableTemplates(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "memori-cli-gate-template-pending.db")
	if _, stderr, err := runMemoriForTest("init", "--db", dbPath, "--issue-prefix", "mem", "--json"); err != nil {
		t.Fatalf("init db: %v\nstderr: %s", err, stderr)
	}

	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer s.Close()

	ctx := context.Background()
	if _, _, err := s.CreateGateTemplate(ctx, store.CreateGateTemplateParams{
		TemplateID:     "pending-exec",
		Version:        1,
		AppliesTo:      []string{"task"},
		DefinitionJSON: `{"gates":[{"id":"build","criteria":{"command":"go test ./..."}}]}`,
		Actor:          "llm:openai:gpt-5",
		CommandID:      "cmd-cli-gtemplate-pending-create-1",
	}); err != nil {
		t.Fatalf("create pending executable template: %v", err)
	}
	if _, _, err := s.CreateGateTemplate(ctx, store.CreateGateTemplateParams{
		TemplateID:     "approved-exec",
		Version:        1,
		AppliesTo:      []string{"task"},
		DefinitionJSON: `{"gates":[{"id":"lint","criteria":{"command":"go test ./internal/store"}}]}`,
		Actor:          "human:alice",
		CommandID:      "cmd-cli-gtemplate-pending-create-2",
	}); err != nil {
		t.Fatalf("create approved executable template: %v", err)
	}
	if _, _, err := s.CreateGateTemplate(ctx, store.CreateGateTemplateParams{
		TemplateID:     "manual-check",
		Version:        1,
		AppliesTo:      []string{"task"},
		DefinitionJSON: `{"gates":[{"id":"review","criteria":{"ref":"manual-validation"}}]}`,
		Actor:          "llm:openai:gpt-5",
		CommandID:      "cmd-cli-gtemplate-pending-create-3",
	}); err != nil {
		t.Fatalf("create manual template: %v", err)
	}

	stdout, stderr, err := runMemoriForTest("gate", "template", "pending", "--db", dbPath, "--json")
	if err != nil {
		t.Fatalf("gate template pending json: %v\nstderr: %s", err, stderr)
	}
	var pending gateTemplateListEnvelope
	if err := json.Unmarshal([]byte(stdout), &pending); err != nil {
		t.Fatalf("decode gate template pending json: %v\nstdout: %s", err, stdout)
	}
	if pending.Command != "gate template pending" {
		t.Fatalf("expected gate template pending command, got %q", pending.Command)
	}
	if pending.Data.Count != 1 || len(pending.Data.Templates) != 1 {
		t.Fatalf("expected exactly one pending template, got count=%d templates=%d", pending.Data.Count, len(pending.Data.Templates))
	}
	if pending.Data.Templates[0].TemplateID != "pending-exec" {
		t.Fatalf("expected pending-exec template, got %+v", pending.Data.Templates[0])
	}
	if pending.Data.Templates[0].ApprovedBy != "" || !pending.Data.Templates[0].Executable {
		t.Fatalf("expected unapproved executable template, got %+v", pending.Data.Templates[0])
	}

	stdout, stderr, err = runMemoriForTest("gate", "template", "pending", "--db", dbPath)
	if err != nil {
		t.Fatalf("gate template pending text: %v\nstderr: %s", err, stderr)
	}
	if !strings.Contains(stdout, "pending-exec@1") || !strings.Contains(stdout, "approval=pending-human-review") {
		t.Fatalf("expected readable pending approval output, got %q", stdout)
	}
	if strings.Contains(stdout, "approved-exec") || strings.Contains(stdout, "manual-check") {
		t.Fatalf("expected only pending executable template in text output, got %q", stdout)
	}
}

func TestGateSetInstantiateRejectsTemplateTypeMismatch(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "memori-cli-gate-template-mismatch.db")
	if _, stderr, err := runMemoriForTest("init", "--db", dbPath, "--issue-prefix", "mem", "--json"); err != nil {
		t.Fatalf("init db: %v\nstderr: %s", err, stderr)
	}
	if _, stderr, err := runMemoriForTest(
		"issue", "create",
		"--db", dbPath,
		"--key", "mem-b111111",
		"--type", "task",
		"--title", "Type mismatch issue",
		"--command-id", "cmd-cli-gset-mismatch-create-1",
		"--json",
	); err != nil {
		t.Fatalf("create issue: %v\nstderr: %s", err, stderr)
	}

	defPath := filepath.Join(t.TempDir(), "story-only-gates.json")
	if err := os.WriteFile(defPath, []byte(`{"gates":[{"id":"story-check"}]}`), 0o644); err != nil {
		t.Fatalf("write template definition file: %v", err)
	}
	if _, stderr, err := runMemoriForTest(
		"gate", "template", "create",
		"--db", dbPath,
		"--id", "storyonly",
		"--version", "1",
		"--applies-to", "story",
		"--file", defPath,
		"--command-id", "cmd-cli-gtemplate-create-mismatch-1",
		"--json",
	); err != nil {
		t.Fatalf("gate template create: %v\nstderr: %s", err, stderr)
	}

	_, _, err := runMemoriForTest(
		"gate", "set", "instantiate",
		"--db", dbPath,
		"--issue", "mem-b111111",
		"--template", "storyonly@1",
		"--json",
	)
	if err == nil || !strings.Contains(err.Error(), "does not apply to issue type Task") {
		t.Fatalf("expected template type mismatch error, got: %v", err)
	}
}

func TestGateSetInstantiateAutoSelectsLatestEligibleTemplateVersion(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "memori-cli-gate-template-autoselect.db")
	if _, stderr, err := runMemoriForTest("init", "--db", dbPath, "--issue-prefix", "mem", "--json"); err != nil {
		t.Fatalf("init db: %v\nstderr: %s", err, stderr)
	}
	if _, stderr, err := runMemoriForTest(
		"issue", "create",
		"--db", dbPath,
		"--key", "mem-a211111",
		"--type", "story",
		"--title", "Auto-select story close template",
		"--command-id", "cmd-cli-gset-autoselect-create-1",
		"--json",
	); err != nil {
		t.Fatalf("create issue: %v\nstderr: %s", err, stderr)
	}

	defV1Path := filepath.Join(t.TempDir(), "close-story-v1.json")
	if err := os.WriteFile(defV1Path, []byte(`{"gates":[{"id":"verify-v1","kind":"check","required":true,"criteria":{"command":"echo v1"}}]}`), 0o644); err != nil {
		t.Fatalf("write template definition file v1: %v", err)
	}
	if _, stderr, err := runMemoriForTest(
		"gate", "template", "create",
		"--db", dbPath,
		"--id", "close-story",
		"--version", "1",
		"--applies-to", "story",
		"--file", defV1Path,
		"--command-id", "cmd-cli-gtemplate-autoselect-create-v1",
		"--json",
	); err != nil {
		t.Fatalf("gate template create v1: %v\nstderr: %s", err, stderr)
	}
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer s.Close()
	ctx := context.Background()
	if _, _, err := s.ApproveGateTemplate(ctx, store.ApproveGateTemplateParams{
		TemplateID: "close-story",
		Version:    1,
		Actor:      "human:will",
		CommandID:  "cmd-cli-gtemplate-autoselect-approve-v1",
	}); err != nil {
		t.Fatalf("approve gate template v1 via store: %v", err)
	}

	defV2Path := filepath.Join(t.TempDir(), "close-story-v2.json")
	if err := os.WriteFile(defV2Path, []byte(`{"gates":[{"id":"verify-v2","kind":"check","required":true,"criteria":{"command":"echo v2"}}]}`), 0o644); err != nil {
		t.Fatalf("write template definition file v2: %v", err)
	}
	if _, stderr, err := runMemoriForTest(
		"gate", "template", "create",
		"--db", dbPath,
		"--id", "close-story",
		"--version", "2",
		"--applies-to", "story",
		"--file", defV2Path,
		"--command-id", "cmd-cli-gtemplate-autoselect-create-v2",
		"--json",
	); err != nil {
		t.Fatalf("gate template create v2: %v\nstderr: %s", err, stderr)
	}
	if _, _, err := s.ApproveGateTemplate(ctx, store.ApproveGateTemplateParams{
		TemplateID: "close-story",
		Version:    2,
		Actor:      "human:will",
		CommandID:  "cmd-cli-gtemplate-autoselect-approve-v2",
	}); err != nil {
		t.Fatalf("approve gate template v2 via store: %v", err)
	}

	stdout, stderr, err := runMemoriForTest(
		"gate", "set", "instantiate",
		"--db", dbPath,
		"--issue", "mem-a211111",
		"--command-id", "cmd-cli-gset-autoselect-instantiate-1",
		"--json",
	)
	if err != nil {
		t.Fatalf("gate set instantiate: %v\nstderr: %s", err, stderr)
	}

	var instantiated gateSetInstantiateEnvelope
	if err := json.Unmarshal([]byte(stdout), &instantiated); err != nil {
		t.Fatalf("decode gate set instantiate json: %v\nstdout: %s", err, stdout)
	}
	if !instantiated.Data.AutoSelected {
		t.Fatalf("expected gate set instantiate to report auto_selected")
	}
	if len(instantiated.Data.GateSet.TemplateRefs) != 1 || instantiated.Data.GateSet.TemplateRefs[0] != "close-story@2" {
		t.Fatalf("expected auto-selected latest eligible template, got %+v", instantiated.Data.GateSet.TemplateRefs)
	}
}

func TestGateSetInstantiateWithoutTemplateRejectsAmbiguousEligibleTemplates(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "memori-cli-gate-template-ambiguous.db")
	if _, stderr, err := runMemoriForTest("init", "--db", dbPath, "--issue-prefix", "mem", "--json"); err != nil {
		t.Fatalf("init db: %v\nstderr: %s", err, stderr)
	}
	if _, stderr, err := runMemoriForTest(
		"issue", "create",
		"--db", dbPath,
		"--key", "mem-a311111",
		"--type", "task",
		"--title", "Ambiguous close template issue",
		"--command-id", "cmd-cli-gset-ambiguous-create-1",
		"--json",
	); err != nil {
		t.Fatalf("create issue: %v\nstderr: %s", err, stderr)
	}

	qualityPath := filepath.Join(t.TempDir(), "quality.json")
	if err := os.WriteFile(qualityPath, []byte(`{"gates":[{"id":"build","kind":"check","required":true,"criteria":{"ref":"manual-build"}}]}`), 0o644); err != nil {
		t.Fatalf("write quality template definition: %v", err)
	}
	if _, stderr, err := runMemoriForTest(
		"gate", "template", "create",
		"--db", dbPath,
		"--id", "quality",
		"--version", "1",
		"--applies-to", "task",
		"--file", qualityPath,
		"--command-id", "cmd-cli-gtemplate-ambiguous-quality-1",
		"--json",
	); err != nil {
		t.Fatalf("create quality template: %v\nstderr: %s", err, stderr)
	}

	provenancePath := filepath.Join(t.TempDir(), "provenance.json")
	if err := os.WriteFile(provenancePath, []byte(`{"gates":[{"id":"audit","kind":"check","required":true,"criteria":{"ref":"manual-audit"}}]}`), 0o644); err != nil {
		t.Fatalf("write provenance template definition: %v", err)
	}
	if _, stderr, err := runMemoriForTest(
		"gate", "template", "create",
		"--db", dbPath,
		"--id", "provenance",
		"--version", "1",
		"--applies-to", "task",
		"--file", provenancePath,
		"--command-id", "cmd-cli-gtemplate-ambiguous-provenance-1",
		"--json",
	); err != nil {
		t.Fatalf("create provenance template: %v\nstderr: %s", err, stderr)
	}

	_, _, err := runMemoriForTest(
		"gate", "set", "instantiate",
		"--db", dbPath,
		"--issue", "mem-a311111",
		"--command-id", "cmd-cli-gset-ambiguous-instantiate-1",
		"--json",
	)
	if err == nil || !strings.Contains(err.Error(), "multiple eligible gate templates apply to issue type Task; specify --template explicitly: provenance@1, quality@1") {
		t.Fatalf("expected ambiguous template error, got: %v", err)
	}
}

func TestGateSetInstantiateWithoutTemplateExplainsPendingApproval(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "memori-cli-gate-template-pending.db")
	if _, stderr, err := runMemoriForTest("init", "--db", dbPath, "--issue-prefix", "mem", "--json"); err != nil {
		t.Fatalf("init db: %v\nstderr: %s", err, stderr)
	}
	if _, stderr, err := runMemoriForTest(
		"issue", "create",
		"--db", dbPath,
		"--key", "mem-a411111",
		"--type", "story",
		"--title", "Pending approval close template issue",
		"--command-id", "cmd-cli-gset-pending-create-1",
		"--json",
	); err != nil {
		t.Fatalf("create issue: %v\nstderr: %s", err, stderr)
	}

	defPath := filepath.Join(t.TempDir(), "pending-close-story.json")
	if err := os.WriteFile(defPath, []byte(`{"gates":[{"id":"verify","kind":"check","required":true,"criteria":{"command":"echo pending"}}]}`), 0o644); err != nil {
		t.Fatalf("write pending template definition: %v", err)
	}
	if _, stderr, err := runMemoriForTest(
		"gate", "template", "create",
		"--db", dbPath,
		"--id", "close-story",
		"--version", "1",
		"--applies-to", "story",
		"--file", defPath,
		"--command-id", "cmd-cli-gtemplate-pending-create-1",
		"--json",
	); err != nil {
		t.Fatalf("create pending close template: %v\nstderr: %s", err, stderr)
	}

	_, _, err := runMemoriForTest(
		"gate", "set", "instantiate",
		"--db", dbPath,
		"--issue", "mem-a411111",
		"--command-id", "cmd-cli-gset-pending-instantiate-1",
		"--json",
	)
	if err == nil || !strings.Contains(err.Error(), "no eligible gate templates apply to issue type Story; pending approval: close-story@1") {
		t.Fatalf("expected pending approval error, got: %v", err)
	}
}
