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

func TestGateSetInstantiateAndLockHumanOutputCoversTextBranches(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "memori-cli-gate-template-set-text.db")
	if _, stderr, err := runMemoriForTest("init", "--db", dbPath, "--issue-prefix", "mem", "--json"); err != nil {
		t.Fatalf("init db: %v\nstderr: %s", err, stderr)
	}
	if _, stderr, err := runMemoriForTest(
		"issue", "create",
		"--db", dbPath,
		"--key", "mem-a919191",
		"--type", "task",
		"--title", "Gate set text flow issue",
		"--command-id", "cmd-cli-gset-text-issue-1",
		"--json",
	); err != nil {
		t.Fatalf("create issue: %v\nstderr: %s", err, stderr)
	}

	defPath := filepath.Join(t.TempDir(), "quality-text.json")
	definition := `{"gates":[{"id":"build","kind":"check","required":true,"criteria":{"command":"echo build"}}]}`
	if err := os.WriteFile(defPath, []byte(definition), 0o644); err != nil {
		t.Fatalf("write template definition file: %v", err)
	}
	if _, stderr, err := runMemoriForTest(
		"gate", "template", "create",
		"--db", dbPath,
		"--id", "quality-text",
		"--version", "1",
		"--applies-to", "task",
		"--file", defPath,
		"--command-id", "cmd-cli-gset-text-template-1",
		"--json",
	); err != nil {
		t.Fatalf("gate template create: %v\nstderr: %s", err, stderr)
	}
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer s.Close()
	if _, _, err := s.ApproveGateTemplate(context.Background(), store.ApproveGateTemplateParams{
		TemplateID: "quality-text",
		Version:    1,
		Actor:      "human:will",
		CommandID:  "cmd-cli-gset-text-approve-1",
	}); err != nil {
		t.Fatalf("approve gate template via store: %v", err)
	}

	stdout, stderr, err := runMemoriForTest(
		"gate", "set", "instantiate",
		"--db", dbPath,
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
		"--db", dbPath,
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
		"--db", dbPath,
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

func TestGateSetInstantiateAutoSelectedHumanOutputShowsSelectedTemplate(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "memori-cli-gset-autoselect-text.db")
	if _, stderr, err := runMemoriForTest("init", "--db", dbPath, "--issue-prefix", "mem", "--json"); err != nil {
		t.Fatalf("init db: %v\nstderr: %s", err, stderr)
	}
	if _, stderr, err := runMemoriForTest(
		"issue", "create",
		"--db", dbPath,
		"--key", "mem-a818181",
		"--type", "story",
		"--title", "Auto-select close template text issue",
		"--command-id", "cmd-cli-gset-autoselect-text-issue-1",
		"--json",
	); err != nil {
		t.Fatalf("create issue: %v\nstderr: %s", err, stderr)
	}

	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer s.Close()

	ctx := context.Background()
	if _, _, err := s.CreateGateTemplate(ctx, store.CreateGateTemplateParams{
		TemplateID:     "close-story-text",
		Version:        1,
		AppliesTo:      []string{"story"},
		DefinitionJSON: `{"gates":[{"id":"build","kind":"check","required":true,"criteria":{"command":"echo v1"}}]}`,
		Actor:          "human:will",
		CommandID:      "cmd-cli-gset-autoselect-text-template-v1",
	}); err != nil {
		t.Fatalf("create gate template v1 via store: %v", err)
	}
	if _, _, err := s.ApproveGateTemplate(ctx, store.ApproveGateTemplateParams{
		TemplateID: "close-story-text",
		Version:    1,
		Actor:      "human:will",
		CommandID:  "cmd-cli-gset-autoselect-text-approve-v1",
	}); err != nil {
		t.Fatalf("approve gate template v1 via store: %v", err)
	}
	if _, _, err := s.CreateGateTemplate(ctx, store.CreateGateTemplateParams{
		TemplateID:     "close-story-text",
		Version:        2,
		AppliesTo:      []string{"story"},
		DefinitionJSON: `{"gates":[{"id":"verify","kind":"check","required":true,"criteria":{"command":"echo v2"}}]}`,
		Actor:          "human:will",
		CommandID:      "cmd-cli-gset-autoselect-text-template-v2",
	}); err != nil {
		t.Fatalf("create gate template v2 via store: %v", err)
	}
	if _, _, err := s.ApproveGateTemplate(ctx, store.ApproveGateTemplateParams{
		TemplateID: "close-story-text",
		Version:    2,
		Actor:      "human:will",
		CommandID:  "cmd-cli-gset-autoselect-text-approve-v2",
	}); err != nil {
		t.Fatalf("approve gate template v2 via store: %v", err)
	}

	stdout, stderr, err := runMemoriForTest(
		"gate", "set", "instantiate",
		"--db", dbPath,
		"--issue", "mem-a818181",
		"--command-id", "cmd-cli-gset-autoselect-text-instantiate-1",
	)
	if err != nil {
		t.Fatalf("gate set instantiate auto-selected text: %v\nstderr: %s", err, stderr)
	}
	for _, want := range []string{
		"Auto-selected templates: close-story-text@2",
		"Instantiated gate set ",
		"Templates: close-story-text@2",
	} {
		if !strings.Contains(stdout, want) {
			t.Fatalf("expected auto-selected instantiate text output to contain %q, got:\n%s", want, stdout)
		}
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
