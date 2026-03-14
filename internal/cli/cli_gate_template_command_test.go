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

func TestGateTemplateTextFlowsCoverCreateListAndApproveMessages(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "memori-cli-gate-template-text.db")
	if _, stderr, err := runMemoriForTest("init", "--db", dbPath, "--issue-prefix", "mem", "--json"); err != nil {
		t.Fatalf("init db: %v\nstderr: %s", err, stderr)
	}

	defPath := filepath.Join(t.TempDir(), "approval-text-gates.json")
	definition := `{"gates":[{"id":"build","kind":"check","required":true,"criteria":{"command":"go test ./..."}}]}`
	if err := os.WriteFile(defPath, []byte(definition), 0o644); err != nil {
		t.Fatalf("write template definition file: %v", err)
	}

	stdout, stderr, err := runMemoriForTest(
		"gate", "template", "create",
		"--db", dbPath,
		"--id", "approval-text",
		"--version", "1",
		"--applies-to", "task",
		"--file", defPath,
		"--command-id", "cmd-cli-gtemplate-text-create-1",
	)
	if err != nil {
		t.Fatalf("gate template create text: %v\nstderr: %s", err, stderr)
	}
	for _, want := range []string{
		"Created gate template approval-text@1",
		"Applies To: Task",
		"Definition Hash:",
		"Approval: pending human approval before instantiate/verify",
	} {
		if !strings.Contains(stdout, want) {
			t.Fatalf("expected create output to contain %q, got:\n%s", want, stdout)
		}
	}

	stdout, stderr, err = runMemoriForTest(
		"gate", "template", "create",
		"--db", dbPath,
		"--id", "approval-text",
		"--version", "1",
		"--applies-to", "task",
		"--file", defPath,
		"--command-id", "cmd-cli-gtemplate-text-create-1",
	)
	if err != nil {
		t.Fatalf("gate template idempotent create text: %v\nstderr: %s", err, stderr)
	}
	if !strings.Contains(stdout, "Gate template approval-text@1 already exists.") {
		t.Fatalf("expected idempotent create output, got:\n%s", stdout)
	}

	stdout, stderr, err = runMemoriForTest("gate", "template", "list", "--db", dbPath, "--type", "bug")
	if err != nil {
		t.Fatalf("gate template empty list text: %v\nstderr: %s", err, stderr)
	}
	if !strings.Contains(stdout, "No gate templates matched.") {
		t.Fatalf("expected empty list message, got:\n%s", stdout)
	}

	stdout, stderr, err = runMemoriForTest("gate", "template", "list", "--db", dbPath, "--type", "task")
	if err != nil {
		t.Fatalf("gate template list text: %v\nstderr: %s", err, stderr)
	}
	if !strings.Contains(stdout, "- approval-text@1 applies_to=Task hash=") {
		t.Fatalf("expected task list output, got:\n%s", stdout)
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
		"--id", "approval-text",
		"--version", "1",
		"--command-id", "cmd-cli-gtemplate-text-approve-1",
	)
	if err != nil {
		t.Fatalf("gate template approve text: %v\nstderr: %s", err, stderr)
	}
	for _, want := range []string{
		"Approved gate template approval-text@1",
		"Approved By: human:",
		"Approved At:",
	} {
		if !strings.Contains(stdout, want) {
			t.Fatalf("expected approve output to contain %q, got:\n%s", want, stdout)
		}
	}

	stdout, stderr, err = runMemoriForTest(
		"gate", "template", "approve",
		"--db", dbPath,
		"--id", "approval-text",
		"--version", "1",
		"--command-id", "cmd-cli-gtemplate-text-approve-2",
	)
	if err != nil {
		t.Fatalf("gate template already-approved text: %v\nstderr: %s", err, stderr)
	}
	if !strings.Contains(stdout, "Gate template approval-text@1 is already approved.") {
		t.Fatalf("expected already-approved output, got:\n%s", stdout)
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
