package store

import (
	"context"
	"strings"
	"testing"
)

func TestGateTemplatesImmutabilityTriggersBlockMutation(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	_, err := s.db.ExecContext(ctx, `
		INSERT INTO gate_templates(
			template_id, version, applies_to_json, definition_json,
			definition_hash, created_at, created_by
		) VALUES(?, ?, ?, ?, ?, ?, ?)
	`, "tmpl-default", 1, `["Task"]`, `{"gates":[{"id":"build"}]}`, "tmplhash1", nowUTC(), "agent-1")
	if err != nil {
		t.Fatalf("insert gate template: %v", err)
	}

	_, err = s.db.ExecContext(ctx, `
		UPDATE gate_templates SET definition_json = ? WHERE template_id = ? AND version = ?
	`, `{"gates":[{"id":"lint"}]}`, "tmpl-default", 1)
	if err == nil {
		t.Fatalf("expected update on gate_templates to fail due to immutability trigger")
	}
	if !strings.Contains(err.Error(), "gate_templates are immutable") {
		t.Fatalf("expected gate template immutability error, got: %v", err)
	}

	_, err = s.db.ExecContext(ctx, `
		DELETE FROM gate_templates WHERE template_id = ? AND version = ?
	`, "tmpl-default", 1)
	if err == nil {
		t.Fatalf("expected delete on gate_templates to fail due to immutability trigger")
	}
	if !strings.Contains(err.Error(), "gate_templates are immutable") {
		t.Fatalf("expected gate template delete immutability error, got: %v", err)
	}
}

func TestGateSetItemsImmutabilityTriggersBlockMutation(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	_, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   "mem-1212121",
		Type:      "task",
		Title:     "Gate item trigger test",
		Actor:     "agent-1",
		CommandID: "cmd-gate-item-1",
	})
	if err != nil {
		t.Fatalf("create issue: %v", err)
	}

	_, err = s.db.ExecContext(ctx, `
		INSERT INTO gate_sets(
			gate_set_id, issue_id, cycle_no, template_refs_json, frozen_definition_json,
			gate_set_hash, locked_at, created_at, created_by
		) VALUES(?, ?, ?, ?, ?, ?, NULL, ?, ?)
	`, "gs_1", "mem-1212121", 1, `["tmpl-default@1"]`, `{"gates":[{"id":"build"}]}`, "gshash1", nowUTC(), "agent-1")
	if err != nil {
		t.Fatalf("insert gate set: %v", err)
	}

	_, err = s.db.ExecContext(ctx, `
		INSERT INTO gate_set_items(gate_set_id, gate_id, kind, required, criteria_json)
		VALUES(?, ?, ?, ?, ?)
	`, "gs_1", "build", "check", 1, `{"command":"go test ./..."}`)
	if err != nil {
		t.Fatalf("insert gate set item: %v", err)
	}

	_, err = s.db.ExecContext(ctx, `
		UPDATE gate_set_items SET required = 0 WHERE gate_set_id = ? AND gate_id = ?
	`, "gs_1", "build")
	if err == nil {
		t.Fatalf("expected update on gate_set_items to fail due to immutability trigger")
	}
	if !strings.Contains(err.Error(), "gate_set_items are immutable") {
		t.Fatalf("expected gate_set_items immutability error, got: %v", err)
	}

	_, err = s.db.ExecContext(ctx, `
		DELETE FROM gate_set_items WHERE gate_set_id = ? AND gate_id = ?
	`, "gs_1", "build")
	if err == nil {
		t.Fatalf("expected delete on gate_set_items to fail due to immutability trigger")
	}
	if !strings.Contains(err.Error(), "gate_set_items are immutable") {
		t.Fatalf("expected gate_set_items delete immutability error, got: %v", err)
	}
}

func TestGateSetsImmutabilityTriggersEnforceFrozenFieldsAndLocking(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	_, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   "mem-2323232",
		Type:      "task",
		Title:     "Gate set trigger test",
		Actor:     "agent-1",
		CommandID: "cmd-gate-set-1",
	})
	if err != nil {
		t.Fatalf("create issue: %v", err)
	}

	createdAt := nowUTC()
	_, err = s.db.ExecContext(ctx, `
		INSERT INTO gate_sets(
			gate_set_id, issue_id, cycle_no, template_refs_json, frozen_definition_json,
			gate_set_hash, locked_at, created_at, created_by
		) VALUES(?, ?, ?, ?, ?, ?, NULL, ?, ?)
	`, "gs_2", "mem-2323232", 1, `["tmpl-default@1"]`, `{"gates":[{"id":"build"}]}`, "gshash2", createdAt, "agent-1")
	if err != nil {
		t.Fatalf("insert gate set: %v", err)
	}

	_, err = s.db.ExecContext(ctx, `
		UPDATE gate_sets SET template_refs_json = ? WHERE gate_set_id = ?
	`, `["tmpl-default@2"]`, "gs_2")
	if err == nil {
		t.Fatalf("expected frozen field update to fail")
	}
	if !strings.Contains(err.Error(), "gate_set definitions are immutable") {
		t.Fatalf("expected frozen definition immutability error, got: %v", err)
	}

	lockedAt := nowUTC()
	_, err = s.db.ExecContext(ctx, `
		UPDATE gate_sets SET locked_at = ? WHERE gate_set_id = ?
	`, lockedAt, "gs_2")
	if err != nil {
		t.Fatalf("lock gate set (set locked_at once): %v", err)
	}

	_, err = s.db.ExecContext(ctx, `
		UPDATE gate_sets SET created_by = ? WHERE gate_set_id = ?
	`, "agent-2", "gs_2")
	if err == nil {
		t.Fatalf("expected update on locked gate_set to fail")
	}
	if !strings.Contains(err.Error(), "locked gate_sets are immutable") {
		t.Fatalf("expected locked gate_set immutability error, got: %v", err)
	}

	_, err = s.db.ExecContext(ctx, `
		UPDATE gate_sets SET locked_at = ? WHERE gate_set_id = ?
	`, nowUTC(), "gs_2")
	if err == nil {
		t.Fatalf("expected second lock attempt to fail")
	}
	if !strings.Contains(err.Error(), "gate_set is already locked") {
		t.Fatalf("expected clear lock no-op rejection error, got: %v", err)
	}

	_, err = s.db.ExecContext(ctx, `
		DELETE FROM gate_sets WHERE gate_set_id = ?
	`, "gs_2")
	if err == nil {
		t.Fatalf("expected delete on gate_sets to fail due to immutability trigger")
	}
	if !strings.Contains(err.Error(), "gate_sets are immutable") {
		t.Fatalf("expected gate_sets delete immutability error, got: %v", err)
	}
}

func TestListPendingExecutableGateTemplates(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	pending, _, err := s.CreateGateTemplate(ctx, CreateGateTemplateParams{
		TemplateID:     "pending-exec",
		Version:        1,
		AppliesTo:      []string{"task"},
		DefinitionJSON: `{"gates":[{"id":"build","criteria":{"command":"go test ./..."}}]}`,
		Actor:          "llm:openai:gpt-5",
		CommandID:      "cmd-pending-template-1",
	})
	if err != nil {
		t.Fatalf("create pending executable template: %v", err)
	}

	if _, _, err := s.CreateGateTemplate(ctx, CreateGateTemplateParams{
		TemplateID:     "approved-exec",
		Version:        1,
		AppliesTo:      []string{"task"},
		DefinitionJSON: `{"gates":[{"id":"lint","criteria":{"command":"go test ./internal/store"}}]}`,
		Actor:          "human:alice",
		CommandID:      "cmd-approved-template-1",
	}); err != nil {
		t.Fatalf("create approved executable template: %v", err)
	}

	if _, _, err := s.CreateGateTemplate(ctx, CreateGateTemplateParams{
		TemplateID:     "manual-check",
		Version:        1,
		AppliesTo:      []string{"task"},
		DefinitionJSON: `{"gates":[{"id":"review","criteria":{"ref":"manual-validation"}}]}`,
		Actor:          "llm:openai:gpt-5",
		CommandID:      "cmd-manual-template-1",
	}); err != nil {
		t.Fatalf("create non-executable template: %v", err)
	}

	templates, err := s.ListPendingExecutableGateTemplates(ctx)
	if err != nil {
		t.Fatalf("list pending executable gate templates: %v", err)
	}
	if len(templates) != 1 {
		t.Fatalf("expected 1 pending executable template, got %d", len(templates))
	}

	got := templates[0]
	if got.TemplateID != pending.TemplateID || got.Version != pending.Version {
		t.Fatalf("expected pending template %s@%d, got %s@%d", pending.TemplateID, pending.Version, got.TemplateID, got.Version)
	}
	if !got.Executable {
		t.Fatalf("expected pending template to be executable")
	}
	if got.ApprovedBy != "" || got.ApprovedAt != "" {
		t.Fatalf("expected pending template approval fields to be empty, got approved_by=%q approved_at=%q", got.ApprovedBy, got.ApprovedAt)
	}
	if got.DefinitionHash == "" || got.CreatedBy == "" || got.CreatedAt == "" {
		t.Fatalf("expected pending template provenance metadata, got %+v", got)
	}
}

func TestApproveGateTemplateRemovesTemplateFromPendingList(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	if _, _, err := s.CreateGateTemplate(ctx, CreateGateTemplateParams{
		TemplateID:     "agent-authored",
		Version:        1,
		AppliesTo:      []string{"task"},
		DefinitionJSON: `{"gates":[{"id":"build","criteria":{"command":"go test ./..."}}]}`,
		Actor:          "llm:openai:gpt-5",
		CommandID:      "cmd-pending-approve-create-1",
	}); err != nil {
		t.Fatalf("create pending executable template: %v", err)
	}

	pending, err := s.ListPendingExecutableGateTemplates(ctx)
	if err != nil {
		t.Fatalf("list pending executable templates before approval: %v", err)
	}
	if len(pending) != 1 {
		t.Fatalf("expected 1 pending executable template before approval, got %d", len(pending))
	}

	approved, _, err := s.ApproveGateTemplate(ctx, ApproveGateTemplateParams{
		TemplateID: "agent-authored",
		Version:    1,
		Actor:      "human:alice",
		CommandID:  "cmd-pending-approve-human-1",
	})
	if err != nil {
		t.Fatalf("approve executable template: %v", err)
	}
	if approved.ApprovedBy != "human:alice" {
		t.Fatalf("expected approval actor recorded, got %q", approved.ApprovedBy)
	}

	pending, err = s.ListPendingExecutableGateTemplates(ctx)
	if err != nil {
		t.Fatalf("list pending executable templates after approval: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("expected no pending executable templates after approval, got %d", len(pending))
	}
}

func TestInstantiateGateSetRejectsExecutableTemplateWithoutHumanGovernance(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	issueID := "mem-5858585"
	if _, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   issueID,
		Type:      "task",
		Title:     "Unsafe template instantiate test",
		Actor:     "agent-1",
		CommandID: "cmd-gate-unsafe-create-1",
	}); err != nil {
		t.Fatalf("create issue: %v", err)
	}
	if _, _, _, err := s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
		IssueID:   issueID,
		Status:    "inprogress",
		Actor:     "agent-1",
		CommandID: "cmd-gate-unsafe-progress-1",
	}); err != nil {
		t.Fatalf("move issue to inprogress: %v", err)
	}
	seedGateTemplateRowForTest(t, s, "unsafe", 1, []string{"Task"}, `{"gates":[{"id":"build","criteria":{"command":"go test ./..."}}]}`, "llm:openai:gpt-5")

	_, _, err := s.InstantiateGateSet(ctx, InstantiateGateSetParams{
		IssueID:      issueID,
		TemplateRefs: []string{"unsafe@1"},
		Actor:        "human:alice",
		CommandID:    "cmd-gate-unsafe-instantiate-1",
	})
	if err == nil || !strings.Contains(err.Error(), "pending human approval") {
		t.Fatalf("expected approval rejection, got: %v", err)
	}
}

func TestApproveGateTemplateAllowsAgentAuthoredExecutableTemplateAfterHumanApproval(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	issueID := "mem-5868686"
	if _, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   issueID,
		Type:      "task",
		Title:     "Executable template approval workflow",
		Actor:     "agent-1",
		CommandID: "cmd-gate-approve-create-1",
	}); err != nil {
		t.Fatalf("create issue: %v", err)
	}
	if _, _, _, err := s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
		IssueID:   issueID,
		Status:    "inprogress",
		Actor:     "agent-1",
		CommandID: "cmd-gate-approve-progress-1",
	}); err != nil {
		t.Fatalf("move issue to inprogress: %v", err)
	}

	template, _, err := s.CreateGateTemplate(ctx, CreateGateTemplateParams{
		TemplateID:     "agent-authored",
		Version:        1,
		AppliesTo:      []string{"task"},
		DefinitionJSON: `{"gates":[{"id":"build","criteria":{"command":"go test ./..."}}]}`,
		Actor:          "llm:openai:gpt-5",
		CommandID:      "cmd-gate-approve-template-1",
	})
	if err != nil {
		t.Fatalf("create executable template: %v", err)
	}
	if !template.Executable {
		t.Fatalf("expected executable template")
	}
	if template.ApprovedBy != "" {
		t.Fatalf("expected executable template to start unapproved, got approved_by=%q", template.ApprovedBy)
	}

	_, _, err = s.InstantiateGateSet(ctx, InstantiateGateSetParams{
		IssueID:      issueID,
		TemplateRefs: []string{"agent-authored@1"},
		Actor:        "human:alice",
		CommandID:    "cmd-gate-approve-instantiate-pre-1",
	})
	if err == nil || !strings.Contains(err.Error(), "pending human approval") {
		t.Fatalf("expected pre-approval instantiate rejection, got: %v", err)
	}

	approved, idempotent, err := s.ApproveGateTemplate(ctx, ApproveGateTemplateParams{
		TemplateID: "agent-authored",
		Version:    1,
		Actor:      "human:alice",
		CommandID:  "cmd-gate-approve-template-approve-1",
	})
	if err != nil {
		t.Fatalf("approve executable template: %v", err)
	}
	if idempotent {
		t.Fatalf("expected first approval to be non-idempotent")
	}
	if approved.ApprovedBy != "human:alice" {
		t.Fatalf("expected approval actor recorded, got %q", approved.ApprovedBy)
	}

	gateSet, _, err := s.InstantiateGateSet(ctx, InstantiateGateSetParams{
		IssueID:      issueID,
		TemplateRefs: []string{"agent-authored@1"},
		Actor:        "human:alice",
		CommandID:    "cmd-gate-approve-instantiate-post-1",
	})
	if err != nil {
		t.Fatalf("instantiate approved template: %v", err)
	}
	if gateSet.GateSetID == "" {
		t.Fatalf("expected instantiated gate set id")
	}
}

func TestInstantiateGateSetRejectsRequiredNonExecutableTemplate(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	issueID := "mem-5878787"
	if _, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   issueID,
		Type:      "task",
		Title:     "Manual required gate instantiate test",
		Actor:     "human:alice",
		CommandID: "cmd-gate-manual-create-1",
	}); err != nil {
		t.Fatalf("create issue: %v", err)
	}
	if _, _, _, err := s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
		IssueID:   issueID,
		Status:    "inprogress",
		Actor:     "human:alice",
		CommandID: "cmd-gate-manual-progress-1",
	}); err != nil {
		t.Fatalf("move issue to inprogress: %v", err)
	}
	if _, _, err := s.CreateGateTemplate(ctx, CreateGateTemplateParams{
		TemplateID:     "manual-required",
		Version:        1,
		AppliesTo:      []string{"task"},
		DefinitionJSON: `{"gates":[{"id":"review","kind":"check","required":true,"criteria":{"ref":"manual-review"}}]}`,
		Actor:          "human:alice",
		CommandID:      "cmd-gate-manual-template-1",
	}); err != nil {
		t.Fatalf("create manual gate template: %v", err)
	}

	_, _, err := s.InstantiateGateSet(ctx, InstantiateGateSetParams{
		IssueID:      issueID,
		TemplateRefs: []string{"manual-required@1"},
		Actor:        "human:alice",
		CommandID:    "cmd-gate-manual-instantiate-1",
	})
	if err == nil || !strings.Contains(err.Error(), "required gate(s) lack executable criteria.command") {
		t.Fatalf("expected required non-executable gate rejection, got: %v", err)
	}
}

func TestInstantiateGateSetAllowsRequiredManualValidationTemplate(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	issueID := "mem-5879797"
	if _, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   issueID,
		Type:      "task",
		Title:     "Manual validation instantiate test",
		Actor:     "human:alice",
		CommandID: "cmd-gate-manual-validation-create-1",
	}); err != nil {
		t.Fatalf("create issue: %v", err)
	}
	if _, _, _, err := s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
		IssueID:   issueID,
		Status:    "inprogress",
		Actor:     "human:alice",
		CommandID: "cmd-gate-manual-validation-progress-1",
	}); err != nil {
		t.Fatalf("move issue to inprogress: %v", err)
	}
	if _, _, err := s.CreateGateTemplate(ctx, CreateGateTemplateParams{
		TemplateID:     "manual-validation-required",
		Version:        1,
		AppliesTo:      []string{"task"},
		DefinitionJSON: `{"gates":[{"id":"validated","kind":"check","required":true,"criteria":{"ref":"manual-validation"}}]}`,
		Actor:          "human:alice",
		CommandID:      "cmd-gate-manual-validation-template-1",
	}); err != nil {
		t.Fatalf("create manual-validation gate template: %v", err)
	}

	gateSet, _, err := s.InstantiateGateSet(ctx, InstantiateGateSetParams{
		IssueID:      issueID,
		TemplateRefs: []string{"manual-validation-required@1"},
		Actor:        "human:alice",
		CommandID:    "cmd-gate-manual-validation-instantiate-1",
	})
	if err != nil {
		t.Fatalf("expected required manual-validation gate set to instantiate: %v", err)
	}
	if gateSet.GateSetID == "" {
		t.Fatalf("expected gate set id")
	}
}
