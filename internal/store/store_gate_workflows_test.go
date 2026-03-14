package store

import (
	"context"
	"database/sql"
	"strings"
	"testing"
)

func TestCreateGateTemplateVersioningAndListing(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	firstDef := `{"gates":[{"id":"build","required":true}]}`
	created, idempotent, err := s.CreateGateTemplate(ctx, CreateGateTemplateParams{
		TemplateID:     "quality",
		Version:        1,
		AppliesTo:      []string{"task"},
		DefinitionJSON: firstDef,
		Actor:          "agent-1",
		CommandID:      "cmd-template-create-1",
	})
	if err != nil {
		t.Fatalf("create gate template v1: %v", err)
	}
	if idempotent {
		t.Fatalf("expected first template create to be non-idempotent")
	}
	if created.TemplateID != "quality" || created.Version != 1 {
		t.Fatalf("unexpected created template identity: %#v", created)
	}

	same, sameIdempotent, err := s.CreateGateTemplate(ctx, CreateGateTemplateParams{
		TemplateID:     "quality",
		Version:        1,
		AppliesTo:      []string{"task"},
		DefinitionJSON: firstDef,
		Actor:          "agent-1",
		CommandID:      "cmd-template-create-1",
	})
	if err != nil {
		t.Fatalf("idempotent template create retry: %v", err)
	}
	if !sameIdempotent {
		t.Fatalf("expected same template create to be idempotent")
	}
	if same.DefinitionHash != created.DefinitionHash {
		t.Fatalf("expected same hash on idempotent retry, got %s vs %s", same.DefinitionHash, created.DefinitionHash)
	}

	_, _, err = s.CreateGateTemplate(ctx, CreateGateTemplateParams{
		TemplateID:     "quality",
		Version:        1,
		AppliesTo:      []string{"task"},
		DefinitionJSON: `{"gates":[{"id":"build","required":false}]}`,
		Actor:          "agent-1",
		CommandID:      "cmd-template-create-2",
	})
	if err == nil || !strings.Contains(err.Error(), "create a new version") {
		t.Fatalf("expected same-version mutation rejection, got: %v", err)
	}

	_, idempotent, err = s.CreateGateTemplate(ctx, CreateGateTemplateParams{
		TemplateID:     "quality",
		Version:        2,
		AppliesTo:      []string{"task"},
		DefinitionJSON: `{"gates":[{"id":"build","required":false}]}`,
		Actor:          "agent-1",
		CommandID:      "cmd-template-create-3",
	})
	if err != nil {
		t.Fatalf("create gate template v2: %v", err)
	}
	if idempotent {
		t.Fatalf("expected v2 create to be non-idempotent")
	}

	templates, err := s.ListGateTemplates(ctx, ListGateTemplatesParams{IssueType: "task"})
	if err != nil {
		t.Fatalf("list gate templates by type: %v", err)
	}
	if len(templates) != 2 {
		t.Fatalf("expected 2 task templates, got %d", len(templates))
	}
}

func TestCreateGateTemplateCanonicalEquivalenceAndApprovalIdempotency(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	created, idempotent, err := s.CreateGateTemplate(ctx, CreateGateTemplateParams{
		TemplateID: "release-check",
		Version:    1,
		AppliesTo:  []string{" Task ", "task"},
		DefinitionJSON: `{
			"gates": [
				{"id":"build","kind":"check","required":true,"criteria":{"command":"go test ./..."}}
			]
		}`,
		Actor:     "llm:openai:gpt-5",
		CommandID: "cmd-template-canonical-create-1",
	})
	if err != nil {
		t.Fatalf("create canonical gate template: %v", err)
	}
	if idempotent {
		t.Fatal("expected first canonical template create to be non-idempotent")
	}
	if created.TemplateID != "release-check" || created.Version != 1 {
		t.Fatalf("unexpected created canonical template: %#v", created)
	}

	same, idempotent, err := s.CreateGateTemplate(ctx, CreateGateTemplateParams{
		TemplateID:     " release-check ",
		Version:        1,
		AppliesTo:      []string{"task"},
		DefinitionJSON: `{"gates":[{"required":true,"kind":"check","criteria":{"command":"go test ./..."},"id":"build"}]}`,
		Actor:          "llm:openai:gpt-5",
		CommandID:      "cmd-template-canonical-create-2",
	})
	if err != nil {
		t.Fatalf("create equivalent canonical gate template: %v", err)
	}
	if !idempotent {
		t.Fatal("expected equivalent canonical template create to be idempotent")
	}
	if same.DefinitionHash != created.DefinitionHash || !equalStringSlices(same.AppliesTo, []string{"Task"}) {
		t.Fatalf("expected canonical template identity to be preserved, got %#v", same)
	}

	approved, idempotent, err := s.ApproveGateTemplate(ctx, ApproveGateTemplateParams{
		TemplateID: "release-check",
		Version:    1,
		Actor:      "human:alice",
		CommandID:  "cmd-template-canonical-approve-1",
	})
	if err != nil {
		t.Fatalf("approve canonical gate template: %v", err)
	}
	if idempotent {
		t.Fatal("expected first template approval to be non-idempotent")
	}
	if approved.ApprovedBy != "human:alice" || approved.ApprovedAt == "" {
		t.Fatalf("expected approval metadata after first approval, got %#v", approved)
	}

	approvedAgain, idempotent, err := s.ApproveGateTemplate(ctx, ApproveGateTemplateParams{
		TemplateID: "release-check",
		Version:    1,
		Actor:      "human:alice",
		CommandID:  "cmd-template-canonical-approve-2",
	})
	if err != nil {
		t.Fatalf("approve already-approved canonical gate template: %v", err)
	}
	if !idempotent {
		t.Fatal("expected already-approved template approval to be idempotent")
	}
	if approvedAgain.ApprovedBy != approved.ApprovedBy || approvedAgain.ApprovedAt != approved.ApprovedAt {
		t.Fatalf("expected approval metadata to remain stable, got %#v", approvedAgain)
	}
}

func TestInstantiateAndLockGateSetFlow(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	issueID := "mem-7878787"
	_, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   issueID,
		Type:      "task",
		Title:     "Instantiate lock flow",
		Actor:     "agent-1",
		CommandID: "cmd-gset-flow-create-1",
	})
	if err != nil {
		t.Fatalf("create issue: %v", err)
	}

	_, _, err = s.CreateGateTemplate(ctx, CreateGateTemplateParams{
		TemplateID:     "quality",
		Version:        1,
		AppliesTo:      []string{"task"},
		DefinitionJSON: `{"gates":[{"id":"build","kind":"check","required":true,"criteria":{"command":"go test ./..."}},{"id":"lint","kind":"check","required":false}]}`,
		Actor:          "agent-1",
		CommandID:      "cmd-gset-template-create-1",
	})
	if err != nil {
		t.Fatalf("create gate template: %v", err)
	}

	gateSet, idempotent, err := s.InstantiateGateSet(ctx, InstantiateGateSetParams{
		IssueID:      issueID,
		TemplateRefs: []string{"quality@1"},
		Actor:        "agent-1",
		CommandID:    "cmd-gset-instantiate-1",
	})
	if err != nil {
		t.Fatalf("instantiate gate set: %v", err)
	}
	if idempotent {
		t.Fatalf("expected first instantiate to be non-idempotent")
	}
	if gateSet.CycleNo != 1 {
		t.Fatalf("expected cycle 1, got %d", gateSet.CycleNo)
	}
	if len(gateSet.Items) != 2 {
		t.Fatalf("expected 2 gate items, got %d", len(gateSet.Items))
	}

	retrySet, retryIdempotent, err := s.InstantiateGateSet(ctx, InstantiateGateSetParams{
		IssueID:      issueID,
		TemplateRefs: []string{"quality@1"},
		Actor:        "agent-1",
		CommandID:    "cmd-gset-instantiate-1",
	})
	if err != nil {
		t.Fatalf("instantiate gate set retry: %v", err)
	}
	if !retryIdempotent {
		t.Fatalf("expected retry instantiate to be idempotent")
	}
	if retrySet.GateSetID != gateSet.GateSetID {
		t.Fatalf("expected same gate set id on retry, got %q vs %q", retrySet.GateSetID, gateSet.GateSetID)
	}

	locked, lockedNow, err := s.LockGateSet(ctx, LockGateSetParams{
		IssueID:   issueID,
		Actor:     "agent-1",
		CommandID: "cmd-gset-lock-1",
	})
	if err != nil {
		t.Fatalf("lock gate set: %v", err)
	}
	if !lockedNow {
		t.Fatalf("expected first lock call to lock now")
	}
	if strings.TrimSpace(locked.LockedAt) == "" {
		t.Fatalf("expected locked_at timestamp to be set")
	}

	relock, relockNow, err := s.LockGateSet(ctx, LockGateSetParams{
		IssueID:   issueID,
		Actor:     "agent-1",
		CommandID: "cmd-gset-lock-1",
	})
	if err != nil {
		t.Fatalf("lock gate set second time: %v", err)
	}
	if relockNow {
		t.Fatalf("expected second lock call to report already locked")
	}
	if relock.GateSetID != gateSet.GateSetID {
		t.Fatalf("expected same gate set on relock, got %q vs %q", relock.GateSetID, gateSet.GateSetID)
	}

	var activeGateSetID sql.NullString
	if err := s.db.QueryRowContext(ctx, `SELECT active_gate_set_id FROM work_items WHERE id = ?`, issueID).Scan(&activeGateSetID); err != nil {
		t.Fatalf("read active_gate_set_id: %v", err)
	}
	if !activeGateSetID.Valid || activeGateSetID.String != gateSet.GateSetID {
		t.Fatalf("expected active_gate_set_id=%q, got %#v", gateSet.GateSetID, activeGateSetID)
	}
}

func TestInstantiateGateSetNormalizesTemplateRefsForIdempotency(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	issueID := "mem-7a8b9c0"
	if _, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   issueID,
		Type:      "task",
		Title:     "Normalized gate refs",
		Actor:     "agent-1",
		CommandID: "cmd-gset-normalize-create-1",
	}); err != nil {
		t.Fatalf("create issue: %v", err)
	}
	if _, _, err := s.CreateGateTemplate(ctx, CreateGateTemplateParams{
		TemplateID:     "beta",
		Version:        1,
		AppliesTo:      []string{"task"},
		DefinitionJSON: `{"gates":[{"id":"docs","kind":"check","required":true,"criteria":{"ref":"manual-validation"}}]}`,
		Actor:          "agent-1",
		CommandID:      "cmd-gset-normalize-template-1",
	}); err != nil {
		t.Fatalf("create beta template: %v", err)
	}
	if _, _, err := s.CreateGateTemplate(ctx, CreateGateTemplateParams{
		TemplateID:     "alpha",
		Version:        1,
		AppliesTo:      []string{"task"},
		DefinitionJSON: `{"gates":[{"id":"build","kind":"check","required":true,"criteria":{"ref":"manual-validation"}}]}`,
		Actor:          "agent-1",
		CommandID:      "cmd-gset-normalize-template-2",
	}); err != nil {
		t.Fatalf("create alpha template: %v", err)
	}

	first, idempotent, err := s.InstantiateGateSet(ctx, InstantiateGateSetParams{
		IssueID:      issueID,
		TemplateRefs: []string{"beta@1", " alpha@1 ", "beta@1"},
		Actor:        "agent-1",
		CommandID:    "cmd-gset-normalize-instantiate-1",
	})
	if err != nil {
		t.Fatalf("instantiate normalized gate set: %v", err)
	}
	if idempotent {
		t.Fatal("expected first normalized instantiation to be non-idempotent")
	}
	if !equalStringSlices(first.TemplateRefs, []string{"alpha@1", "beta@1"}) {
		t.Fatalf("expected normalized template refs, got %#v", first.TemplateRefs)
	}
	if len(first.Items) != 2 {
		t.Fatalf("expected two gate items from normalized template refs, got %#v", first.Items)
	}

	second, idempotent, err := s.InstantiateGateSet(ctx, InstantiateGateSetParams{
		IssueID:      issueID,
		TemplateRefs: []string{"alpha@1", "beta@1"},
		Actor:        "agent-1",
		CommandID:    "cmd-gset-normalize-instantiate-2",
	})
	if err != nil {
		t.Fatalf("instantiate normalized gate set retry: %v", err)
	}
	if !idempotent {
		t.Fatal("expected normalized template ref retry to be idempotent")
	}
	if second.GateSetID != first.GateSetID {
		t.Fatalf("expected same gate set id after normalized retry, got %q vs %q", second.GateSetID, first.GateSetID)
	}
}

func TestLockGateSetRestoresActiveGateSetWhenAlreadyLocked(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	issueID := "mem-8b9c0d1"
	if _, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   issueID,
		Type:      "task",
		Title:     "Restore active gate set",
		Actor:     "agent-1",
		CommandID: "cmd-gset-restore-create-1",
	}); err != nil {
		t.Fatalf("create issue: %v", err)
	}
	if _, _, err := s.CreateGateTemplate(ctx, CreateGateTemplateParams{
		TemplateID:     "restore-lock",
		Version:        1,
		AppliesTo:      []string{"task"},
		DefinitionJSON: `{"gates":[{"id":"build","kind":"check","required":true,"criteria":{"ref":"manual-validation"}}]}`,
		Actor:          "agent-1",
		CommandID:      "cmd-gset-restore-template-1",
	}); err != nil {
		t.Fatalf("create restore-lock template: %v", err)
	}
	gateSet, _, err := s.InstantiateGateSet(ctx, InstantiateGateSetParams{
		IssueID:      issueID,
		TemplateRefs: []string{"restore-lock@1"},
		Actor:        "agent-1",
		CommandID:    "cmd-gset-restore-instantiate-1",
	})
	if err != nil {
		t.Fatalf("instantiate gate set: %v", err)
	}
	if _, _, err := s.LockGateSet(ctx, LockGateSetParams{
		IssueID:   issueID,
		Actor:     "agent-1",
		CommandID: "cmd-gset-restore-lock-1",
	}); err != nil {
		t.Fatalf("initial lock gate set: %v", err)
	}

	if _, err := s.db.ExecContext(ctx, `UPDATE work_items SET active_gate_set_id = NULL WHERE id = ?`, issueID); err != nil {
		t.Fatalf("clear active gate set id: %v", err)
	}

	locked, lockedNow, err := s.LockGateSet(ctx, LockGateSetParams{
		IssueID:   issueID,
		Actor:     "agent-1",
		CommandID: "cmd-gset-restore-lock-2",
	})
	if err != nil {
		t.Fatalf("relock already locked gate set: %v", err)
	}
	if lockedNow {
		t.Fatal("expected relock of already-locked gate set not to lock now")
	}
	if locked.GateSetID != gateSet.GateSetID || strings.TrimSpace(locked.LockedAt) == "" {
		t.Fatalf("expected existing locked gate set back, got %#v", locked)
	}

	var activeGateSetID sql.NullString
	if err := s.db.QueryRowContext(ctx, `SELECT active_gate_set_id FROM work_items WHERE id = ?`, issueID).Scan(&activeGateSetID); err != nil {
		t.Fatalf("read restored active gate set id: %v", err)
	}
	if !activeGateSetID.Valid || activeGateSetID.String != gateSet.GateSetID {
		t.Fatalf("expected active gate set id %q to be restored, got %#v", gateSet.GateSetID, activeGateSetID)
	}
}
