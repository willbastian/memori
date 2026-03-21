package store

import (
	"context"
	"testing"
)

func TestGetIssueRepairsMissingProjectionFromLatestIssueEvent(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	issueID := "mem-5151515"
	if _, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   issueID,
		Type:      "task",
		Title:     "Repair missing issue projection",
		Actor:     "agent-1",
		CommandID: "cmd-issue-repair-create-1",
	}); err != nil {
		t.Fatalf("create issue: %v", err)
	}

	todo := "Todo"
	inProgress := "InProgress"
	appendStoreEventForTest(t, s, entityTypeIssue, issueID, eventTypeIssueUpdate, issueUpdatedPayload{
		IssueID:    issueID,
		StatusFrom: &todo,
		StatusTo:   &inProgress,
		UpdatedAt:  nowUTC(),
	}, "agent-1", "cmd-issue-repair-update-1", defaultCorrelationID(entityTypeIssue, issueID))

	issue, err := s.GetIssue(ctx, issueID)
	if err != nil {
		t.Fatalf("get issue should repair projection: %v", err)
	}
	if issue.Status != "InProgress" {
		t.Fatalf("expected repaired issue status InProgress, got %s", issue.Status)
	}
}

func TestUpdateIssueStatusRetryRepairsMissingProjectionFromExistingEvent(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	issueID := "mem-5252525"
	if _, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   issueID,
		Type:      "task",
		Title:     "Retry repair issue projection",
		Actor:     "agent-1",
		CommandID: "cmd-issue-retry-create-1",
	}); err != nil {
		t.Fatalf("create issue: %v", err)
	}

	todo := "Todo"
	inProgress := "InProgress"
	appendStoreEventForTest(t, s, entityTypeIssue, issueID, eventTypeIssueUpdate, issueUpdatedPayload{
		IssueID:    issueID,
		StatusFrom: &todo,
		StatusTo:   &inProgress,
		UpdatedAt:  nowUTC(),
	}, "agent-1", "cmd-issue-retry-update-1", defaultCorrelationID(entityTypeIssue, issueID))

	updated, event, idempotent, err := s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
		IssueID:   issueID,
		Status:    "inprogress",
		Actor:     "agent-1",
		CommandID: "cmd-issue-retry-update-1",
	})
	if err != nil {
		t.Fatalf("retry issue update should repair projection: %v", err)
	}
	if !idempotent {
		t.Fatal("expected retry issue update to be idempotent")
	}
	if event.EventType != eventTypeIssueUpdate || updated.Status != "InProgress" {
		t.Fatalf("expected repaired issue update result, got event=%#v issue=%#v", event, updated)
	}
}

func TestUpdateIssueStatusRetryDoesNotOverwriteNewerProjection(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	issueID := "mem-5353535"
	if _, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   issueID,
		Type:      "task",
		Title:     "Retry should not overwrite newer issue state",
		Actor:     "agent-1",
		CommandID: "cmd-issue-order-create-1",
	}); err != nil {
		t.Fatalf("create issue: %v", err)
	}
	if _, _, _, err := s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
		IssueID:   issueID,
		Status:    "inprogress",
		Actor:     "agent-1",
		CommandID: "cmd-issue-order-update-1",
	}); err != nil {
		t.Fatalf("move issue to inprogress: %v", err)
	}
	if _, _, _, err := s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
		IssueID:   issueID,
		Status:    "blocked",
		Actor:     "agent-1",
		CommandID: "cmd-issue-order-update-2",
	}); err != nil {
		t.Fatalf("move issue to blocked: %v", err)
	}

	retried, _, idempotent, err := s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
		IssueID:   issueID,
		Status:    "inprogress",
		Actor:     "agent-1",
		CommandID: "cmd-issue-order-update-1",
	})
	if err != nil {
		t.Fatalf("retry older issue update: %v", err)
	}
	if !idempotent {
		t.Fatal("expected older retry to be idempotent")
	}
	if retried.Status != "Blocked" {
		t.Fatalf("expected newer blocked projection to win, got %s", retried.Status)
	}
}

func TestEvaluateGateRepairsMissingLockedProjectionForCurrentCycle(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	issueID := "mem-5454545"
	if _, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   issueID,
		Type:      "task",
		Title:     "Repair locked gate projection before evaluate",
		Actor:     "agent-1",
		CommandID: "cmd-eval-repair-issue-1",
	}); err != nil {
		t.Fatalf("create issue: %v", err)
	}
	if _, _, _, err := s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
		IssueID:   issueID,
		Status:    "inprogress",
		Actor:     "agent-1",
		CommandID: "cmd-eval-repair-issue-2",
	}); err != nil {
		t.Fatalf("move issue to inprogress: %v", err)
	}

	template, _, err := s.CreateGateTemplate(ctx, CreateGateTemplateParams{
		TemplateID:     "eval-repair-template",
		Version:        1,
		AppliesTo:      []string{"task"},
		DefinitionJSON: `{"gates":[{"id":"docs","kind":"check","required":true,"criteria":{"ref":"manual-validation"}}]}`,
		Actor:          "agent-1",
		CommandID:      "cmd-eval-repair-template-1",
	})
	if err != nil {
		t.Fatalf("create template: %v", err)
	}

	gateSet, _, err := s.InstantiateGateSet(ctx, InstantiateGateSetParams{
		IssueID:      issueID,
		TemplateRefs: []string{template.TemplateID + "@1"},
		Actor:        "agent-1",
		CommandID:    "cmd-eval-repair-gset-1",
	})
	if err != nil {
		t.Fatalf("instantiate gate set: %v", err)
	}
	if _, _, err := s.LockGateSet(ctx, LockGateSetParams{
		IssueID:   issueID,
		Actor:     "agent-1",
		CommandID: "cmd-eval-repair-lock-1",
	}); err != nil {
		t.Fatalf("lock gate set: %v", err)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin projection cleanup tx: %v", err)
	}
	if err := dropReplayProjectionDeleteTriggersTx(ctx, tx); err != nil {
		t.Fatalf("drop replay delete triggers: %v", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM gate_set_items WHERE gate_set_id = ?`, gateSet.GateSetID); err != nil {
		t.Fatalf("delete gate set items projection: %v", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM gate_sets WHERE gate_set_id = ?`, gateSet.GateSetID); err != nil {
		t.Fatalf("delete gate set projection: %v", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE work_items SET active_gate_set_id = NULL WHERE id = ?`, issueID); err != nil {
		t.Fatalf("clear active gate set reference: %v", err)
	}
	if err := restoreReplayProjectionDeleteTriggersTx(ctx, tx); err != nil {
		t.Fatalf("restore replay delete triggers: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit projection cleanup tx: %v", err)
	}

	evaluation, _, _, err := s.EvaluateGate(ctx, EvaluateGateParams{
		IssueID:      issueID,
		GateID:       "docs",
		Result:       "PASS",
		EvidenceRefs: []string{"memo://manual-check"},
		Actor:        "agent-1",
		CommandID:    "cmd-eval-repair-evaluate-1",
	})
	if err != nil {
		t.Fatalf("evaluate gate should repair locked projection: %v", err)
	}
	if evaluation.GateSetID != gateSet.GateSetID || evaluation.Result != "PASS" {
		t.Fatalf("expected repaired gate evaluation, got %#v", evaluation)
	}
}
