package store

import (
	"context"
	"database/sql"
	"testing"
)

func TestUpdateIssueStatusReopenAdvancesCycleAndClearsActiveGateSet(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	issueID := "mem-6767676"
	if _, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   issueID,
		Type:      "task",
		Title:     "Reopen cycle test",
		Actor:     "agent-1",
		CommandID: "cmd-reopen-create-1",
	}); err != nil {
		t.Fatalf("create issue: %v", err)
	}
	if _, _, _, err := s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
		IssueID:   issueID,
		Status:    "inprogress",
		Actor:     "agent-1",
		CommandID: "cmd-reopen-progress-1",
	}); err != nil {
		t.Fatalf("move issue to inprogress: %v", err)
	}
	if _, _, err := s.CreateGateTemplate(ctx, CreateGateTemplateParams{
		TemplateID:     "reopen",
		Version:        1,
		AppliesTo:      []string{"task"},
		DefinitionJSON: `{"gates":[{"id":"build","kind":"check","required":true,"criteria":{"command":"go test ./..."}}]}`,
		Actor:          "agent-1",
		CommandID:      "cmd-reopen-template-create-1",
	}); err != nil {
		t.Fatalf("create gate template: %v", err)
	}

	gateSet, _, err := s.InstantiateGateSet(ctx, InstantiateGateSetParams{
		IssueID:      issueID,
		TemplateRefs: []string{"reopen@1"},
		Actor:        "agent-1",
		CommandID:    "cmd-reopen-gset-create-1",
	})
	if err != nil {
		t.Fatalf("instantiate gate set: %v", err)
	}
	if _, _, err := s.LockGateSet(ctx, LockGateSetParams{
		IssueID:   issueID,
		Actor:     "agent-1",
		CommandID: "cmd-reopen-gset-lock-1",
	}); err != nil {
		t.Fatalf("lock gate set: %v", err)
	}
	appendGateEvaluationEventForTest(t, s, issueID, gateSet.GateSetID, "build", "PASS", "agent-1", "cmd-reopen-gate-pass-1")

	if _, _, _, err := s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
		IssueID:   issueID,
		Status:    "done",
		Actor:     "agent-1",
		CommandID: "cmd-reopen-done-1",
	}); err != nil {
		t.Fatalf("close issue before reopen: %v", err)
	}

	reopened, _, _, err := s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
		IssueID:   issueID,
		Status:    "inprogress",
		Actor:     "agent-1",
		CommandID: "cmd-reopen-inprogress-2",
	})
	if err != nil {
		t.Fatalf("reopen issue: %v", err)
	}
	if reopened.Status != "InProgress" {
		t.Fatalf("expected reopened status InProgress, got %s", reopened.Status)
	}

	var (
		cycleNo         int
		activeGateSetID sql.NullString
	)
	if err := s.db.QueryRowContext(ctx, `
		SELECT current_cycle_no, active_gate_set_id
		FROM work_items
		WHERE id = ?
	`, issueID).Scan(&cycleNo, &activeGateSetID); err != nil {
		t.Fatalf("read reopened work item state: %v", err)
	}
	if cycleNo != 2 {
		t.Fatalf("expected reopened issue to advance to cycle 2, got %d", cycleNo)
	}
	if activeGateSetID.Valid {
		t.Fatalf("expected reopened issue to clear active_gate_set_id, got %#v", activeGateSetID)
	}
}

func TestReopenSupportsNewCycleGateSetAndReplay(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	issueID := "mem-6868686"
	if _, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   issueID,
		Type:      "task",
		Title:     "Reopen replay test",
		Actor:     "agent-1",
		CommandID: "cmd-reopen-replay-create-1",
	}); err != nil {
		t.Fatalf("create issue: %v", err)
	}
	if _, _, _, err := s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
		IssueID:   issueID,
		Status:    "inprogress",
		Actor:     "agent-1",
		CommandID: "cmd-reopen-replay-progress-1",
	}); err != nil {
		t.Fatalf("move issue to inprogress: %v", err)
	}
	if _, _, err := s.CreateGateTemplate(ctx, CreateGateTemplateParams{
		TemplateID:     "reopen-replay",
		Version:        1,
		AppliesTo:      []string{"task"},
		DefinitionJSON: `{"gates":[{"id":"build","kind":"check","required":true,"criteria":{"command":"go test ./..."}}]}`,
		Actor:          "agent-1",
		CommandID:      "cmd-reopen-replay-template-create-1",
	}); err != nil {
		t.Fatalf("create gate template: %v", err)
	}

	cycle1, _, err := s.InstantiateGateSet(ctx, InstantiateGateSetParams{
		IssueID:      issueID,
		TemplateRefs: []string{"reopen-replay@1"},
		Actor:        "agent-1",
		CommandID:    "cmd-reopen-replay-gset-create-1",
	})
	if err != nil {
		t.Fatalf("instantiate cycle 1 gate set: %v", err)
	}
	if _, _, err := s.LockGateSet(ctx, LockGateSetParams{
		IssueID:   issueID,
		Actor:     "agent-1",
		CommandID: "cmd-reopen-replay-gset-lock-1",
	}); err != nil {
		t.Fatalf("lock cycle 1 gate set: %v", err)
	}
	appendGateEvaluationEventForTest(t, s, issueID, cycle1.GateSetID, "build", "PASS", "agent-1", "cmd-reopen-replay-gate-pass-1")
	if _, _, _, err := s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
		IssueID:   issueID,
		Status:    "done",
		Actor:     "agent-1",
		CommandID: "cmd-reopen-replay-done-1",
	}); err != nil {
		t.Fatalf("close issue before reopen: %v", err)
	}
	if _, _, _, err := s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
		IssueID:   issueID,
		Status:    "inprogress",
		Actor:     "agent-1",
		CommandID: "cmd-reopen-replay-inprogress-2",
	}); err != nil {
		t.Fatalf("reopen issue: %v", err)
	}

	cycle2, _, err := s.InstantiateGateSet(ctx, InstantiateGateSetParams{
		IssueID:      issueID,
		TemplateRefs: []string{"reopen-replay@1"},
		Actor:        "agent-1",
		CommandID:    "cmd-reopen-replay-gset-create-2",
	})
	if err != nil {
		t.Fatalf("instantiate cycle 2 gate set: %v", err)
	}
	if cycle2.CycleNo != 2 {
		t.Fatalf("expected cycle 2 gate set, got cycle %d", cycle2.CycleNo)
	}
	if cycle2.GateSetID == cycle1.GateSetID {
		t.Fatalf("expected new gate set id for reopened cycle, got %q", cycle2.GateSetID)
	}
	if _, _, err := s.LockGateSet(ctx, LockGateSetParams{
		IssueID:   issueID,
		Actor:     "agent-1",
		CommandID: "cmd-reopen-replay-gset-lock-2",
	}); err != nil {
		t.Fatalf("lock cycle 2 gate set: %v", err)
	}
	evaluation, _, _, err := s.EvaluateGate(ctx, EvaluateGateParams{
		IssueID:      issueID,
		GateID:       "build",
		Result:       "FAIL",
		EvidenceRefs: []string{"ci://run/reopen-replay-2"},
		Actor:        "agent-1",
		CommandID:    "cmd-reopen-replay-gate-fail-2",
	})
	if err != nil {
		t.Fatalf("evaluate cycle 2 gate: %v", err)
	}
	if evaluation.GateSetID != cycle2.GateSetID {
		t.Fatalf("expected cycle 2 evaluation to bind gate set %q, got %q", cycle2.GateSetID, evaluation.GateSetID)
	}

	if _, err := s.ReplayProjections(ctx); err != nil {
		t.Fatalf("replay projections: %v", err)
	}

	var (
		cycleNo         int
		activeGateSetID sql.NullString
	)
	if err := s.db.QueryRowContext(ctx, `
		SELECT current_cycle_no, active_gate_set_id
		FROM work_items
		WHERE id = ?
	`, issueID).Scan(&cycleNo, &activeGateSetID); err != nil {
		t.Fatalf("read replayed work item state: %v", err)
	}
	if cycleNo != 2 {
		t.Fatalf("expected replayed current_cycle_no 2, got %d", cycleNo)
	}
	if !activeGateSetID.Valid || activeGateSetID.String != cycle2.GateSetID {
		t.Fatalf("expected replay to restore active_gate_set_id %q, got %#v", cycle2.GateSetID, activeGateSetID)
	}

	status, err := s.GetGateStatus(ctx, issueID)
	if err != nil {
		t.Fatalf("get gate status after replay: %v", err)
	}
	if status.CycleNo != 2 {
		t.Fatalf("expected replayed gate status cycle 2, got %d", status.CycleNo)
	}
	if len(status.Gates) != 1 || status.Gates[0].GateID != "build" || status.Gates[0].Result != "FAIL" {
		t.Fatalf("unexpected replayed gate status: %#v", status.Gates)
	}

	templateEvents, err := s.ListEventsForEntity(ctx, "gate_template", "reopen-replay@1")
	if err != nil {
		t.Fatalf("list gate template events: %v", err)
	}
	if len(templateEvents) != 1 || templateEvents[0].EventType != "gate_template.created" {
		t.Fatalf("unexpected gate template events: %#v", templateEvents)
	}

	gateSetEvents, err := s.ListEventsForEntity(ctx, "gate_set", cycle2.GateSetID)
	if err != nil {
		t.Fatalf("list gate set events: %v", err)
	}
	if len(gateSetEvents) != 2 || gateSetEvents[0].EventType != "gate_set.instantiated" || gateSetEvents[1].EventType != "gate_set.locked" {
		t.Fatalf("unexpected gate set events: %#v", gateSetEvents)
	}
}
