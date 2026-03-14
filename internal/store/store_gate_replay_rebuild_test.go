package store

import (
	"context"
	"reflect"
	"strings"
	"testing"
)

func TestReplayProjectionsRebuildsGateTemplatesAndGateSets(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	if _, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   "mem-a7b8c9d",
		Type:      "task",
		Title:     "Replay gate projections",
		Actor:     "agent-1",
		CommandID: "cmd-replay-gate-projections-issue-1",
	}); err != nil {
		t.Fatalf("create issue: %v", err)
	}

	template, _, err := s.CreateGateTemplate(ctx, CreateGateTemplateParams{
		TemplateID:     "replay-gates",
		Version:        1,
		AppliesTo:      []string{"task"},
		DefinitionJSON: `{"gates":[{"id":"build","kind":"check","required":true,"criteria":{"command":"go test ./..."}}]}`,
		Actor:          "human:alice",
		CommandID:      "cmd-replay-gate-projections-template-1",
	})
	if err != nil {
		t.Fatalf("create gate template: %v", err)
	}

	gateSet, _, err := s.InstantiateGateSet(ctx, InstantiateGateSetParams{
		IssueID:      "mem-a7b8c9d",
		TemplateRefs: []string{"replay-gates@1"},
		Actor:        "agent-1",
		CommandID:    "cmd-replay-gate-projections-set-1",
	})
	if err != nil {
		t.Fatalf("instantiate gate set: %v", err)
	}
	if _, _, err := s.LockGateSet(ctx, LockGateSetParams{
		IssueID:   "mem-a7b8c9d",
		Actor:     "agent-1",
		CommandID: "cmd-replay-gate-projections-lock-1",
	}); err != nil {
		t.Fatalf("lock gate set: %v", err)
	}

	replay, err := s.ReplayProjections(ctx)
	if err != nil {
		t.Fatalf("replay projections: %v", err)
	}
	if replay.EventsApplied < 4 {
		t.Fatalf("expected replay to apply gate-related events, got %d", replay.EventsApplied)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.Rollback()

	replayedTemplate, found, err := gateTemplateByIDVersionTx(ctx, tx, template.TemplateID, template.Version)
	if err != nil {
		t.Fatalf("lookup replayed template: %v", err)
	}
	if !found {
		t.Fatalf("expected replayed template %s@%d", template.TemplateID, template.Version)
	}
	if replayedTemplate.ApprovedBy != "human:alice" || !replayedTemplate.Executable {
		t.Fatalf("expected replayed approved executable template, got %#v", replayedTemplate)
	}

	replayedGateSet, found, err := gateSetForIssueCycleTx(ctx, tx, "mem-a7b8c9d", 1)
	if err != nil {
		t.Fatalf("lookup replayed gate set: %v", err)
	}
	if !found {
		t.Fatal("expected replayed gate set for issue cycle 1")
	}
	if replayedGateSet.GateSetID != gateSet.GateSetID || strings.TrimSpace(replayedGateSet.LockedAt) == "" || len(replayedGateSet.Items) != 1 {
		t.Fatalf("unexpected replayed gate set: %#v", replayedGateSet)
	}

	var activeGateSetID string
	if err := tx.QueryRowContext(ctx, `SELECT active_gate_set_id FROM work_items WHERE id = ?`, "mem-a7b8c9d").Scan(&activeGateSetID); err != nil {
		t.Fatalf("read active_gate_set_id after replay: %v", err)
	}
	if activeGateSetID != gateSet.GateSetID {
		t.Fatalf("expected active_gate_set_id %q after replay, got %q", gateSet.GateSetID, activeGateSetID)
	}
}

func TestReplayProjectionsClearsStaleGateProjectionRows(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	if _, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   "mem-b7c8d9e",
		Type:      "task",
		Title:     "Replay stale gate projections",
		Actor:     "agent-1",
		CommandID: "cmd-replay-gate-stale-issue-1",
	}); err != nil {
		t.Fatalf("create issue: %v", err)
	}

	if _, _, err := s.CreateGateTemplate(ctx, CreateGateTemplateParams{
		TemplateID:     "stale-replay-gates",
		Version:        1,
		AppliesTo:      []string{"task"},
		DefinitionJSON: `{"gates":[{"id":"build","kind":"check","required":true,"criteria":{"command":"go test ./..."}}]}`,
		Actor:          "human:alice",
		CommandID:      "cmd-replay-gate-stale-template-1",
	}); err != nil {
		t.Fatalf("create gate template: %v", err)
	}

	gateSet, _, err := s.InstantiateGateSet(ctx, InstantiateGateSetParams{
		IssueID:      "mem-b7c8d9e",
		TemplateRefs: []string{"stale-replay-gates@1"},
		Actor:        "agent-1",
		CommandID:    "cmd-replay-gate-stale-set-1",
	})
	if err != nil {
		t.Fatalf("instantiate gate set: %v", err)
	}
	if _, _, err := s.LockGateSet(ctx, LockGateSetParams{
		IssueID:   "mem-b7c8d9e",
		Actor:     "agent-1",
		CommandID: "cmd-replay-gate-stale-lock-1",
	}); err != nil {
		t.Fatalf("lock gate set: %v", err)
	}
	gateEval, _, _, err := s.EvaluateGate(ctx, EvaluateGateParams{
		IssueID:      "mem-b7c8d9e",
		GateID:       "build",
		Result:       "FAIL",
		EvidenceRefs: []string{"ci://run/stale-replay-1"},
		Actor:        "agent-1",
		CommandID:    "cmd-replay-gate-stale-eval-1",
	})
	if err != nil {
		t.Fatalf("evaluate gate: %v", err)
	}

	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO gate_templates(
			template_id, version, applies_to_json, definition_json, definition_hash, created_at, created_by
		) VALUES(?, ?, ?, ?, ?, ?, ?)
	`, "stale-template", 9, `["Task"]`, `{"gates":[{"id":"docs"}]}`, "stale-template-hash", nowUTC(), "agent-1"); err != nil {
		t.Fatalf("insert stale gate template: %v", err)
	}
	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO gate_template_approvals(template_id, version, approved_at, approved_by)
		VALUES(?, ?, ?, ?)
	`, "stale-template", 9, nowUTC(), "human:alice"); err != nil {
		t.Fatalf("insert stale gate template approval: %v", err)
	}
	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO gate_sets(
			gate_set_id, issue_id, cycle_no, template_refs_json, frozen_definition_json,
			gate_set_hash, locked_at, created_at, created_by
		) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, "gset_stale_projection", "mem-b7c8d9e", 99, `["stale-template@9"]`, `{"gates":[{"id":"docs"}]}`, "gset_stale_projection_hash", nowUTC(), nowUTC(), "agent-1"); err != nil {
		t.Fatalf("insert stale gate set: %v", err)
	}
	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO gate_set_items(gate_set_id, gate_id, kind, required, criteria_json)
		VALUES(?, ?, ?, ?, ?)
	`, "gset_stale_projection", "docs", "check", 1, `{"ref":"manual-validation"}`); err != nil {
		t.Fatalf("insert stale gate set item: %v", err)
	}
	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO gate_status_projection(
			issue_id, gate_set_id, gate_id, result, evidence_refs_json, evaluated_at, updated_at, last_event_id
		) VALUES(?, ?, ?, ?, ?, ?, ?, ?)
	`, "mem-b7c8d9e", "gset_stale_projection", "docs", "PASS", `["docs://stale"]`, nowUTC(), nowUTC(), "evt_stale_gate_status"); err != nil {
		t.Fatalf("insert stale gate status projection: %v", err)
	}

	replay, err := s.ReplayProjections(ctx)
	if err != nil {
		t.Fatalf("replay projections: %v", err)
	}
	if replay.EventsApplied < 5 {
		t.Fatalf("expected replay to apply gate workflow events, got %d", replay.EventsApplied)
	}

	var count int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(1) FROM gate_templates WHERE template_id = ?`, "stale-template").Scan(&count); err != nil {
		t.Fatalf("count stale template rows after replay: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected stale gate template rows to be cleared, got %d", count)
	}
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(1) FROM gate_sets WHERE gate_set_id = ?`, "gset_stale_projection").Scan(&count); err != nil {
		t.Fatalf("count stale gate set rows after replay: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected stale gate set rows to be cleared, got %d", count)
	}
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(1) FROM gate_status_projection WHERE gate_set_id = ?`, "gset_stale_projection").Scan(&count); err != nil {
		t.Fatalf("count stale gate status rows after replay: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected stale gate status rows to be cleared, got %d", count)
	}

	status, err := s.GetGateStatus(ctx, "mem-b7c8d9e")
	if err != nil {
		t.Fatalf("get gate status after replay: %v", err)
	}
	if status.GateSetID != gateSet.GateSetID || len(status.Gates) != 1 {
		t.Fatalf("unexpected gate status after replay: %#v", status)
	}
	if status.Gates[0].GateID != "build" || status.Gates[0].Result != gateEval.Result {
		t.Fatalf("expected replayed gate evaluation to survive stale cleanup, got %#v", status.Gates[0])
	}
	if !reflect.DeepEqual(status.Gates[0].EvidenceRefs, gateEval.EvidenceRefs) {
		t.Fatalf("expected replayed evidence refs %#v, got %#v", gateEval.EvidenceRefs, status.Gates[0].EvidenceRefs)
	}
}

func TestReplayProjectionsSurfacesProjectionCleanupAndEventQueryFailures(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name      string
		tableName string
		want      string
	}{
		{name: "missing gate status projection table", tableName: "gate_status_projection", want: "clear gate_status_projection"},
		{name: "missing gate template approvals table", tableName: "gate_template_approvals", want: "clear gate_template_approvals"},
		{name: "missing agent focus table", tableName: "agent_focus", want: "clear agent_focus"},
		{name: "missing issue summaries table", tableName: "issue_summaries", want: "clear issue_summaries"},
		{name: "missing open loops table", tableName: "open_loops", want: "clear open_loops"},
		{name: "missing context chunks table", tableName: "context_chunks", want: "clear context_chunks"},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := newTestStore(t)
			ctx := context.Background()
			if _, err := s.db.ExecContext(ctx, `DROP TABLE `+tc.tableName); err != nil {
				t.Fatalf("drop %s: %v", tc.tableName, err)
			}

			if _, err := s.ReplayProjections(ctx); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("expected replay cleanup error %q, got %v", tc.want, err)
			}
		})
	}

	t.Run("missing events table", func(t *testing.T) {
		t.Parallel()

		s := newTestStore(t)
		ctx := context.Background()
		if _, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
			IssueID:   "mem-c8d9e0f",
			Type:      "task",
			Title:     "Replay missing events table",
			Actor:     "agent-1",
			CommandID: "cmd-replay-missing-events-1",
		}); err != nil {
			t.Fatalf("create issue before dropping events table: %v", err)
		}
		if _, err := s.db.ExecContext(ctx, `DROP TABLE events`); err != nil {
			t.Fatalf("drop events table: %v", err)
		}

		if _, err := s.ReplayProjections(ctx); err == nil || !strings.Contains(err.Error(), "query events for replay") {
			t.Fatalf("expected replay query events error, got %v", err)
		}
	})
}
