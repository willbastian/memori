package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
)

func TestReplayRebuildsEventSourcedPacketsAndIssueSummaries(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	issueID := "mem-9191919"
	if _, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   issueID,
		Type:      "task",
		Title:     "Replay packet test issue",
		Actor:     "agent-1",
		CommandID: "cmd-replay-packet-create-1",
	}); err != nil {
		t.Fatalf("create issue: %v", err)
	}
	if _, _, _, err := s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
		IssueID:   issueID,
		Status:    "inprogress",
		Actor:     "agent-1",
		CommandID: "cmd-replay-packet-progress-1",
	}); err != nil {
		t.Fatalf("move issue to inprogress: %v", err)
	}
	createLockedGateSetEventSourcedForTest(t, s, issueID, "replay-packet-gate", "build", "cmd-replay-packet-gset")
	_, gateEvent, _, err := s.EvaluateGate(ctx, EvaluateGateParams{
		IssueID:      issueID,
		GateID:       "build",
		Result:       "FAIL",
		EvidenceRefs: []string{"ci://run/replay-packet-1"},
		Actor:        "agent-1",
		CommandID:    "cmd-replay-packet-gate-1",
	})
	if err != nil {
		t.Fatalf("evaluate gate for replay packet test: %v", err)
	}

	packet, err := s.BuildRehydratePacket(ctx, BuildPacketParams{
		Scope:     "issue",
		ScopeID:   issueID,
		Actor:     "agent-1",
		CommandID: "cmd-replay-packet-build-1",
	})
	if err != nil {
		t.Fatalf("build issue packet: %v", err)
	}
	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO rehydrate_packets(packet_id, scope, packet_json, packet_schema_version, built_from_event_id, created_at)
		VALUES(?, ?, ?, ?, ?, ?)
	`, "pkt_stale_replay", "issue", `{"scope":"issue","scope_id":"mem-stale"}`, 1, "evt_stale", nowUTC()); err != nil {
		t.Fatalf("insert stale packet row: %v", err)
	}
	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO agent_focus(agent_id, active_issue_id, active_cycle_no, last_packet_id, updated_at)
		VALUES(?, ?, ?, ?, ?)
	`, "agent-stale-replay", issueID, 1, "pkt_stale_replay", nowUTC()); err != nil {
		t.Fatalf("insert stale agent focus: %v", err)
	}

	if _, err := s.db.ExecContext(ctx, `DELETE FROM issue_summaries WHERE summary_level = 'packet'`); err != nil {
		t.Fatalf("delete packet issue summaries: %v", err)
	}
	if _, err := s.db.ExecContext(ctx, `DELETE FROM rehydrate_packets WHERE packet_id = ?`, packet.PacketID); err != nil {
		t.Fatalf("delete packet row: %v", err)
	}
	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO issue_summaries(
			summary_id, issue_id, cycle_no, summary_level, summary_json,
			from_entity_seq, to_entity_seq, parent_summary_id, created_at
		) VALUES(?, ?, ?, ?, ?, ?, ?, NULL, ?)
	`, "sum_stale_replay", issueID, 1, "packet", `{"stale":true}`, 1, 1, nowUTC()); err != nil {
		t.Fatalf("insert stale issue summary: %v", err)
	}
	if _, err := s.db.ExecContext(ctx, `DELETE FROM open_loops WHERE issue_id = ?`, issueID); err != nil {
		t.Fatalf("delete open loops: %v", err)
	}

	if _, err := s.ReplayProjections(ctx); err != nil {
		t.Fatalf("replay projections: %v", err)
	}

	var packetCount int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(1) FROM rehydrate_packets WHERE packet_id = ?`, packet.PacketID).Scan(&packetCount); err != nil {
		t.Fatalf("count replayed packet row: %v", err)
	}
	if packetCount != 1 {
		t.Fatalf("expected replay to rebuild packet row, got %d", packetCount)
	}
	var (
		scopeID    sql.NullString
		issueIDRaw sql.NullString
		sessionID  sql.NullString
		cycleNoRaw sql.NullInt64
	)
	if err := s.db.QueryRowContext(ctx, `
		SELECT scope_id, issue_id, session_id, issue_cycle_no
		FROM rehydrate_packets
		WHERE packet_id = ?
	`, packet.PacketID).Scan(&scopeID, &issueIDRaw, &sessionID, &cycleNoRaw); err != nil {
		t.Fatalf("read replayed normalized packet routing fields: %v", err)
	}
	if !scopeID.Valid || scopeID.String != issueID || !issueIDRaw.Valid || issueIDRaw.String != issueID {
		t.Fatalf("expected replay to rebuild normalized issue routing, got scope=%#v issue=%#v", scopeID, issueIDRaw)
	}
	if sessionID.Valid || !cycleNoRaw.Valid || int(cycleNoRaw.Int64) != 1 {
		t.Fatalf("expected replay to rebuild issue cycle without session routing, got session=%#v cycle=%#v", sessionID, cycleNoRaw)
	}

	var summaryCount int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(1) FROM issue_summaries WHERE summary_level = 'packet' AND issue_id = ?`, issueID).Scan(&summaryCount); err != nil {
		t.Fatalf("count replayed packet issue summaries: %v", err)
	}
	if summaryCount == 0 {
		t.Fatalf("expected replay to rebuild packet-derived issue summaries")
	}
	var replayedSummaryJSON string
	if err := s.db.QueryRowContext(ctx, `
		SELECT summary_json
		FROM issue_summaries
		WHERE summary_level = 'packet' AND issue_id = ?
		ORDER BY created_at DESC, summary_id DESC
		LIMIT 1
	`, issueID).Scan(&replayedSummaryJSON); err != nil {
		t.Fatalf("read replayed packet summary json: %v", err)
	}
	var replayedSummary map[string]any
	if err := json.Unmarshal([]byte(replayedSummaryJSON), &replayedSummary); err != nil {
		t.Fatalf("decode replayed packet summary json: %v", err)
	}
	if _, ok := replayedSummary["decision_summary"].(map[string]any); !ok {
		t.Fatalf("expected replayed summary to include decision summary, got %#v", replayedSummary["decision_summary"])
	}

	var loopCount int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(1) FROM open_loops WHERE issue_id = ?`, issueID).Scan(&loopCount); err != nil {
		t.Fatalf("count replayed open loops: %v", err)
	}
	if loopCount == 0 {
		t.Fatalf("expected replay to rebuild open loops")
	}
	var stalePacketCount int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(1) FROM rehydrate_packets WHERE packet_id = ?`, "pkt_stale_replay").Scan(&stalePacketCount); err != nil {
		t.Fatalf("count stale packet rows after replay: %v", err)
	}
	if stalePacketCount != 0 {
		t.Fatalf("expected replay to clear stale packet rows, got %d", stalePacketCount)
	}
	var staleSummaryCount int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(1) FROM issue_summaries WHERE summary_id = ?`, "sum_stale_replay").Scan(&staleSummaryCount); err != nil {
		t.Fatalf("count stale summary rows after replay: %v", err)
	}
	if staleSummaryCount != 0 {
		t.Fatalf("expected replay to clear stale issue summaries, got %d", staleSummaryCount)
	}
	var staleFocusCount int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(1) FROM agent_focus WHERE agent_id = ?`, "agent-stale-replay").Scan(&staleFocusCount); err != nil {
		t.Fatalf("count stale agent focus rows after replay: %v", err)
	}
	if staleFocusCount != 0 {
		t.Fatalf("expected replay to clear stale agent focus rows, got %d", staleFocusCount)
	}
	var loopSourceEventID string
	if err := s.db.QueryRowContext(ctx, `
		SELECT source_event_id
		FROM open_loops
		WHERE issue_id = ? AND loop_type = 'gate' AND status = 'Open'
		LIMIT 1
	`, issueID).Scan(&loopSourceEventID); err != nil {
		t.Fatalf("read replayed open loop source_event_id: %v", err)
	}
	if loopSourceEventID != gateEvent.EventID {
		t.Fatalf("expected replayed loop source_event_id %q, got %q", gateEvent.EventID, loopSourceEventID)
	}
}
