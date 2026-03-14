package store

import (
	"context"
	"database/sql"
	"testing"
)

func TestPacketProjectionStoresNormalizedRoutingFields(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	issueID := "mem-a1b2c31"
	if _, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   issueID,
		Type:      "task",
		Title:     "Packet routing projection test",
		Actor:     "agent-1",
		CommandID: "cmd-pkt-route-create-1",
	}); err != nil {
		t.Fatalf("create issue: %v", err)
	}
	if _, _, _, err := s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
		IssueID:   issueID,
		Status:    "inprogress",
		Actor:     "agent-1",
		CommandID: "cmd-pkt-route-progress-1",
	}); err != nil {
		t.Fatalf("move issue to inprogress: %v", err)
	}
	if _, _, err := s.CheckpointSession(ctx, CheckpointSessionParams{
		SessionID: "sess-pktroute-1",
		Trigger:   "manual",
		Actor:     "agent-1",
		CommandID: "cmd-pkt-route-session-1",
	}); err != nil {
		t.Fatalf("checkpoint session: %v", err)
	}

	issuePacket, err := s.BuildRehydratePacket(ctx, BuildPacketParams{
		Scope:     "issue",
		ScopeID:   issueID,
		Actor:     "agent-1",
		CommandID: "cmd-pkt-route-issue-packet-1",
	})
	if err != nil {
		t.Fatalf("build issue packet: %v", err)
	}
	sessionPacket, err := s.BuildRehydratePacket(ctx, BuildPacketParams{
		Scope:     "session",
		ScopeID:   "sess-pktroute-1",
		Actor:     "agent-1",
		CommandID: "cmd-pkt-route-session-packet-1",
	})
	if err != nil {
		t.Fatalf("build session packet: %v", err)
	}

	var (
		issueScopeID    sql.NullString
		issueIssueID    sql.NullString
		issueSessionID  sql.NullString
		issueCycleNoRaw sql.NullInt64
	)
	if err := s.db.QueryRowContext(ctx, `
		SELECT scope_id, issue_id, session_id, issue_cycle_no
		FROM rehydrate_packets
		WHERE packet_id = ?
	`, issuePacket.PacketID).Scan(&issueScopeID, &issueIssueID, &issueSessionID, &issueCycleNoRaw); err != nil {
		t.Fatalf("read normalized issue packet routing fields: %v", err)
	}
	if !issueScopeID.Valid || issueScopeID.String != issueID || !issueIssueID.Valid || issueIssueID.String != issueID {
		t.Fatalf("expected normalized issue routing fields for %q, got scope=%#v issue=%#v", issueID, issueScopeID, issueIssueID)
	}
	if issueSessionID.Valid {
		t.Fatalf("did not expect session_id for issue packet, got %#v", issueSessionID)
	}
	if !issueCycleNoRaw.Valid || int(issueCycleNoRaw.Int64) != 1 {
		t.Fatalf("expected issue_cycle_no 1, got %#v", issueCycleNoRaw)
	}

	var (
		sessionScopeID    sql.NullString
		sessionIssueID    sql.NullString
		sessionSessionID  sql.NullString
		sessionCycleNoRaw sql.NullInt64
	)
	if err := s.db.QueryRowContext(ctx, `
		SELECT scope_id, issue_id, session_id, issue_cycle_no
		FROM rehydrate_packets
		WHERE packet_id = ?
	`, sessionPacket.PacketID).Scan(&sessionScopeID, &sessionIssueID, &sessionSessionID, &sessionCycleNoRaw); err != nil {
		t.Fatalf("read normalized session packet routing fields: %v", err)
	}
	if !sessionScopeID.Valid || sessionScopeID.String != "sess-pktroute-1" || !sessionSessionID.Valid || sessionSessionID.String != "sess-pktroute-1" {
		t.Fatalf("expected normalized session routing fields, got scope=%#v session=%#v", sessionScopeID, sessionSessionID)
	}
	if sessionIssueID.Valid || sessionCycleNoRaw.Valid {
		t.Fatalf("did not expect issue routing metadata for session packet, got issue=%#v cycle=%#v", sessionIssueID, sessionCycleNoRaw)
	}
}

func TestLatestPacketLookupFallsBackToLegacyJSONScopeID(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	legacyJSON := `{"scope":"issue","scope_id":"mem-legacy1","state":{"issue_id":"mem-legacy1","cycle_no":3},"provenance":{"issue_id":"mem-legacy1","issue_cycle_no":3}}`
	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO rehydrate_packets(
			packet_id, scope, scope_id, issue_id, session_id, issue_cycle_no, packet_json, packet_schema_version, built_from_event_id, created_at
		) VALUES(?, ?, NULL, NULL, NULL, NULL, ?, ?, ?, ?)
	`, "pkt_legacy_route", "issue", legacyJSON, 2, "evt_legacy_route", nowUTC()); err != nil {
		t.Fatalf("insert legacy packet row: %v", err)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.Rollback()

	packet, found, err := latestPacketForScopeIDTx(ctx, tx, "issue", "mem-legacy1")
	if err != nil {
		t.Fatalf("lookup legacy packet by normalized helper: %v", err)
	}
	if !found || packet.PacketID != "pkt_legacy_route" {
		t.Fatalf("expected legacy packet lookup to succeed, got found=%v packet=%#v", found, packet)
	}
}

func TestBuildCompactionPolicyUsesDeterministicThresholds(t *testing.T) {
	t.Parallel()

	triggered := buildCompactionPolicy("issue", compactionEventThreshold, compactionOpenLoopThreshold, compactionContextChunkThreshold)
	if triggered["triggered"] != true {
		t.Fatalf("expected compaction policy to trigger at thresholds, got %#v", triggered)
	}
	reasons, ok := triggered["reasons"].([]any)
	if !ok || len(reasons) != 3 {
		t.Fatalf("expected compaction reasons for all threshold breaches, got %#v", triggered["reasons"])
	}
	observed, ok := triggered["observed"].(map[string]any)
	if !ok || anyToInt(observed["event_count"]) != compactionEventThreshold {
		t.Fatalf("expected observed counts in compaction policy, got %#v", triggered["observed"])
	}

	notTriggered := buildCompactionPolicy("session", compactionEventThreshold-1, 0, compactionContextChunkThreshold-1)
	if notTriggered["triggered"] != false {
		t.Fatalf("expected compaction policy to stay inactive below thresholds, got %#v", notTriggered)
	}
}
