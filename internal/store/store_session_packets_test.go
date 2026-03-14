package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"testing"
)

func TestSessionCheckpointPacketAndRehydrateFlow(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	issueID := "mem-9898989"
	if _, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   issueID,
		Type:      "task",
		Title:     "Context packet test issue",
		Actor:     "agent-1",
		CommandID: "cmd-context-create-1",
	}); err != nil {
		t.Fatalf("create issue: %v", err)
	}
	if _, _, _, err := s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
		IssueID:   issueID,
		Status:    "inprogress",
		Actor:     "agent-1",
		CommandID: "cmd-context-progress-1",
	}); err != nil {
		t.Fatalf("move issue to inprogress: %v", err)
	}
	createLockedGateSetEventSourcedForTest(t, s, issueID, "context-gate", "build", "cmd-context-gset")
	if _, _, _, err := s.EvaluateGate(ctx, EvaluateGateParams{
		IssueID:      issueID,
		GateID:       "build",
		Result:       "FAIL",
		EvidenceRefs: []string{"ci://run/context-1"},
		Actor:        "agent-1",
		CommandID:    "cmd-context-gate-fail-1",
	}); err != nil {
		t.Fatalf("evaluate gate fail for context packet: %v", err)
	}

	session, created, err := s.CheckpointSession(ctx, CheckpointSessionParams{
		SessionID: "sess-1",
		Trigger:   "manual",
		Actor:     "agent-1",
		CommandID: "cmd-context-checkpoint-1",
	})
	if err != nil {
		t.Fatalf("checkpoint session: %v", err)
	}
	if !created {
		t.Fatalf("expected first checkpoint to create session")
	}
	if session.SessionID != "sess-1" {
		t.Fatalf("expected session id sess-1, got %q", session.SessionID)
	}
	var chunkCount int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(1) FROM context_chunks WHERE session_id = ?`, "sess-1").Scan(&chunkCount); err != nil {
		t.Fatalf("count context chunks: %v", err)
	}
	if chunkCount == 0 {
		t.Fatalf("expected checkpoint to persist context_chunks rows")
	}

	issuePacket, err := s.BuildRehydratePacket(ctx, BuildPacketParams{
		Scope:     "issue",
		ScopeID:   issueID,
		Actor:     "agent-1",
		CommandID: "cmd-context-packet-issue-1",
	})
	if err != nil {
		t.Fatalf("build issue packet: %v", err)
	}
	if issuePacket.PacketID == "" || issuePacket.Scope != "issue" {
		t.Fatalf("expected issue packet metadata, got %#v", issuePacket)
	}
	stateRaw, ok := issuePacket.Packet["state"].(map[string]any)
	if !ok {
		t.Fatalf("expected issue packet state map, got %#v", issuePacket.Packet["state"])
	}
	if cycleNo, ok := stateRaw["cycle_no"].(float64); !ok || int(cycleNo) != 1 {
		t.Fatalf("expected issue packet cycle_no 1, got %#v", stateRaw["cycle_no"])
	}
	if builtFrom, ok := issuePacket.Packet["built_from_event_id"].(string); !ok || builtFrom == "" {
		t.Fatalf("expected issue packet built_from_event_id, got %#v", issuePacket.Packet["built_from_event_id"])
	}
	provenanceRaw, ok := issuePacket.Packet["provenance"].(map[string]any)
	if !ok {
		t.Fatalf("expected packet provenance map, got %#v", issuePacket.Packet["provenance"])
	}
	if provenanceCycle, ok := provenanceRaw["issue_cycle_no"].(float64); !ok || int(provenanceCycle) != 1 {
		t.Fatalf("expected packet provenance issue_cycle_no 1, got %#v", provenanceRaw["issue_cycle_no"])
	}
	gatesRaw, ok := issuePacket.Packet["gates"].([]any)
	if !ok || len(gatesRaw) == 0 {
		t.Fatalf("expected issue packet to include gate health, got %#v", issuePacket.Packet["gates"])
	}
	openLoopsRaw, ok := issuePacket.Packet["open_loops"].([]any)
	if !ok || len(openLoopsRaw) == 0 {
		t.Fatalf("expected issue packet to include open loops, got %#v", issuePacket.Packet["open_loops"])
	}
	decisionSummaryRaw, ok := issuePacket.Packet["decision_summary"].(map[string]any)
	if !ok {
		t.Fatalf("expected issue packet decision summary, got %#v", issuePacket.Packet["decision_summary"])
	}
	if _, ok := decisionSummaryRaw["linked_work_item_count"]; !ok {
		t.Fatalf("expected issue decision summary to include linked work count, got %#v", decisionSummaryRaw)
	}
	openQuestionsRaw, ok := issuePacket.Packet["open_questions"].([]any)
	if !ok || len(openQuestionsRaw) == 0 {
		t.Fatalf("expected issue packet open questions, got %#v", issuePacket.Packet["open_questions"])
	}
	linkedWorkItemsRaw, ok := issuePacket.Packet["linked_work_items"].([]any)
	if !ok {
		t.Fatalf("expected issue packet linked work items, got %#v", issuePacket.Packet["linked_work_items"])
	}
	_ = linkedWorkItemsRaw
	continuityRaw, ok := issuePacket.Packet["continuity"].(map[string]any)
	if !ok {
		t.Fatalf("expected issue packet continuity metadata, got %#v", issuePacket.Packet["continuity"])
	}
	compactionRaw, ok := continuityRaw["compaction"].(map[string]any)
	if !ok || anyToInt(compactionRaw["policy_version"]) != 1 {
		t.Fatalf("expected issue packet compaction policy metadata, got %#v", continuityRaw["compaction"])
	}
	loops, err := s.ListOpenLoops(ctx, ListOpenLoopsParams{IssueID: issueID})
	if err != nil {
		t.Fatalf("list open loops: %v", err)
	}
	if len(loops) == 0 {
		t.Fatalf("expected persisted open loops for issue %s", issueID)
	}
	var summaryCount int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(1) FROM issue_summaries WHERE issue_id = ?`, issueID).Scan(&summaryCount); err != nil {
		t.Fatalf("count issue summaries: %v", err)
	}
	if summaryCount == 0 {
		t.Fatalf("expected issue summaries to persist after packet build")
	}
	var firstSummaryJSON string
	var firstParentSummaryID sql.NullString
	if err := s.db.QueryRowContext(ctx, `
		SELECT summary_json, parent_summary_id
		FROM issue_summaries
		WHERE summary_id = ?
	`, "sum_"+issuePacket.PacketID).Scan(&firstSummaryJSON, &firstParentSummaryID); err != nil {
		t.Fatalf("read packet-derived issue summary: %v", err)
	}
	if firstParentSummaryID.Valid {
		t.Fatalf("expected first packet summary to have no parent, got %q", firstParentSummaryID.String)
	}
	var firstSummary map[string]any
	if err := json.Unmarshal([]byte(firstSummaryJSON), &firstSummary); err != nil {
		t.Fatalf("decode packet-derived issue summary: %v", err)
	}
	if _, ok := firstSummary["decision_summary"].(map[string]any); !ok {
		t.Fatalf("expected issue summary decision summary, got %#v", firstSummary["decision_summary"])
	}

	secondIssuePacket, err := s.BuildRehydratePacket(ctx, BuildPacketParams{
		Scope:     "issue",
		ScopeID:   issueID,
		Actor:     "agent-1",
		CommandID: "cmd-context-packet-issue-2",
	})
	if err != nil {
		t.Fatalf("build second issue packet: %v", err)
	}
	var secondParentSummaryID sql.NullString
	if err := s.db.QueryRowContext(ctx, `
		SELECT parent_summary_id
		FROM issue_summaries
		WHERE summary_id = ?
	`, "sum_"+secondIssuePacket.PacketID).Scan(&secondParentSummaryID); err != nil {
		t.Fatalf("read second packet-derived issue summary: %v", err)
	}
	if !secondParentSummaryID.Valid || secondParentSummaryID.String != "sum_"+issuePacket.PacketID {
		t.Fatalf("expected second packet summary parent %q, got %#v", "sum_"+issuePacket.PacketID, secondParentSummaryID)
	}

	storedIssuePacket, err := s.GetRehydratePacket(ctx, GetPacketParams{PacketID: issuePacket.PacketID})
	if err != nil {
		t.Fatalf("get stored issue packet: %v", err)
	}
	if storedIssuePacket.PacketID != issuePacket.PacketID {
		t.Fatalf("expected stored packet id %q, got %q", issuePacket.PacketID, storedIssuePacket.PacketID)
	}
	packetEvents, err := s.ListEventsForEntity(ctx, "packet", issuePacket.PacketID)
	if err != nil {
		t.Fatalf("list packet events: %v", err)
	}
	if len(packetEvents) != 1 || packetEvents[0].CorrelationID == "" {
		t.Fatalf("expected packet lineage metadata, got %#v", packetEvents)
	}

	focus, usedPacket, idempotent, err := s.UseRehydratePacket(ctx, UsePacketParams{
		AgentID:   "agent-ctx-1",
		PacketID:  issuePacket.PacketID,
		Actor:     "agent-1",
		CommandID: "cmd-context-packet-use-1",
	})
	if err != nil {
		t.Fatalf("use issue packet: %v", err)
	}
	if idempotent {
		t.Fatalf("expected first packet use to be non-idempotent")
	}
	if focus.AgentID != "agent-ctx-1" || focus.LastPacketID != issuePacket.PacketID {
		t.Fatalf("unexpected agent focus after packet use: %#v", focus)
	}
	if focus.ActiveIssueID != issueID {
		t.Fatalf("expected active issue %q, got %q", issueID, focus.ActiveIssueID)
	}
	if usedPacket.PacketID != issuePacket.PacketID {
		t.Fatalf("expected used packet id %q, got %q", issuePacket.PacketID, usedPacket.PacketID)
	}
	focusEvents, err := s.ListEventsForEntity(ctx, "focus", "agent-ctx-1")
	if err != nil {
		t.Fatalf("list focus events: %v", err)
	}
	if len(focusEvents) != 1 || focusEvents[0].EventType != "focus.used" {
		t.Fatalf("expected one focus.used event, got %#v", focusEvents)
	}
	if focusEvents[0].CausationID != packetEvents[0].EventID {
		t.Fatalf("expected focus causation_id %q, got %q", packetEvents[0].EventID, focusEvents[0].CausationID)
	}
	if focusEvents[0].CorrelationID != packetEvents[0].CorrelationID {
		t.Fatalf("expected focus correlation_id %q, got %q", packetEvents[0].CorrelationID, focusEvents[0].CorrelationID)
	}
	replayedFocus, _, idempotent, err := s.UseRehydratePacket(ctx, UsePacketParams{
		AgentID:   "agent-ctx-1",
		PacketID:  issuePacket.PacketID,
		Actor:     "agent-1",
		CommandID: "cmd-context-packet-use-1",
	})
	if err != nil {
		t.Fatalf("replay use issue packet: %v", err)
	}
	if !idempotent {
		t.Fatalf("expected replayed packet use to be idempotent")
	}
	if replayedFocus.LastPacketID != issuePacket.PacketID {
		t.Fatalf("expected replayed focus to keep last packet %q, got %q", issuePacket.PacketID, replayedFocus.LastPacketID)
	}

	rehydratedFallback, err := s.RehydrateSession(ctx, RehydrateSessionParams{SessionID: "sess-1"})
	if err != nil {
		t.Fatalf("rehydrate session (fallback): %v", err)
	}
	if rehydratedFallback.Source != "relevant-chunks-fallback" {
		t.Fatalf("expected fallback source, got %q", rehydratedFallback.Source)
	}
	fallbackContinuity, ok := rehydratedFallback.Packet.Packet["continuity"].(map[string]any)
	if !ok {
		t.Fatalf("expected fallback continuity metadata, got %#v", rehydratedFallback.Packet.Packet["continuity"])
	}
	fallbackChunks, ok := fallbackContinuity["relevant_chunks"].([]any)
	if !ok || len(fallbackChunks) == 0 {
		t.Fatalf("expected fallback to include relevant chunks, got %#v", fallbackContinuity["relevant_chunks"])
	}

	sessionPacket, err := s.BuildRehydratePacket(ctx, BuildPacketParams{
		Scope:     "session",
		ScopeID:   "sess-1",
		Actor:     "agent-1",
		CommandID: "cmd-context-packet-session-1",
	})
	if err != nil {
		t.Fatalf("build session packet: %v", err)
	}
	if sessionPacket.Scope != "session" {
		t.Fatalf("expected session scope packet, got %#v", sessionPacket)
	}
	sessionDecisionSummary, ok := sessionPacket.Packet["decision_summary"].(map[string]any)
	if !ok || anyToInt(sessionDecisionSummary["context_chunk_count"]) == 0 {
		t.Fatalf("expected session packet decision summary with context chunks, got %#v", sessionPacket.Packet["decision_summary"])
	}

	if _, err := s.ReplayProjections(ctx); err != nil {
		t.Fatalf("replay projections: %v", err)
	}
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(1) FROM sessions WHERE session_id = ?`, "sess-1").Scan(&chunkCount); err != nil {
		t.Fatalf("count replayed sessions: %v", err)
	}
	if chunkCount != 1 {
		t.Fatalf("expected replay to rebuild session row, got %d", chunkCount)
	}
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(1) FROM context_chunks WHERE session_id = ?`, "sess-1").Scan(&chunkCount); err != nil {
		t.Fatalf("count replayed context chunks: %v", err)
	}
	if chunkCount == 0 {
		t.Fatalf("expected replay to rebuild context chunks")
	}
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(1) FROM agent_focus WHERE agent_id = ?`, "agent-ctx-1").Scan(&chunkCount); err != nil {
		t.Fatalf("count replayed agent focus rows: %v", err)
	}
	if chunkCount != 1 {
		t.Fatalf("expected replay to rebuild agent focus row, got %d", chunkCount)
	}

	rehydratedPacket, err := s.RehydrateSession(ctx, RehydrateSessionParams{SessionID: "sess-1"})
	if err != nil {
		t.Fatalf("rehydrate session (packet-first): %v", err)
	}
	if rehydratedPacket.Source != "packet" {
		t.Fatalf("expected packet source, got %q", rehydratedPacket.Source)
	}
	if rehydratedPacket.Packet.PacketID != sessionPacket.PacketID {
		t.Fatalf("expected latest session packet %q, got %q", sessionPacket.PacketID, rehydratedPacket.Packet.PacketID)
	}
}

func TestSessionLifecycleSummariesAndClosedRehydrateFlow(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	session, created, err := s.CheckpointSession(ctx, CheckpointSessionParams{
		SessionID: "sess-life-1",
		Trigger:   "manual",
		Actor:     "agent-1",
		CommandID: "cmd-session-life-checkpoint-1",
	})
	if err != nil {
		t.Fatalf("checkpoint session: %v", err)
	}
	if !created {
		t.Fatalf("expected first checkpoint to create session")
	}

	activePacket, err := s.BuildRehydratePacket(ctx, BuildPacketParams{
		Scope:     "session",
		ScopeID:   session.SessionID,
		Actor:     "agent-1",
		CommandID: "cmd-session-life-packet-active-1",
	})
	if err != nil {
		t.Fatalf("build active session packet: %v", err)
	}
	if activePacket.PacketID == "" {
		t.Fatalf("expected active session packet id")
	}

	summarized, err := s.SummarizeSession(ctx, SummarizeSessionParams{
		SessionID: session.SessionID,
		Note:      "paused after initial triage",
		Actor:     "agent-1",
		CommandID: "cmd-session-life-summary-1",
	})
	if err != nil {
		t.Fatalf("summarize session: %v", err)
	}
	if summarized.SummaryEventID == "" {
		t.Fatalf("expected summary_event_id after summarize, got %#v", summarized)
	}
	if summarized.EndedAt != "" {
		t.Fatalf("expected summarized session to remain active, got %#v", summarized)
	}

	closed, err := s.CloseSession(ctx, CloseSessionParams{
		SessionID: session.SessionID,
		Reason:    "handoff complete",
		Actor:     "agent-1",
		CommandID: "cmd-session-life-close-1",
	})
	if err != nil {
		t.Fatalf("close session: %v", err)
	}
	if closed.EndedAt == "" {
		t.Fatalf("expected ended_at after close, got %#v", closed)
	}
	if closed.SummaryEventID != summarized.SummaryEventID {
		t.Fatalf("expected close to preserve summary_event_id %q, got %q", summarized.SummaryEventID, closed.SummaryEventID)
	}

	if _, _, err := s.CheckpointSession(ctx, CheckpointSessionParams{
		SessionID: session.SessionID,
		Trigger:   "manual",
		Actor:     "agent-1",
		CommandID: "cmd-session-life-checkpoint-2",
	}); err == nil || !strings.Contains(err.Error(), "is closed") {
		t.Fatalf("expected closed-session checkpoint rejection, got %v", err)
	}

	closedFallback, err := s.RehydrateSession(ctx, RehydrateSessionParams{SessionID: session.SessionID})
	if err != nil {
		t.Fatalf("rehydrate closed session fallback: %v", err)
	}
	if closedFallback.Source != "closed-session-summary" {
		t.Fatalf("expected closed-session-summary source, got %q", closedFallback.Source)
	}
	state, ok := closedFallback.Packet.Packet["state"].(map[string]any)
	if !ok || anyToString(state["status"]) != "closed" {
		t.Fatalf("expected closed session state, got %#v", closedFallback.Packet.Packet["state"])
	}
	continuity, ok := closedFallback.Packet.Packet["continuity"].(map[string]any)
	if !ok {
		t.Fatalf("expected continuity metadata, got %#v", closedFallback.Packet.Packet["continuity"])
	}
	relevantChunks, ok := continuity["relevant_chunks"].([]any)
	if !ok || len(relevantChunks) < 2 {
		t.Fatalf("expected summary and closure chunks in fallback, got %#v", continuity["relevant_chunks"])
	}

	closedPacket, err := s.BuildRehydratePacket(ctx, BuildPacketParams{
		Scope:     "session",
		ScopeID:   session.SessionID,
		Actor:     "agent-1",
		CommandID: "cmd-session-life-packet-closed-1",
	})
	if err != nil {
		t.Fatalf("build closed session packet: %v", err)
	}
	closedPacketState, ok := closedPacket.Packet["state"].(map[string]any)
	if !ok || anyToString(closedPacketState["status"]) != "closed" {
		t.Fatalf("expected closed packet state, got %#v", closedPacket.Packet["state"])
	}
	closedSummary, ok := closedPacket.Packet["decision_summary"].(map[string]any)
	if !ok || anyToString(closedSummary["summary_event_id"]) != summarized.SummaryEventID {
		t.Fatalf("expected closed packet summary metadata, got %#v", closedPacket.Packet["decision_summary"])
	}

	rehydratedPacket, err := s.RehydrateSession(ctx, RehydrateSessionParams{SessionID: session.SessionID})
	if err != nil {
		t.Fatalf("rehydrate closed session packet: %v", err)
	}
	if rehydratedPacket.Source != "packet" || rehydratedPacket.Packet.PacketID != closedPacket.PacketID {
		t.Fatalf("expected packet-first closed rehydrate, got %#v", rehydratedPacket)
	}

	sessionEvents, err := s.ListEventsForEntity(ctx, "session", session.SessionID)
	if err != nil {
		t.Fatalf("list session events: %v", err)
	}
	if len(sessionEvents) != 3 {
		t.Fatalf("expected checkpoint, summary, and close events, got %#v", sessionEvents)
	}
	if sessionEvents[1].EventType != "session.summarized" || sessionEvents[2].EventType != "session.closed" {
		t.Fatalf("unexpected session lifecycle events: %#v", sessionEvents)
	}

	var summaryChunkCount, closureChunkCount int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(1) FROM context_chunks WHERE session_id = ? AND kind = 'summary'`, session.SessionID).Scan(&summaryChunkCount); err != nil {
		t.Fatalf("count summary chunks: %v", err)
	}
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(1) FROM context_chunks WHERE session_id = ? AND kind = 'closure'`, session.SessionID).Scan(&closureChunkCount); err != nil {
		t.Fatalf("count closure chunks: %v", err)
	}
	if summaryChunkCount != 1 || closureChunkCount != 1 {
		t.Fatalf("expected summary and closure chunks, got summary=%d closure=%d", summaryChunkCount, closureChunkCount)
	}

	if _, err := s.ReplayProjections(ctx); err != nil {
		t.Fatalf("replay projections: %v", err)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx after replay: %v", err)
	}
	defer tx.Rollback()
	replayedSession, err := sessionByIDTx(ctx, tx, session.SessionID)
	if err != nil {
		t.Fatalf("load replayed session: %v", err)
	}
	if replayedSession.EndedAt == "" || replayedSession.SummaryEventID == "" {
		t.Fatalf("expected replayed lifecycle markers, got %#v", replayedSession)
	}
}
