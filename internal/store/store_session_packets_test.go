package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"testing"
)

func TestNextIssuePrefersContinuitySignalsForAgentResume(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	baselineIssueID := "mem-4343434"
	if _, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   baselineIssueID,
		Type:      "task",
		Title:     "Baseline in-progress task",
		Actor:     "agent-1",
		CommandID: "cmd-next-create-baseline-1",
	}); err != nil {
		t.Fatalf("create baseline issue: %v", err)
	}
	if _, _, _, err := s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
		IssueID:   baselineIssueID,
		Status:    "inprogress",
		Actor:     "agent-1",
		CommandID: "cmd-next-progress-baseline-1",
	}); err != nil {
		t.Fatalf("move baseline issue to inprogress: %v", err)
	}
	priority := "p0"
	if _, _, _, err := s.UpdateIssue(ctx, UpdateIssueParams{
		IssueID:   baselineIssueID,
		Priority:  &priority,
		Actor:     "agent-1",
		CommandID: "cmd-next-priority-baseline-1",
	}); err != nil {
		t.Fatalf("set baseline issue priority: %v", err)
	}

	continuityIssueID := "mem-4545454"
	if _, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   continuityIssueID,
		Type:      "task",
		Title:     "Continuity-heavy resume task",
		Actor:     "agent-1",
		CommandID: "cmd-next-create-continuity-1",
	}); err != nil {
		t.Fatalf("create continuity issue: %v", err)
	}
	definition := `{"gates":[{"id":"build","kind":"check","required":true,"criteria":{"command":"echo continuity"}}]}`
	if _, _, err := s.CreateGateTemplate(ctx, CreateGateTemplateParams{
		TemplateID:     "next-continuity",
		Version:        1,
		AppliesTo:      []string{"task"},
		DefinitionJSON: definition,
		Actor:          "human:alice",
		CommandID:      "cmd-next-template-1",
	}); err != nil {
		t.Fatalf("create continuity gate template: %v", err)
	}
	if _, _, err := s.InstantiateGateSet(ctx, InstantiateGateSetParams{
		IssueID:      continuityIssueID,
		TemplateRefs: []string{"next-continuity@1"},
		Actor:        "agent-1",
		CommandID:    "cmd-next-instantiate-1",
	}); err != nil {
		t.Fatalf("instantiate continuity gate set: %v", err)
	}
	if _, _, err := s.LockGateSet(ctx, LockGateSetParams{
		IssueID:   continuityIssueID,
		Actor:     "agent-1",
		CommandID: "cmd-next-lock-1",
	}); err != nil {
		t.Fatalf("lock continuity gate set: %v", err)
	}
	packet, err := s.BuildRehydratePacket(ctx, BuildPacketParams{
		Scope:     "issue",
		ScopeID:   continuityIssueID,
		Actor:     "agent-1",
		CommandID: "cmd-next-packet-build-1",
	})
	if err != nil {
		t.Fatalf("build continuity packet: %v", err)
	}
	if _, _, _, err := s.EvaluateGate(ctx, EvaluateGateParams{
		IssueID:      continuityIssueID,
		GateID:       "build",
		Result:       "FAIL",
		EvidenceRefs: []string{"ci://run/next-continuity-1"},
		Actor:        "agent-1",
		CommandID:    "cmd-next-gate-fail-1",
	}); err != nil {
		t.Fatalf("evaluate continuity gate fail: %v", err)
	}
	if _, _, _, err := s.UseRehydratePacket(ctx, UsePacketParams{
		AgentID:   "agent-next-1",
		PacketID:  packet.PacketID,
		Actor:     "agent-1",
		CommandID: "cmd-next-packet-use-1",
	}); err != nil {
		t.Fatalf("use continuity packet: %v", err)
	}

	baselineNext, err := s.NextIssue(ctx, "")
	if err != nil {
		t.Fatalf("next issue without continuity agent: %v", err)
	}
	if baselineNext.Candidate.Issue.ID != baselineIssueID {
		t.Fatalf("expected baseline issue %q without continuity agent, got %q", baselineIssueID, baselineNext.Candidate.Issue.ID)
	}

	resumeNext, err := s.NextIssue(ctx, "agent-next-1")
	if err != nil {
		t.Fatalf("next issue for continuity agent: %v", err)
	}
	if resumeNext.Candidate.Issue.ID != continuityIssueID {
		t.Fatalf("expected continuity issue %q, got %q", continuityIssueID, resumeNext.Candidate.Issue.ID)
	}
	if resumeNext.Considered != 2 {
		t.Fatalf("expected 2 considered issues, got %d", resumeNext.Considered)
	}

	reasonText := strings.Join(resumeNext.Candidate.Reasons, "\n")
	for _, expected := range []string{
		"matches the agent's active focus for resume",
		"agent already holds the latest recovery packet",
		"has 1 open loop(s) that need continuity",
		"1 required gate(s) are failing",
		"available issue packet is stale",
	} {
		if !strings.Contains(reasonText, expected) {
			t.Fatalf("expected next issue reasons to contain %q, got %q", expected, reasonText)
		}
	}
}

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

func TestAppendEventAutoLineageDefaults(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.Rollback()

	first, err := s.appendEventTx(ctx, tx, appendEventRequest{
		EntityType:          entityTypeSession,
		EntityID:            "sess-lineage",
		EventType:           eventTypeSessionCheckpoint,
		PayloadJSON:         `{"session_id":"sess-lineage"}`,
		Actor:               "agent-1",
		CommandID:           "cmd-lineage-1",
		EventPayloadVersion: 1,
	})
	if err != nil {
		t.Fatalf("append first lineage event: %v", err)
	}
	if first.Event.CorrelationID != "session:sess-lineage" {
		t.Fatalf("expected default correlation id, got %#v", first.Event)
	}
	if first.Event.CausationID != "" {
		t.Fatalf("expected first lineage event to have empty causation id, got %#v", first.Event)
	}

	second, err := s.appendEventTx(ctx, tx, appendEventRequest{
		EntityType:          entityTypeSession,
		EntityID:            "sess-lineage",
		EventType:           eventTypeSessionCheckpoint,
		PayloadJSON:         `{"session_id":"sess-lineage","step":2}`,
		Actor:               "agent-1",
		CommandID:           "cmd-lineage-2",
		EventPayloadVersion: 1,
	})
	if err != nil {
		t.Fatalf("append second lineage event: %v", err)
	}
	if second.Event.CorrelationID != first.Event.CorrelationID {
		t.Fatalf("expected second event to keep correlation id %q, got %#v", first.Event.CorrelationID, second.Event)
	}
	if second.Event.CausationID != first.Event.EventID {
		t.Fatalf("expected second event causation id %q, got %#v", first.Event.EventID, second.Event)
	}

	replayed, err := s.appendEventTx(ctx, tx, appendEventRequest{
		EntityType:          entityTypeSession,
		EntityID:            "sess-lineage",
		EventType:           eventTypeSessionCheckpoint,
		PayloadJSON:         `{"session_id":"sess-lineage","step":2}`,
		Actor:               "agent-1",
		CommandID:           "cmd-lineage-2",
		EventPayloadVersion: 1,
	})
	if err != nil {
		t.Fatalf("replay second lineage event: %v", err)
	}
	if !replayed.AlreadyExists {
		t.Fatalf("expected replayed append to be idempotent")
	}
	if replayed.Event.EventID != second.Event.EventID || replayed.Event.CorrelationID != second.Event.CorrelationID || replayed.Event.CausationID != second.Event.CausationID {
		t.Fatalf("expected replayed event to preserve lineage, got %#v want %#v", replayed.Event, second.Event)
	}
}
