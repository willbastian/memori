package store

import (
	"fmt"
	"strings"
)

func newBasePacketJSON(scope, scopeID string) map[string]any {
	return map[string]any{
		"scope":             scope,
		"scope_id":          scopeID,
		"gates":             []any{},
		"open_loops":        []any{},
		"next_actions":      []any{"Review current state and continue execution"},
		"risks":             []any{},
		"decision_summary":  map[string]any{},
		"open_questions":    []any{},
		"linked_work_items": []any{},
		"continuity": map[string]any{
			"relevant_chunks": []any{},
			"compaction":      map[string]any{},
		},
	}
}

func sessionLifecycleStatus(session Session) string {
	if strings.TrimSpace(session.EndedAt) != "" {
		return "closed"
	}
	return "active"
}

func buildSessionDecisionSummary(session Session, totalChunkCount int) map[string]any {
	summary := map[string]any{
		"session_id":          session.SessionID,
		"status":              sessionLifecycleStatus(session),
		"trigger":             session.Trigger,
		"started_at":          session.StartedAt,
		"context_chunk_count": totalChunkCount,
	}
	if checkpointLatestEventID := strings.TrimSpace(anyToString(session.Checkpoint["latest_event_id"])); checkpointLatestEventID != "" {
		summary["checkpoint_latest_event_id"] = checkpointLatestEventID
	}
	if strings.TrimSpace(session.EndedAt) != "" {
		summary["ended_at"] = session.EndedAt
	}
	if strings.TrimSpace(session.SummaryEventID) != "" {
		summary["summary_event_id"] = session.SummaryEventID
	}
	return summary
}

func buildStructuredSessionSummary(session Session, totalChunkCount int, relevantChunks []any, note, latestEventID string) map[string]any {
	summary := buildSessionDecisionSummary(session, totalChunkCount)
	if latestEventID != "" {
		summary["ledger_latest_event_id"] = latestEventID
	}
	if note != "" {
		summary["note"] = note
	}
	recentKinds := make([]string, 0, len(relevantChunks))
	for _, rawChunk := range relevantChunks {
		chunk, ok := rawChunk.(map[string]any)
		if !ok {
			continue
		}
		kind := strings.TrimSpace(anyToString(chunk["kind"]))
		if kind == "" {
			continue
		}
		recentKinds = append(recentKinds, kind)
	}
	if len(recentKinds) > 0 {
		summary["recent_chunk_kinds"] = recentKinds
	}
	return summary
}

func sessionSummaryChunkContent(sessionID, note string) string {
	if note == "" {
		return fmt.Sprintf("summary for session %s", sessionID)
	}
	return fmt.Sprintf("summary for session %s: %s", sessionID, note)
}

func sessionCloseChunkContent(sessionID, reason string) string {
	if reason == "" {
		return fmt.Sprintf("closed session %s", sessionID)
	}
	return fmt.Sprintf("closed session %s: %s", sessionID, reason)
}

func sessionPacketMatchesClosedLifecycle(packet RehydratePacket, session Session) bool {
	if packet.Scope != "session" {
		return false
	}
	if strings.TrimSpace(packetScopeID(packet)) != session.SessionID {
		return false
	}
	state, ok := packet.Packet["state"].(map[string]any)
	if !ok {
		return false
	}
	if strings.ToLower(strings.TrimSpace(anyToString(state["status"]))) != "closed" {
		return false
	}
	if strings.TrimSpace(anyToString(state["ended_at"])) != strings.TrimSpace(session.EndedAt) {
		return false
	}
	if summaryEventID := strings.TrimSpace(session.SummaryEventID); summaryEventID != "" && strings.TrimSpace(anyToString(state["summary_event_id"])) != summaryEventID {
		return false
	}
	return true
}

func packetScopeID(packet RehydratePacket) string {
	if strings.TrimSpace(packet.ScopeID) != "" {
		return strings.TrimSpace(packet.ScopeID)
	}
	return strings.TrimSpace(anyToString(packet.Packet["scope_id"]))
}

func packetIssueID(packet RehydratePacket) string {
	if strings.TrimSpace(packet.IssueID) != "" {
		return strings.TrimSpace(packet.IssueID)
	}
	if packet.Scope == "issue" {
		return packetScopeID(packet)
	}
	return strings.TrimSpace(anyToString(packet.Packet["issue_id"]))
}
