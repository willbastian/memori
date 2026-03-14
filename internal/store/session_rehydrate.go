package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

func (s *Store) RehydrateSession(ctx context.Context, p RehydrateSessionParams) (SessionRehydrateResult, error) {
	sessionID := strings.TrimSpace(p.SessionID)
	if sessionID == "" {
		return SessionRehydrateResult{}, errors.New("--session is required")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return SessionRehydrateResult{}, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	session, err := sessionByIDTx(ctx, tx, sessionID)
	if err != nil {
		return SessionRehydrateResult{}, err
	}

	packet, found, err := latestPacketForScopeIDTx(ctx, tx, "session", sessionID)
	if err != nil {
		return SessionRehydrateResult{}, err
	}

	result := SessionRehydrateResult{SessionID: sessionID}
	switch {
	case sessionIsClosed(session):
		result, err = rehydrateClosedSession(ctx, tx, session, packet, found)
	case found:
		result = SessionRehydrateResult{
			SessionID: sessionID,
			Source:    "packet",
			Packet:    packet,
		}
	default:
		result, err = rehydrateActiveSessionFallback(ctx, tx, session)
	}
	if err != nil {
		return SessionRehydrateResult{}, err
	}

	if err := tx.Commit(); err != nil {
		return SessionRehydrateResult{}, fmt.Errorf("commit tx: %w", err)
	}
	return result, nil
}

func rehydrateClosedSession(
	ctx context.Context,
	tx *sql.Tx,
	session Session,
	packet RehydratePacket,
	found bool,
) (SessionRehydrateResult, error) {
	if found && sessionPacketMatchesClosedLifecycle(packet, session) {
		return SessionRehydrateResult{
			SessionID: session.SessionID,
			Source:    "packet",
			Packet:    packet,
		}, nil
	}

	fallback, source, err := buildClosedSessionFallbackPacket(ctx, tx, session)
	if err != nil {
		return SessionRehydrateResult{}, err
	}
	return SessionRehydrateResult{
		SessionID: session.SessionID,
		Source:    source,
		Packet:    fallback,
	}, nil
}

func buildClosedSessionFallbackPacket(ctx context.Context, tx *sql.Tx, session Session) (RehydratePacket, string, error) {
	latestEventID, relevantChunks, totalChunkCount, eventCount, err := loadSessionFallbackInputs(ctx, tx, session.SessionID)
	if err != nil {
		return RehydratePacket{}, "", err
	}

	packetJSON := newBasePacketJSON("session", session.SessionID)
	packetJSON["goal"] = "Review closed session context"
	packetJSON["state"] = buildClosedSessionState(session)
	packetJSON["decision_summary"] = buildSessionDecisionSummary(session, totalChunkCount)
	packetJSON["source"] = "closed-session-summary"
	packetJSON["next_actions"] = []any{
		"Review how the session concluded before resuming related work",
		"Start a new session if more execution is needed",
	}

	continuity := packetJSON["continuity"].(map[string]any)
	continuity["relevant_chunks"] = relevantChunks
	continuity["compaction"] = buildCompactionPolicy("session", eventCount, 0, totalChunkCount)
	if latestEventID != "" {
		packetJSON["latest_event_id"] = latestEventID
	}

	source := "closed-session-summary"
	if strings.TrimSpace(session.SummaryEventID) == "" {
		packetJSON["source"] = "closed-session-fallback"
		source = "closed-session-fallback"
	}

	return RehydratePacket{
		Scope:               "session",
		Packet:              packetJSON,
		PacketSchemaVersion: packetSchemaVersion,
		BuiltFromEventID:    latestEventID,
		CreatedAt:           nowUTC(),
	}, source, nil
}

func rehydrateActiveSessionFallback(ctx context.Context, tx *sql.Tx, session Session) (SessionRehydrateResult, error) {
	packet, source, err := buildActiveSessionFallbackPacket(ctx, tx, session)
	if err != nil {
		return SessionRehydrateResult{}, err
	}
	return SessionRehydrateResult{
		SessionID: session.SessionID,
		Source:    source,
		Packet:    packet,
	}, nil
}

func buildActiveSessionFallbackPacket(ctx context.Context, tx *sql.Tx, session Session) (RehydratePacket, string, error) {
	latestEventID, relevantChunks, totalChunkCount, eventCount, err := loadSessionFallbackInputs(ctx, tx, session.SessionID)
	if err != nil {
		return RehydratePacket{}, "", err
	}

	packetJSON := newBasePacketJSON("session", session.SessionID)
	packetJSON["goal"] = "Resume session context"
	packetJSON["state"] = map[string]any{"session_id": session.SessionID}
	packetJSON["decision_summary"] = buildSessionDecisionSummary(session, totalChunkCount)

	continuity := packetJSON["continuity"].(map[string]any)
	continuity["relevant_chunks"] = relevantChunks
	continuity["compaction"] = buildCompactionPolicy("session", eventCount, 0, totalChunkCount)
	if latestEventID != "" {
		packetJSON["latest_event_id"] = latestEventID
	}

	source := "relevant-chunks-fallback"
	packetJSON["source"] = source
	packetJSON["next_actions"] = []any{
		"Review the latest context chunks and resume the active thread of work",
		"Build a fresh session packet once the session state is current",
	}
	if len(relevantChunks) == 0 {
		source = "raw-events-fallback"
		packetJSON["source"] = source
		packetJSON["next_actions"] = []any{
			"Build or select a packet for this session",
		}
	}

	return RehydratePacket{
		Scope:               "session",
		Packet:              packetJSON,
		PacketSchemaVersion: packetSchemaVersion,
		BuiltFromEventID:    latestEventID,
		CreatedAt:           nowUTC(),
	}, source, nil
}

func loadSessionFallbackInputs(ctx context.Context, tx *sql.Tx, sessionID string) (string, []any, int, int, error) {
	latestEventID, err := latestEventIDTx(ctx, tx)
	if err != nil {
		return "", nil, 0, 0, err
	}
	relevantChunks, totalChunkCount, err := listRelevantContextChunksTx(ctx, tx, "session", sessionID, packetRelevantChunkLimit)
	if err != nil {
		return "", nil, 0, 0, err
	}
	eventCount, err := countEventsForEntityTx(ctx, tx, entityTypeSession, sessionID)
	if err != nil {
		return "", nil, 0, 0, err
	}
	return latestEventID, relevantChunks, totalChunkCount, eventCount, nil
}

func buildClosedSessionState(session Session) map[string]any {
	state := map[string]any{
		"session_id": session.SessionID,
		"status":     "closed",
		"started_at": session.StartedAt,
	}
	if strings.TrimSpace(session.EndedAt) != "" {
		state["ended_at"] = session.EndedAt
	}
	if strings.TrimSpace(session.SummaryEventID) != "" {
		state["summary_event_id"] = session.SummaryEventID
	}
	return state
}

func sessionIsClosed(session Session) bool {
	return strings.TrimSpace(session.EndedAt) != ""
}
