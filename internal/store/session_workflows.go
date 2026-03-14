package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

func (s *Store) CheckpointSession(ctx context.Context, p CheckpointSessionParams) (Session, bool, error) {
	sessionID := strings.TrimSpace(p.SessionID)
	if sessionID == "" {
		return Session{}, false, errors.New("--session is required")
	}
	trigger := strings.TrimSpace(p.Trigger)
	if trigger == "" {
		trigger = "manual"
	}
	actor := strings.TrimSpace(p.Actor)
	if actor == "" {
		actor = defaultActor()
	}
	commandID := strings.TrimSpace(p.CommandID)
	if commandID == "" {
		return Session{}, false, errors.New("--command-id is required")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Session{}, false, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	existingSession, err := sessionByIDTx(ctx, tx, sessionID)
	sessionExists := err == nil
	if err != nil && !strings.Contains(err.Error(), "not found") {
		return Session{}, false, err
	}
	if sessionExists && strings.TrimSpace(existingSession.EndedAt) != "" {
		return Session{}, false, fmt.Errorf("session %q is closed; start a new session id to checkpoint more work", sessionID)
	}

	now := nowUTC()
	latestEventID, err := latestEventIDTx(ctx, tx)
	if err != nil {
		return Session{}, false, err
	}
	checkpoint := map[string]any{
		"session_id":  sessionID,
		"trigger":     trigger,
		"captured_at": now,
	}
	if latestEventID != "" {
		checkpoint["latest_event_id"] = latestEventID
	}

	startedAt := now
	createdBy := actor
	if sessionExists {
		startedAt = existingSession.StartedAt
		createdBy = existingSession.CreatedBy
	}
	chunkID := newID("chk")
	chunkMetadata := map[string]any{
		"trigger":         trigger,
		"latest_event_id": latestEventID,
	}
	payloadBytes, err := json.Marshal(sessionCheckpointedPayload{
		SessionID:           sessionID,
		Trigger:             trigger,
		StartedAt:           startedAt,
		Checkpoint:          checkpoint,
		CheckpointedAt:      now,
		ContextChunkID:      chunkID,
		ContextChunkKind:    "checkpoint",
		ContextChunkContent: fmt.Sprintf("checkpoint for session %s", sessionID),
		ContextChunkMeta:    chunkMetadata,
		CreatedBy:           createdBy,
	})
	if err != nil {
		return Session{}, false, fmt.Errorf("marshal session checkpoint payload: %w", err)
	}

	appendRes, err := s.appendEventTx(ctx, tx, appendEventRequest{
		EntityType:          entityTypeSession,
		EntityID:            sessionID,
		EventType:           eventTypeSessionCheckpoint,
		PayloadJSON:         string(payloadBytes),
		Actor:               actor,
		CommandID:           commandID,
		EventPayloadVersion: 1,
	})
	if err != nil {
		return Session{}, false, err
	}
	if appendRes.Event.EventType != eventTypeSessionCheckpoint {
		return Session{}, false, fmt.Errorf("command id already used by %q", appendRes.Event.EventType)
	}
	if err := applySessionCheckpointedProjectionTx(ctx, tx, appendRes.Event); err != nil {
		return Session{}, false, err
	}

	session, err := sessionByIDTx(ctx, tx, sessionID)
	if err != nil {
		return Session{}, false, err
	}

	if err := tx.Commit(); err != nil {
		return Session{}, false, fmt.Errorf("commit tx: %w", err)
	}
	return session, appendRes.Event.EntitySeq == 1, nil
}

func (s *Store) SummarizeSession(ctx context.Context, p SummarizeSessionParams) (Session, error) {
	sessionID := strings.TrimSpace(p.SessionID)
	if sessionID == "" {
		return Session{}, errors.New("--session is required")
	}
	actor := strings.TrimSpace(p.Actor)
	if actor == "" {
		actor = defaultActor()
	}
	commandID := strings.TrimSpace(p.CommandID)
	if commandID == "" {
		return Session{}, errors.New("--command-id is required")
	}
	note := strings.TrimSpace(p.Note)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Session{}, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	session, err := sessionByIDTx(ctx, tx, sessionID)
	if err != nil {
		return Session{}, err
	}
	relevantChunks, totalChunkCount, err := listRelevantContextChunksTx(ctx, tx, "session", sessionID, packetRelevantChunkLimit)
	if err != nil {
		return Session{}, err
	}
	latestEventID, err := latestEventIDTx(ctx, tx)
	if err != nil {
		return Session{}, err
	}
	summary := buildStructuredSessionSummary(session, totalChunkCount, relevantChunks, note, latestEventID)
	summarizedAt := nowUTC()
	chunkID := newID("chk")
	chunkMetadata := map[string]any{
		"summary":         summary,
		"latest_event_id": latestEventID,
	}
	if note != "" {
		chunkMetadata["note"] = note
	}
	payloadBytes, err := json.Marshal(sessionSummarizedPayload{
		SessionID:           sessionID,
		Summary:             summary,
		SummarizedAt:        summarizedAt,
		ContextChunkID:      chunkID,
		ContextChunkKind:    "summary",
		ContextChunkContent: sessionSummaryChunkContent(sessionID, note),
		ContextChunkMeta:    chunkMetadata,
	})
	if err != nil {
		return Session{}, fmt.Errorf("marshal session summary payload: %w", err)
	}

	appendRes, err := s.appendEventTx(ctx, tx, appendEventRequest{
		EntityType:          entityTypeSession,
		EntityID:            sessionID,
		EventType:           eventTypeSessionSummarized,
		PayloadJSON:         string(payloadBytes),
		Actor:               actor,
		CommandID:           commandID,
		EventPayloadVersion: 1,
	})
	if err != nil {
		return Session{}, err
	}
	if appendRes.Event.EventType != eventTypeSessionSummarized {
		return Session{}, fmt.Errorf("command id already used by %q", appendRes.Event.EventType)
	}
	if err := applySessionSummarizedProjectionTx(ctx, tx, appendRes.Event); err != nil {
		return Session{}, err
	}

	session, err = sessionByIDTx(ctx, tx, sessionID)
	if err != nil {
		return Session{}, err
	}

	if err := tx.Commit(); err != nil {
		return Session{}, fmt.Errorf("commit tx: %w", err)
	}
	return session, nil
}

func (s *Store) CloseSession(ctx context.Context, p CloseSessionParams) (Session, error) {
	sessionID := strings.TrimSpace(p.SessionID)
	if sessionID == "" {
		return Session{}, errors.New("--session is required")
	}
	actor := strings.TrimSpace(p.Actor)
	if actor == "" {
		actor = defaultActor()
	}
	commandID := strings.TrimSpace(p.CommandID)
	if commandID == "" {
		return Session{}, errors.New("--command-id is required")
	}
	reason := strings.TrimSpace(p.Reason)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Session{}, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	session, err := sessionByIDTx(ctx, tx, sessionID)
	if err != nil {
		return Session{}, err
	}
	if strings.TrimSpace(session.EndedAt) != "" {
		return Session{}, fmt.Errorf("session %q is already closed", sessionID)
	}
	closedAt := nowUTC()
	latestEventID, err := latestEventIDTx(ctx, tx)
	if err != nil {
		return Session{}, err
	}
	chunkID := newID("chk")
	chunkMetadata := map[string]any{
		"latest_event_id": latestEventID,
		"status":          "closed",
	}
	if reason != "" {
		chunkMetadata["reason"] = reason
	}
	if strings.TrimSpace(session.SummaryEventID) != "" {
		chunkMetadata["summary_event_id"] = session.SummaryEventID
	}
	payloadBytes, err := json.Marshal(sessionClosedPayload{
		SessionID:           sessionID,
		EndedAt:             closedAt,
		SummaryEventID:      session.SummaryEventID,
		Reason:              reason,
		ClosedAt:            closedAt,
		ContextChunkID:      chunkID,
		ContextChunkKind:    "closure",
		ContextChunkContent: sessionCloseChunkContent(sessionID, reason),
		ContextChunkMeta:    chunkMetadata,
	})
	if err != nil {
		return Session{}, fmt.Errorf("marshal session close payload: %w", err)
	}

	appendRes, err := s.appendEventTx(ctx, tx, appendEventRequest{
		EntityType:          entityTypeSession,
		EntityID:            sessionID,
		EventType:           eventTypeSessionClosed,
		PayloadJSON:         string(payloadBytes),
		Actor:               actor,
		CommandID:           commandID,
		EventPayloadVersion: 1,
	})
	if err != nil {
		return Session{}, err
	}
	if appendRes.Event.EventType != eventTypeSessionClosed {
		return Session{}, fmt.Errorf("command id already used by %q", appendRes.Event.EventType)
	}
	if err := applySessionClosedProjectionTx(ctx, tx, appendRes.Event); err != nil {
		return Session{}, err
	}

	session, err = sessionByIDTx(ctx, tx, sessionID)
	if err != nil {
		return Session{}, err
	}

	if err := tx.Commit(); err != nil {
		return Session{}, fmt.Errorf("commit tx: %w", err)
	}
	return session, nil
}

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

	result := SessionRehydrateResult{
		SessionID: sessionID,
	}
	if strings.TrimSpace(session.EndedAt) != "" {
		if found && sessionPacketMatchesClosedLifecycle(packet, session) {
			result.Source = "packet"
			result.Packet = packet
		} else {
			latestEventID, err := latestEventIDTx(ctx, tx)
			if err != nil {
				return SessionRehydrateResult{}, err
			}
			relevantChunks, totalChunkCount, err := listRelevantContextChunksTx(ctx, tx, "session", sessionID, packetRelevantChunkLimit)
			if err != nil {
				return SessionRehydrateResult{}, err
			}
			eventCount, err := countEventsForEntityTx(ctx, tx, entityTypeSession, sessionID)
			if err != nil {
				return SessionRehydrateResult{}, err
			}
			packetJSON := newBasePacketJSON("session", sessionID)
			packetJSON["goal"] = "Review closed session context"
			state := map[string]any{
				"session_id": sessionID,
				"status":     "closed",
				"started_at": session.StartedAt,
			}
			if strings.TrimSpace(session.EndedAt) != "" {
				state["ended_at"] = session.EndedAt
			}
			if strings.TrimSpace(session.SummaryEventID) != "" {
				state["summary_event_id"] = session.SummaryEventID
			}
			packetJSON["state"] = state
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
			if strings.TrimSpace(session.SummaryEventID) == "" {
				packetJSON["source"] = "closed-session-fallback"
				result.Source = "closed-session-fallback"
			} else {
				result.Source = "closed-session-summary"
			}
			result.Packet = RehydratePacket{
				Scope:               "session",
				Packet:              packetJSON,
				PacketSchemaVersion: packetSchemaVersion,
				BuiltFromEventID:    latestEventID,
				CreatedAt:           nowUTC(),
			}
		}
	} else if found {
		result.Source = "packet"
		result.Packet = packet
	} else {
		latestEventID, err := latestEventIDTx(ctx, tx)
		if err != nil {
			return SessionRehydrateResult{}, err
		}
		relevantChunks, totalChunkCount, err := listRelevantContextChunksTx(ctx, tx, "session", sessionID, packetRelevantChunkLimit)
		if err != nil {
			return SessionRehydrateResult{}, err
		}
		eventCount, err := countEventsForEntityTx(ctx, tx, entityTypeSession, sessionID)
		if err != nil {
			return SessionRehydrateResult{}, err
		}
		packetJSON := newBasePacketJSON("session", sessionID)
		packetJSON["goal"] = "Resume session context"
		packetJSON["state"] = map[string]any{"session_id": sessionID}
		packetJSON["decision_summary"] = buildSessionDecisionSummary(session, totalChunkCount)
		continuity := packetJSON["continuity"].(map[string]any)
		continuity["relevant_chunks"] = relevantChunks
		continuity["compaction"] = buildCompactionPolicy("session", eventCount, 0, totalChunkCount)
		if latestEventID != "" {
			packetJSON["latest_event_id"] = latestEventID
		}
		if len(relevantChunks) > 0 {
			packetJSON["next_actions"] = []any{
				"Review the latest context chunks and resume the active thread of work",
				"Build a fresh session packet once the session state is current",
			}
			packetJSON["source"] = "relevant-chunks-fallback"
			result.Source = "relevant-chunks-fallback"
		} else {
			packetJSON["next_actions"] = []any{
				"Build or select a packet for this session",
			}
			packetJSON["source"] = "raw-events-fallback"
			result.Source = "raw-events-fallback"
		}
		result.Packet = RehydratePacket{
			Scope:               "session",
			Packet:              packetJSON,
			PacketSchemaVersion: packetSchemaVersion,
			BuiltFromEventID:    latestEventID,
			CreatedAt:           nowUTC(),
		}
	}

	if err := tx.Commit(); err != nil {
		return SessionRehydrateResult{}, fmt.Errorf("commit tx: %w", err)
	}
	return result, nil
}

func (s *Store) LatestOpenSession(ctx context.Context) (Session, bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Session{}, false, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	session, err := latestOpenSessionTx(ctx, tx)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Session{}, false, nil
		}
		return Session{}, false, err
	}
	return session, true, nil
}

func (s *Store) LatestSession(ctx context.Context) (Session, bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Session{}, false, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	session, err := latestSessionTx(ctx, tx)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Session{}, false, nil
		}
		return Session{}, false, err
	}
	return session, true, nil
}

func (s *Store) SessionForCommand(ctx context.Context, commandID string) (Session, bool, error) {
	commandID = strings.TrimSpace(commandID)
	if commandID == "" {
		return Session{}, false, nil
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Session{}, false, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	session, err := sessionForCommandIDTx(ctx, tx, commandID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Session{}, false, nil
		}
		return Session{}, false, err
	}
	return session, true, nil
}
