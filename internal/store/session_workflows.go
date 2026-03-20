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
	issueID := strings.TrimSpace(p.IssueID)
	if issueID != "" {
		normalizedIssueID, err := normalizeIssueKey(issueID)
		if err != nil {
			return Session{}, false, fmt.Errorf("invalid issue_id: %w", err)
		}
		issueID = normalizedIssueID
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
	existingIssueID := ""
	if sessionExists {
		existingIssueID = strings.TrimSpace(anyToString(existingSession.Checkpoint["issue_id"]))
		if existingIssueID == "" {
			existingIssueID, err = latestCheckpointIssueIDForSessionTx(ctx, tx, sessionID)
			if err != nil {
				return Session{}, false, fmt.Errorf("resolve existing issue binding for session %q: %w", sessionID, err)
			}
		}
	}
	if sessionExists && issueID == "" && existingIssueID != "" {
		issueID = existingIssueID
	}
	if sessionExists && issueID != "" && existingIssueID != "" && issueID != existingIssueID {
		return Session{}, false, fmt.Errorf("session %q already tracks issue %q; start a new session to work on %q", sessionID, existingIssueID, issueID)
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
	if issueID != "" {
		checkpoint["issue_id"] = issueID
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
	if issueID != "" {
		chunkMetadata["issue_id"] = issueID
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

func (s *Store) LatestOpenSessionForIssue(ctx context.Context, issueID string) (Session, bool, error) {
	normalizedIssueID, err := normalizeIssueKey(issueID)
	if err != nil {
		return Session{}, false, err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Session{}, false, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	session, err := latestOpenSessionForIssueTx(ctx, tx, normalizedIssueID)
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

func (s *Store) LatestSessionForIssue(ctx context.Context, issueID string) (Session, bool, error) {
	normalizedIssueID, err := normalizeIssueKey(issueID)
	if err != nil {
		return Session{}, false, err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Session{}, false, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	session, err := latestSessionForIssueTx(ctx, tx, normalizedIssueID)
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

func (s *Store) GetSession(ctx context.Context, sessionID string) (Session, error) {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return Session{}, errors.New("--session is required")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Session{}, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	return sessionByIDTx(ctx, tx, sessionID)
}

func (s *Store) GetAgentFocus(ctx context.Context, agentID string) (AgentFocus, bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return AgentFocus{}, false, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	return agentFocusByAgentTx(ctx, tx, agentID)
}

func (s *Store) OpenSessionCountForIssue(ctx context.Context, issueID string) (int, error) {
	normalizedIssueID, err := normalizeIssueKey(issueID)
	if err != nil {
		return 0, err
	}

	var count int
	if err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(1)
		FROM sessions
		WHERE COALESCE(TRIM(ended_at), '') = ''
		  AND TRIM(COALESCE(json_extract(checkpoint_json, '$.issue_id'), '')) = ?
	`, normalizedIssueID).Scan(&count); err != nil {
		return 0, fmt.Errorf("count open sessions for issue %q: %w", normalizedIssueID, err)
	}
	return count, nil
}
