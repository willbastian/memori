package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

func sessionByIDTx(ctx context.Context, tx *sql.Tx, sessionID string) (Session, error) {
	var (
		session        Session
		endedAt        sql.NullString
		summaryEventID sql.NullString
		checkpointJSON string
	)
	err := tx.QueryRowContext(ctx, `
		SELECT
			session_id,
			trigger,
			started_at,
			ended_at,
			summary_event_id,
			COALESCE(checkpoint_json, '{}'),
			created_by
		FROM sessions
		WHERE session_id = ?
	`, sessionID).Scan(
		&session.SessionID,
		&session.Trigger,
		&session.StartedAt,
		&endedAt,
		&summaryEventID,
		&checkpointJSON,
		&session.CreatedBy,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return Session{}, fmt.Errorf("session %q not found", sessionID)
	}
	if err != nil {
		return Session{}, fmt.Errorf("query session %q: %w", sessionID, err)
	}
	if endedAt.Valid {
		session.EndedAt = endedAt.String
	}
	if summaryEventID.Valid {
		session.SummaryEventID = summaryEventID.String
	}
	if strings.TrimSpace(checkpointJSON) != "" {
		if err := json.Unmarshal([]byte(checkpointJSON), &session.Checkpoint); err != nil {
			return Session{}, fmt.Errorf("decode session checkpoint_json for %q: %w", sessionID, err)
		}
	}
	return session, nil
}

func latestOpenSessionTx(ctx context.Context, tx *sql.Tx) (Session, error) {
	sessionID, err := sessionIDForQueryTx(ctx, tx, `
		SELECT s.session_id
		FROM sessions s
		JOIN events e
		  ON e.entity_type = ?
		 AND e.entity_id = s.session_id
		WHERE COALESCE(TRIM(s.ended_at), '') = ''
		GROUP BY s.session_id
		ORDER BY MAX(e.event_order) DESC, s.session_id DESC
		LIMIT 1
	`, entityTypeSession)
	if err != nil {
		return Session{}, fmt.Errorf("query latest open session: %w", err)
	}
	return sessionByIDTx(ctx, tx, sessionID)
}

func latestOpenSessionForIssueTx(ctx context.Context, tx *sql.Tx, issueID string) (Session, error) {
	sessionID, err := sessionIDForQueryTx(ctx, tx, `
		SELECT s.session_id
		FROM sessions s
		JOIN events e
		  ON e.entity_type = ?
		 AND e.entity_id = s.session_id
		WHERE COALESCE(TRIM(s.ended_at), '') = ''
		  AND TRIM(COALESCE(json_extract(s.checkpoint_json, '$.issue_id'), '')) = ?
		GROUP BY s.session_id
		ORDER BY MAX(e.event_order) DESC, s.session_id DESC
		LIMIT 1
	`, entityTypeSession, issueID)
	if err != nil {
		return Session{}, fmt.Errorf("query latest open session for issue %q: %w", issueID, err)
	}
	return sessionByIDTx(ctx, tx, sessionID)
}

func latestSessionTx(ctx context.Context, tx *sql.Tx) (Session, error) {
	sessionID, err := sessionIDForQueryTx(ctx, tx, `
		SELECT s.session_id
		FROM sessions s
		JOIN events e
		  ON e.entity_type = ?
		 AND e.entity_id = s.session_id
		GROUP BY s.session_id
		ORDER BY MAX(e.event_order) DESC, s.session_id DESC
		LIMIT 1
	`, entityTypeSession)
	if err != nil {
		return Session{}, fmt.Errorf("query latest session: %w", err)
	}
	return sessionByIDTx(ctx, tx, sessionID)
}

func latestSessionForIssueTx(ctx context.Context, tx *sql.Tx, issueID string) (Session, error) {
	sessionID, err := sessionIDForQueryTx(ctx, tx, `
		SELECT s.session_id
		FROM sessions s
		JOIN events e
		  ON e.entity_type = ?
		 AND e.entity_id = s.session_id
		WHERE TRIM(COALESCE(json_extract(s.checkpoint_json, '$.issue_id'), '')) = ?
		GROUP BY s.session_id
		ORDER BY MAX(e.event_order) DESC, s.session_id DESC
		LIMIT 1
	`, entityTypeSession, issueID)
	if err != nil {
		return Session{}, fmt.Errorf("query latest session for issue %q: %w", issueID, err)
	}
	return sessionByIDTx(ctx, tx, sessionID)
}

func sessionForCommandIDTx(ctx context.Context, tx *sql.Tx, commandID string) (Session, error) {
	sessionID, err := sessionIDForQueryTx(ctx, tx, `
		SELECT entity_id
		FROM events
		WHERE entity_type = ?
		  AND command_id = ?
		ORDER BY event_order DESC, entity_seq DESC
		LIMIT 1
	`, entityTypeSession, commandID)
	if err != nil {
		return Session{}, fmt.Errorf("query session for command %q: %w", commandID, err)
	}
	return sessionByIDTx(ctx, tx, sessionID)
}

func sessionIDForQueryTx(ctx context.Context, tx *sql.Tx, query string, args ...any) (string, error) {
	var sessionID string
	if err := tx.QueryRowContext(ctx, query, args...).Scan(&sessionID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", sql.ErrNoRows
		}
		return "", err
	}
	return sessionID, nil
}

func packetByIDTx(ctx context.Context, tx *sql.Tx, packetID string) (RehydratePacket, error) {
	var (
		packet     RehydratePacket
		packetJSON string
		builtFrom  sql.NullString
		scopeID    sql.NullString
		issueID    sql.NullString
		sessionID  sql.NullString
		issueCycle sql.NullInt64
	)
	err := tx.QueryRowContext(ctx, `
		SELECT
			packet_id,
			scope,
			COALESCE(scope_id, json_extract(packet_json, '$.scope_id')),
			COALESCE(issue_id, CASE WHEN scope = 'issue' THEN json_extract(packet_json, '$.scope_id') END),
			COALESCE(session_id, CASE WHEN scope = 'session' THEN json_extract(packet_json, '$.scope_id') END),
			issue_cycle_no,
			packet_json,
			packet_schema_version,
			built_from_event_id,
			created_at
		FROM rehydrate_packets
		WHERE packet_id = ?
	`, packetID).Scan(
		&packet.PacketID,
		&packet.Scope,
		&scopeID,
		&issueID,
		&sessionID,
		&issueCycle,
		&packetJSON,
		&packet.PacketSchemaVersion,
		&builtFrom,
		&packet.CreatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return RehydratePacket{}, fmt.Errorf("packet %q not found", packetID)
	}
	if err != nil {
		return RehydratePacket{}, fmt.Errorf("query packet %q: %w", packetID, err)
	}
	if builtFrom.Valid {
		packet.BuiltFromEventID = builtFrom.String
	}
	if scopeID.Valid {
		packet.ScopeID = scopeID.String
	}
	if issueID.Valid {
		packet.IssueID = issueID.String
	}
	if sessionID.Valid {
		packet.SessionID = sessionID.String
	}
	if issueCycle.Valid {
		packet.IssueCycleNo = int(issueCycle.Int64)
	}
	if err := json.Unmarshal([]byte(packetJSON), &packet.Packet); err != nil {
		return RehydratePacket{}, fmt.Errorf("decode packet_json for %q: %w", packetID, err)
	}
	return packet, nil
}

func latestPacketForScopeIDTx(ctx context.Context, tx *sql.Tx, scope, scopeID string) (RehydratePacket, bool, error) {
	var (
		packet        RehydratePacket
		packetJSON    string
		builtFrom     sql.NullString
		packetScopeID sql.NullString
		issueID       sql.NullString
		sessionID     sql.NullString
		issueCycle    sql.NullInt64
	)
	err := tx.QueryRowContext(ctx, `
		SELECT
			packet_id,
			scope,
			COALESCE(scope_id, json_extract(packet_json, '$.scope_id')),
			COALESCE(issue_id, CASE WHEN scope = 'issue' THEN json_extract(packet_json, '$.scope_id') END),
			COALESCE(session_id, CASE WHEN scope = 'session' THEN json_extract(packet_json, '$.scope_id') END),
			issue_cycle_no,
			packet_json,
			packet_schema_version,
			built_from_event_id,
			created_at
		FROM rehydrate_packets
		WHERE scope = ?
			AND COALESCE(scope_id, json_extract(packet_json, '$.scope_id')) = ?
		ORDER BY created_at DESC, packet_id DESC
		LIMIT 1
	`, scope, scopeID).Scan(
		&packet.PacketID,
		&packet.Scope,
		&packetScopeID,
		&issueID,
		&sessionID,
		&issueCycle,
		&packetJSON,
		&packet.PacketSchemaVersion,
		&builtFrom,
		&packet.CreatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return RehydratePacket{}, false, nil
	}
	if err != nil {
		return RehydratePacket{}, false, fmt.Errorf("query latest packet for %s:%s: %w", scope, scopeID, err)
	}
	if builtFrom.Valid {
		packet.BuiltFromEventID = builtFrom.String
	}
	if packetScopeID.Valid {
		packet.ScopeID = packetScopeID.String
	}
	if issueID.Valid {
		packet.IssueID = issueID.String
	}
	if sessionID.Valid {
		packet.SessionID = sessionID.String
	}
	if issueCycle.Valid {
		packet.IssueCycleNo = int(issueCycle.Int64)
	}
	if err := json.Unmarshal([]byte(packetJSON), &packet.Packet); err != nil {
		return RehydratePacket{}, false, fmt.Errorf("decode packet_json for %q: %w", packet.PacketID, err)
	}
	return packet, true, nil
}

func agentFocusByAgentTx(ctx context.Context, tx *sql.Tx, agent string) (AgentFocus, bool, error) {
	agent = strings.TrimSpace(agent)
	if agent == "" {
		return AgentFocus{}, false, nil
	}

	var (
		focus         AgentFocus
		activeIssueID sql.NullString
		activeCycleNo sql.NullInt64
	)
	err := tx.QueryRowContext(ctx, `
		SELECT agent_id, active_issue_id, active_cycle_no, last_packet_id, updated_at
		FROM agent_focus
		WHERE agent_id = ?
	`, agent).Scan(&focus.AgentID, &activeIssueID, &activeCycleNo, &focus.LastPacketID, &focus.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return AgentFocus{}, false, nil
	}
	if err != nil {
		return AgentFocus{}, false, fmt.Errorf("query agent focus for %q: %w", agent, err)
	}
	if activeIssueID.Valid {
		focus.ActiveIssueID = activeIssueID.String
	}
	if activeCycleNo.Valid {
		focus.ActiveCycleNo = int(activeCycleNo.Int64)
	}
	return focus, true, nil
}

func eventOrderByIDTx(ctx context.Context, tx *sql.Tx, eventID string) (int64, bool, error) {
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return 0, false, nil
	}

	var order int64
	err := tx.QueryRowContext(ctx, `
		SELECT event_order
		FROM events
		WHERE event_id = ?
	`, eventID).Scan(&order)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("query event order for %q: %w", eventID, err)
	}
	return order, true, nil
}

func latestContinuityEventOrderForIssueCycleTx(
	ctx context.Context,
	tx *sql.Tx,
	issueID string,
	issueLastEventID string,
	currentCycleNo int,
) (int64, error) {
	eventIDs := make([]string, 0, 8)
	if strings.TrimSpace(issueLastEventID) != "" {
		eventIDs = append(eventIDs, issueLastEventID)
	}

	lockedGateSet, found, err := lockedGateSetForIssueCycleTx(ctx, tx, issueID, currentCycleNo)
	if err != nil {
		return 0, err
	}
	if found {
		rows, err := tx.QueryContext(ctx, `
			SELECT COALESCE(gs.last_event_id, '')
			FROM gate_set_items i
			LEFT JOIN gate_status_projection gs
				ON gs.issue_id = ?
				AND gs.gate_set_id = i.gate_set_id
				AND gs.gate_id = i.gate_id
			WHERE i.gate_set_id = ?
		`, issueID, lockedGateSet.GateSetID)
		if err != nil {
			return 0, fmt.Errorf("query gate continuity events for issue %q: %w", issueID, err)
		}
		defer rows.Close()

		for rows.Next() {
			var eventID string
			if err := rows.Scan(&eventID); err != nil {
				return 0, fmt.Errorf("scan gate continuity event for issue %q: %w", issueID, err)
			}
			if strings.TrimSpace(eventID) != "" {
				eventIDs = append(eventIDs, eventID)
			}
		}
		if err := rows.Err(); err != nil {
			return 0, fmt.Errorf("iterate gate continuity events for issue %q: %w", issueID, err)
		}
	}

	rows, err := tx.QueryContext(ctx, `
		SELECT COALESCE(source_event_id, '')
		FROM open_loops
		WHERE issue_id = ?
			AND cycle_no = ?
	`, issueID, currentCycleNo)
	if err != nil {
		return 0, fmt.Errorf("query open-loop continuity events for issue %q cycle %d: %w", issueID, currentCycleNo, err)
	}
	defer rows.Close()

	for rows.Next() {
		var eventID string
		if err := rows.Scan(&eventID); err != nil {
			return 0, fmt.Errorf("scan open-loop continuity event for issue %q cycle %d: %w", issueID, currentCycleNo, err)
		}
		if strings.TrimSpace(eventID) != "" {
			eventIDs = append(eventIDs, eventID)
		}
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("iterate open-loop continuity events for issue %q cycle %d: %w", issueID, currentCycleNo, err)
	}

	maxOrder := int64(0)
	seen := make(map[string]struct{}, len(eventIDs))
	for _, eventID := range eventIDs {
		eventID = strings.TrimSpace(eventID)
		if eventID == "" {
			continue
		}
		if _, ok := seen[eventID]; ok {
			continue
		}
		seen[eventID] = struct{}{}
		order, found, err := eventOrderByIDTx(ctx, tx, eventID)
		if err != nil {
			return 0, err
		}
		if found && order > maxOrder {
			maxOrder = order
		}
	}
	return maxOrder, nil
}
