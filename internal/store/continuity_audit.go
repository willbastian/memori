package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

const (
	continuityAuditSessionLimit = 5
	continuityAuditPacketLimit  = 4
	continuityAuditWriteLimit   = 8
	continuityAuditEventScanCap = 200
)

type continuityAuditSession struct {
	candidate ContinuitySessionCandidate
	packet    RehydratePacket
	chunks    int
}

func (s *Store) ContinuityAuditSnapshot(ctx context.Context, p ContinuityAuditSnapshotParams) (ContinuityAuditSnapshot, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ContinuityAuditSnapshot{}, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	issueID := strings.TrimSpace(p.IssueID)
	if issueID == "" {
		return ContinuityAuditSnapshot{}, fmt.Errorf("--issue is required")
	}
	issueID, err = normalizeIssueKey(issueID)
	if err != nil {
		return ContinuityAuditSnapshot{}, err
	}

	issueSnapshot, err := continuityIssueSnapshotTx(ctx, tx, issueID)
	if err != nil {
		return ContinuityAuditSnapshot{}, err
	}

	agentSnapshot := AgentContinuitySnapshot{}
	if agentID := strings.TrimSpace(p.AgentID); agentID != "" {
		agentSnapshot, err = continuityAgentSnapshotTx(ctx, tx, agentID)
		if err != nil {
			return ContinuityAuditSnapshot{}, err
		}
	}

	sessionSnapshot, err := continuitySessionSnapshotTx(ctx, tx, issueID)
	if err != nil {
		return ContinuityAuditSnapshot{}, err
	}

	sessions, err := continuityAuditSessionsTx(ctx, tx, issueID, continuityAuditSessionLimit)
	if err != nil {
		return ContinuityAuditSnapshot{}, err
	}

	resolution, err := continuityAuditResolution(ctx, tx, issueID, issueSnapshot, agentSnapshot, sessions)
	if err != nil {
		return ContinuityAuditSnapshot{}, err
	}

	snapshot := ContinuityAuditSnapshot{
		Resolution: resolution,
		Issue:      issueSnapshot,
		Agent:      agentSnapshot,
		Session:    sessionSnapshot,
	}
	for _, session := range sessions {
		snapshot.Sessions = append(snapshot.Sessions, session.candidate)
		if session.candidate.HasPacket {
			packetCandidate, err := continuitySessionPacketCandidate(ctx, tx, session, resolution)
			if err != nil {
				return ContinuityAuditSnapshot{}, err
			}
			snapshot.SessionPackets = append(snapshot.SessionPackets, packetCandidate)
		}
	}

	issuePackets, err := continuityIssuePacketCandidatesTx(ctx, tx, issueSnapshot, resolution, continuityAuditPacketLimit)
	if err != nil {
		return ContinuityAuditSnapshot{}, err
	}
	snapshot.IssuePackets = issuePackets

	alerts, err := continuityAuditAlertsTx(ctx, tx, issueID, issueSnapshot, agentSnapshot, sessions, issuePackets, snapshot.SessionPackets)
	if err != nil {
		return ContinuityAuditSnapshot{}, err
	}
	snapshot.Alerts = alerts

	writes, err := continuityAuditRecentWritesTx(ctx, tx, issueID, agentSnapshot, sessions, issuePackets, snapshot.SessionPackets, continuityAuditWriteLimit)
	if err != nil {
		return ContinuityAuditSnapshot{}, err
	}
	snapshot.RecentWrites = writes

	if err := tx.Commit(); err != nil {
		return ContinuityAuditSnapshot{}, fmt.Errorf("commit tx: %w", err)
	}
	return snapshot, nil
}

func continuityAuditSessionsTx(ctx context.Context, tx *sql.Tx, issueID string, limit int) ([]continuityAuditSession, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT s.session_id
		FROM sessions s
		JOIN events e
		  ON e.entity_type = ?
		 AND e.entity_id = s.session_id
		GROUP BY s.session_id
		ORDER BY MAX(e.event_order) DESC, s.session_id DESC
	`, entityTypeSession)
	if err != nil {
		return nil, fmt.Errorf("query continuity audit sessions for issue %q: %w", issueID, err)
	}
	defer rows.Close()

	sessions := make([]continuityAuditSession, 0, limit)
	for rows.Next() {
		if len(sessions) >= limit {
			break
		}
		var sessionID string
		if err := rows.Scan(&sessionID); err != nil {
			return nil, fmt.Errorf("scan continuity audit session id for issue %q: %w", issueID, err)
		}
		boundIssueID, found, err := sessionIssueIDTx(ctx, tx, sessionID)
		if err != nil {
			return nil, err
		}
		if !found || boundIssueID != issueID {
			continue
		}

		session, err := sessionByIDTx(ctx, tx, sessionID)
		if err != nil {
			return nil, err
		}
		packet, packetFound, err := latestPacketForScopeIDTx(ctx, tx, "session", sessionID)
		if err != nil {
			return nil, err
		}
		chunks, err := continuityAuditContextChunkCountTx(ctx, tx, sessionID)
		if err != nil {
			return nil, err
		}

		candidate := ContinuitySessionCandidate{
			Session:    session,
			Lifecycle:  sessionLifecycleStatus(session),
			HasSummary: strings.TrimSpace(session.SummaryEventID) != "",
			HasPacket:  packetFound,
		}
		if packetFound {
			candidate.PacketID = packet.PacketID
			candidate.PacketCreatedAt = packet.CreatedAt
		}
		sessions = append(sessions, continuityAuditSession{
			candidate: candidate,
			packet:    packet,
			chunks:    chunks,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate continuity audit sessions for issue %q: %w", issueID, err)
	}
	return sessions, nil
}

func continuityAuditContextChunkCountTx(ctx context.Context, tx *sql.Tx, sessionID string) (int, error) {
	var count int
	if err := tx.QueryRowContext(ctx, `
		SELECT COUNT(1)
		FROM context_chunks
		WHERE session_id = ?
	`, sessionID).Scan(&count); err != nil {
		return 0, fmt.Errorf("count context chunks for session %q: %w", sessionID, err)
	}
	return count, nil
}

func continuityAuditResolution(
	ctx context.Context,
	tx *sql.Tx,
	issueID string,
	issue IssueContinuitySnapshot,
	agent AgentContinuitySnapshot,
	sessions []continuityAuditSession,
) (ContinuityResolution, error) {
	resolution := ContinuityResolution{}

	selectedIndex := -1
	openIndex := -1
	for idx, session := range sessions {
		if openIndex == -1 && session.candidate.Lifecycle == "active" {
			openIndex = idx
		}
	}

	if agent.HasFocus {
		if agent.HasLastPacket {
			if sessionID := strings.TrimSpace(agent.LastPacket.SessionID); sessionID != "" {
				selectedIndex = continuityAuditSessionIndex(sessions, sessionID)
				if selectedIndex >= 0 {
					resolution.Source = "agent-focus-session"
					resolution.UsedFocus = true
				}
			}
			if selectedIndex == -1 && packetIssueID(agent.LastPacket) == issueID {
				if openIndex >= 0 {
					selectedIndex = openIndex
					resolution.Source = "agent-focus-issue-open"
				} else if len(sessions) > 0 {
					selectedIndex = 0
					resolution.Source = "agent-focus-issue-latest"
				}
				if selectedIndex >= 0 {
					resolution.UsedFocus = true
				}
			}
		}
		if selectedIndex == -1 && strings.TrimSpace(agent.Focus.ActiveIssueID) == issueID {
			if openIndex >= 0 {
				selectedIndex = openIndex
				resolution.Source = "agent-focus-issue-open"
			} else if len(sessions) > 0 {
				selectedIndex = 0
				resolution.Source = "agent-focus-issue-latest"
			}
			if selectedIndex >= 0 {
				resolution.UsedFocus = true
			}
		}
	}

	if selectedIndex == -1 {
		if openIndex >= 0 {
			selectedIndex = openIndex
			resolution.Source = "latest-open-issue"
		} else if len(sessions) > 0 {
			selectedIndex = 0
			resolution.Source = "latest-session-issue"
		}
	}

	if selectedIndex >= 0 {
		selected := sessions[selectedIndex]
		selected.candidate.IsSelected = true
		sessions[selectedIndex] = selected
		resolution.SessionID = selected.candidate.Session.SessionID
		if selected.candidate.HasPacket {
			resolution.PacketID = selected.candidate.PacketID
			resolution.PacketScope = "session"
			resolution.PacketSource = "packet"
		} else if selected.candidate.Lifecycle == "closed" {
			if selected.candidate.HasSummary {
				resolution.PacketSource = "closed-session-summary"
			} else {
				resolution.PacketSource = "closed-session-fallback"
			}
		} else if selected.chunks > 0 {
			resolution.PacketSource = "relevant-chunks-fallback"
		} else {
			resolution.PacketSource = "raw-events-fallback"
		}
	}

	if resolution.PacketID == "" && issue.HasPacket {
		resolution.PacketID = issue.LatestPacket.PacketID
		resolution.PacketScope = "issue"
		resolution.PacketSource = "issue-packet"
		resolution.UsedIssuePacket = true
	}

	openCount, err := openSessionCountForIssueTx(ctx, tx, issueID)
	if err != nil {
		return ContinuityResolution{}, err
	}
	switch {
	case openCount > 1:
		resolution.Status = "ambiguous"
	case resolution.PacketSource == "" && resolution.SessionID == "" && !issue.HasPacket:
		resolution.Status = "missing"
	case strings.Contains(resolution.PacketSource, "fallback") || resolution.PacketSource == "closed-session-summary":
		resolution.Status = "fallback"
	case resolution.PacketScope == "issue" && issue.PacketStale:
		resolution.Status = "stale"
	default:
		resolution.Status = "fresh"
	}

	for idx := range sessions {
		if sessions[idx].candidate.Session.SessionID != resolution.SessionID {
			continue
		}
		sessions[idx].candidate.IsSelected = true
		sessions[idx].candidate.ResolverNote = continuityResolutionNote(resolution.Source)
	}
	return resolution, nil
}

func continuityAuditSessionIndex(sessions []continuityAuditSession, sessionID string) int {
	for idx, session := range sessions {
		if session.candidate.Session.SessionID == sessionID {
			return idx
		}
	}
	return -1
}

func continuityResolutionNote(source string) string {
	switch strings.TrimSpace(source) {
	case "agent-focus-session":
		return "agent focus selected this session directly"
	case "agent-focus-issue-open":
		return "agent focus points at this issue and prefers its latest open session"
	case "agent-focus-issue-latest":
		return "agent focus points at this issue and fell back to its latest session"
	case "latest-open-issue":
		return "latest open session for this issue"
	case "latest-session-issue":
		return "latest session for this issue"
	default:
		return ""
	}
}

func continuityIssuePacketCandidatesTx(
	ctx context.Context,
	tx *sql.Tx,
	issue IssueContinuitySnapshot,
	resolution ContinuityResolution,
	limit int,
) ([]ContinuityPacketCandidate, error) {
	if !issue.HasPacket {
		return nil, nil
	}

	packetIDs, err := continuityAuditPacketIDsForScopeTx(ctx, tx, "issue", issue.IssueID, limit)
	if err != nil {
		return nil, err
	}

	candidates := make([]ContinuityPacketCandidate, 0, len(packetIDs))
	for idx, packetID := range packetIDs {
		packet, err := packetByIDTx(ctx, tx, packetID)
		if err != nil {
			return nil, err
		}
		builtFromFound, err := continuityAuditBuiltFromFound(ctx, tx, packet)
		if err != nil {
			return nil, err
		}
		status := "historical"
		note := "recent issue packet"
		if idx == 0 {
			note = "latest issue packet"
			if issue.PacketFresh {
				status = "fresh"
			} else if issue.PacketStale {
				status = "stale"
			} else {
				status = "latest"
			}
		}
		candidates = append(candidates, ContinuityPacketCandidate{
			Packet:              packet,
			Status:              status,
			ResolverNote:        note,
			BuiltFromEventFound: builtFromFound,
			IsSelected:          resolution.PacketID == packet.PacketID,
		})
	}
	return candidates, nil
}

func continuitySessionPacketCandidate(
	ctx context.Context,
	tx *sql.Tx,
	session continuityAuditSession,
	resolution ContinuityResolution,
) (ContinuityPacketCandidate, error) {
	builtFromFound, err := continuityAuditBuiltFromFound(ctx, tx, session.packet)
	if err != nil {
		return ContinuityPacketCandidate{}, err
	}
	status := "active"
	note := "latest session packet"
	if session.candidate.Lifecycle == "closed" {
		note = "closed session packet"
		if sessionPacketMatchesClosedLifecycle(session.packet, session.candidate.Session) {
			status = "closed"
		} else {
			status = "stale"
		}
	}
	return ContinuityPacketCandidate{
		Packet:              session.packet,
		Status:              status,
		ResolverNote:        note,
		BuiltFromEventFound: builtFromFound,
		IsSelected:          resolution.PacketID == session.packet.PacketID,
	}, nil
}

func continuityAuditPacketIDsForScopeTx(ctx context.Context, tx *sql.Tx, scope, scopeID string, limit int) ([]string, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT packet_id
		FROM rehydrate_packets
		WHERE scope = ?
		  AND COALESCE(scope_id, json_extract(packet_json, '$.scope_id')) = ?
		ORDER BY created_at DESC, packet_id DESC
		LIMIT ?
	`, scope, scopeID, limit)
	if err != nil {
		return nil, fmt.Errorf("query packet ids for %s:%s: %w", scope, scopeID, err)
	}
	defer rows.Close()

	ids := make([]string, 0, limit)
	for rows.Next() {
		var packetID string
		if err := rows.Scan(&packetID); err != nil {
			return nil, fmt.Errorf("scan packet id for %s:%s: %w", scope, scopeID, err)
		}
		ids = append(ids, packetID)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate packet ids for %s:%s: %w", scope, scopeID, err)
	}
	return ids, nil
}

func continuityAuditBuiltFromFound(ctx context.Context, tx *sql.Tx, packet RehydratePacket) (bool, error) {
	if strings.TrimSpace(packet.BuiltFromEventID) == "" {
		return true, nil
	}
	_, found, err := eventOrderByIDTx(ctx, tx, packet.BuiltFromEventID)
	return found, err
}

func continuityAuditAlertsTx(
	ctx context.Context,
	tx *sql.Tx,
	issueID string,
	issue IssueContinuitySnapshot,
	agent AgentContinuitySnapshot,
	sessions []continuityAuditSession,
	issuePackets []ContinuityPacketCandidate,
	sessionPackets []ContinuityPacketCandidate,
) ([]ContinuityAlert, error) {
	alerts := make([]ContinuityAlert, 0, 8)
	seen := make(map[string]struct{})
	appendAlert := func(level, code, message string) {
		key := code + "|" + message
		if _, ok := seen[key]; ok {
			return
		}
		seen[key] = struct{}{}
		alerts = append(alerts, ContinuityAlert{Level: level, Code: code, Message: message})
	}

	if issue.PacketStale {
		appendAlert("warn", "issue-packet-stale", "issue packet is stale relative to the latest continuity-relevant write")
	}

	status, err := continuityAuditIssueStatusTx(ctx, tx, issueID)
	if err != nil {
		return nil, err
	}
	if !issue.HasPacket && (status == "InProgress" || status == "Blocked") {
		appendAlert("warn", "issue-packet-missing", "active or blocked issue has no saved issue packet")
	}

	openCount, err := openSessionCountForIssueTx(ctx, tx, issueID)
	if err != nil {
		return nil, err
	}
	if openCount > 1 {
		appendAlert("warn", "multiple-open-sessions", fmt.Sprintf("%d open sessions exist for this issue", openCount))
	}

	if agent.HasFocus && strings.TrimSpace(agent.Focus.ActiveIssueID) != "" && strings.TrimSpace(agent.Focus.ActiveIssueID) != issueID {
		appendAlert("warn", "focus-other-issue", fmt.Sprintf("agent focus points at %s instead of %s", agent.Focus.ActiveIssueID, issueID))
	}
	if agent.HasFocus && strings.TrimSpace(agent.Focus.LastPacketID) != "" && !agent.HasLastPacket {
		appendAlert("warn", "focus-packet-missing", fmt.Sprintf("agent focus references missing packet %s", agent.Focus.LastPacketID))
	}
	if agent.HasFocus &&
		strings.TrimSpace(agent.Focus.ActiveIssueID) == issueID &&
		agent.Focus.ActiveCycleNo > 0 &&
		issue.CurrentCycleNo > 0 &&
		agent.Focus.ActiveCycleNo != issue.CurrentCycleNo {
		appendAlert("warn", "focus-cycle-mismatch", fmt.Sprintf("focus cycle %d does not match issue cycle %d", agent.Focus.ActiveCycleNo, issue.CurrentCycleNo))
	}

	if len(sessions) > 0 {
		latest := sessions[0]
		if latest.candidate.Lifecycle == "closed" && !latest.candidate.HasPacket {
			appendAlert("warn", "closed-session-without-packet", fmt.Sprintf("latest session %s is closed and has no saved session packet", latest.candidate.Session.SessionID))
		}
		if latest.candidate.Lifecycle == "closed" && latest.candidate.HasPacket && !sessionPacketMatchesClosedLifecycle(latest.packet, latest.candidate.Session) {
			appendAlert("warn", "closed-session-packet-mismatch", fmt.Sprintf("closed session packet for %s no longer matches the recorded lifecycle", latest.candidate.Session.SessionID))
		}
	}

	for _, session := range sessions {
		if session.chunks > 0 && !session.candidate.HasSummary && !session.candidate.HasPacket {
			appendAlert("warn", "session-unsaved", fmt.Sprintf("session %s has context chunks but no summary and no session packet", session.candidate.Session.SessionID))
		}
	}

	for _, packet := range issuePackets {
		if !packet.BuiltFromEventFound {
			appendAlert("warn", "issue-packet-built-from-missing", fmt.Sprintf("issue packet %s references an unresolved built-from event", packet.Packet.PacketID))
		}
	}
	for _, packet := range sessionPackets {
		if !packet.BuiltFromEventFound {
			appendAlert("warn", "session-packet-built-from-missing", fmt.Sprintf("session packet %s references an unresolved built-from event", packet.Packet.PacketID))
		}
	}
	return alerts, nil
}

func continuityAuditIssueStatusTx(ctx context.Context, tx *sql.Tx, issueID string) (string, error) {
	var status string
	if err := tx.QueryRowContext(ctx, `
		SELECT status
		FROM work_items
		WHERE id = ?
	`, issueID).Scan(&status); err != nil {
		return "", fmt.Errorf("query issue status for %q: %w", issueID, err)
	}
	return status, nil
}

func continuityAuditRecentWritesTx(
	ctx context.Context,
	tx *sql.Tx,
	issueID string,
	agent AgentContinuitySnapshot,
	sessions []continuityAuditSession,
	issuePackets []ContinuityPacketCandidate,
	sessionPackets []ContinuityPacketCandidate,
	limit int,
) ([]ContinuityWrite, error) {
	sessionIDs := make(map[string]struct{}, len(sessions))
	packetIDs := make(map[string]struct{}, len(issuePackets)+len(sessionPackets))
	for _, session := range sessions {
		sessionIDs[session.candidate.Session.SessionID] = struct{}{}
	}
	for _, packet := range issuePackets {
		packetIDs[packet.Packet.PacketID] = struct{}{}
	}
	for _, packet := range sessionPackets {
		packetIDs[packet.Packet.PacketID] = struct{}{}
	}

	rows, err := tx.QueryContext(ctx, `
		SELECT event_id, event_type, entity_type, entity_id, actor, command_id, created_at
		FROM events
		WHERE event_type IN (?, ?, ?, ?, ?, ?, ?)
		ORDER BY event_order DESC
		LIMIT ?
	`, eventTypeIssueCreate, eventTypeIssueUpdate, eventTypeSessionCheckpoint, eventTypeSessionSummarized, eventTypeSessionClosed, eventTypePacketBuilt, eventTypeFocusUsed, continuityAuditEventScanCap)
	if err != nil {
		return nil, fmt.Errorf("query continuity audit writes for issue %q: %w", issueID, err)
	}
	defer rows.Close()

	writes := make([]ContinuityWrite, 0, limit)
	for rows.Next() {
		var write ContinuityWrite
		if err := rows.Scan(&write.EventID, &write.EventType, &write.EntityType, &write.EntityID, &write.Actor, &write.CommandID, &write.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan continuity audit write for issue %q: %w", issueID, err)
		}

		include := false
		switch write.EntityType {
		case entityTypeIssue:
			include = write.EntityID == issueID
		case entityTypeSession:
			_, include = sessionIDs[write.EntityID]
		case entityTypePacket:
			_, include = packetIDs[write.EntityID]
		case entityTypeFocus:
			include = agent.HasFocus && write.EntityID == agent.AgentID
		}
		if !include {
			continue
		}

		writes = append(writes, write)
		if len(writes) >= limit {
			break
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate continuity audit writes for issue %q: %w", issueID, err)
	}
	return writes, nil
}
