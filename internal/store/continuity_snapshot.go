package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

func (s *Store) ContinuitySnapshot(ctx context.Context, p ContinuitySnapshotParams) (ContinuitySnapshot, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ContinuitySnapshot{}, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	snapshot := ContinuitySnapshot{}

	if issueID := strings.TrimSpace(p.IssueID); issueID != "" {
		normalizedIssueID, err := normalizeIssueKey(issueID)
		if err != nil {
			return ContinuitySnapshot{}, err
		}
		issueSnapshot, err := continuityIssueSnapshotTx(ctx, tx, normalizedIssueID)
		if err != nil {
			return ContinuitySnapshot{}, err
		}
		snapshot.Issue = issueSnapshot
	}

	if agentID := strings.TrimSpace(p.AgentID); agentID != "" {
		agentSnapshot, err := continuityAgentSnapshotTx(ctx, tx, agentID)
		if err != nil {
			return ContinuitySnapshot{}, err
		}
		snapshot.Agent = agentSnapshot
	}

	sessionSnapshot, err := continuitySessionSnapshotTx(ctx, tx, strings.TrimSpace(p.IssueID))
	if err != nil {
		return ContinuitySnapshot{}, err
	}
	snapshot.Session = sessionSnapshot

	return snapshot, nil
}

func continuityIssueSnapshotTx(ctx context.Context, tx *sql.Tx, issueID string) (IssueContinuitySnapshot, error) {
	snapshot := IssueContinuitySnapshot{IssueID: issueID}
	if err := tx.QueryRowContext(ctx, `
		SELECT current_cycle_no, COALESCE(last_event_id, '')
		FROM work_items
		WHERE id = ?
	`, issueID).Scan(&snapshot.CurrentCycleNo, &snapshot.LastEventID); err != nil {
		if err == sql.ErrNoRows {
			return IssueContinuitySnapshot{}, fmt.Errorf("issue %q not found", issueID)
		}
		return IssueContinuitySnapshot{}, fmt.Errorf("query continuity issue %q: %w", issueID, err)
	}

	if err := tx.QueryRowContext(ctx, `
		SELECT COUNT(1)
		FROM open_loops
		WHERE issue_id = ?
			AND cycle_no = ?
			AND status = 'Open'
	`, issueID, snapshot.CurrentCycleNo).Scan(&snapshot.OpenLoopCount); err != nil {
		return IssueContinuitySnapshot{}, fmt.Errorf("query open loops for issue %q: %w", issueID, err)
	}

	packet, found, err := latestPacketForScopeIDTx(ctx, tx, "issue", issueID)
	if err != nil {
		return IssueContinuitySnapshot{}, err
	}
	if !found {
		return snapshot, nil
	}

	snapshot.LatestPacket = packet
	snapshot.HasPacket = true

	packetEventOrder, packetEventFound, err := eventOrderByIDTx(ctx, tx, packet.BuiltFromEventID)
	if err != nil {
		return IssueContinuitySnapshot{}, err
	}
	latestContinuityOrder, err := latestContinuityEventOrderForIssueCycleTx(ctx, tx, issueID, snapshot.LastEventID, snapshot.CurrentCycleNo)
	if err != nil {
		return IssueContinuitySnapshot{}, err
	}
	if issuePacketCycleNo(packet) == snapshot.CurrentCycleNo && ((packetEventFound && packetEventOrder >= latestContinuityOrder) || (!packetEventFound && latestContinuityOrder == 0)) {
		snapshot.PacketFresh = true
		return snapshot, nil
	}

	snapshot.PacketStale = true
	return snapshot, nil
}

func continuityAgentSnapshotTx(ctx context.Context, tx *sql.Tx, agentID string) (AgentContinuitySnapshot, error) {
	snapshot := AgentContinuitySnapshot{AgentID: agentID}
	focus, found, err := agentFocusByAgentTx(ctx, tx, agentID)
	if err != nil {
		return AgentContinuitySnapshot{}, err
	}
	if !found {
		return snapshot, nil
	}

	snapshot.Focus = focus
	snapshot.HasFocus = true
	if strings.TrimSpace(focus.LastPacketID) == "" {
		return snapshot, nil
	}

	packet, err := packetByIDTx(ctx, tx, focus.LastPacketID)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			return snapshot, nil
		}
		return AgentContinuitySnapshot{}, err
	}
	snapshot.LastPacket = packet
	snapshot.HasLastPacket = true
	return snapshot, nil
}

func continuitySessionSnapshotTx(ctx context.Context, tx *sql.Tx, issueID string) (SessionContinuitySnapshot, error) {
	session, source, found, err := latestContinuitySessionTx(ctx, tx, issueID)
	if err != nil {
		return SessionContinuitySnapshot{}, err
	}
	if !found {
		return SessionContinuitySnapshot{}, nil
	}

	snapshot := SessionContinuitySnapshot{
		Source:     source,
		Session:    session,
		HasSession: true,
	}
	packet, found, err := latestPacketForScopeIDTx(ctx, tx, "session", session.SessionID)
	if err != nil {
		return SessionContinuitySnapshot{}, err
	}
	if found {
		snapshot.Packet = packet
		snapshot.HasPacket = true
	}
	return snapshot, nil
}

func latestContinuitySessionTx(ctx context.Context, tx *sql.Tx, issueID string) (Session, string, bool, error) {
	issueID = strings.TrimSpace(issueID)
	if issueID != "" {
		session, err := latestOpenSessionForIssueTx(ctx, tx, issueID)
		if err == nil {
			return session, "latest-open-issue", true, nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return Session{}, "", false, err
		}

		session, err = latestSessionForIssueTx(ctx, tx, issueID)
		if err == nil {
			return session, "latest-session-issue", true, nil
		}
		if errors.Is(err, sql.ErrNoRows) {
			return Session{}, "", false, nil
		}
		return Session{}, "", false, err
	}

	session, err := latestOpenSessionTx(ctx, tx)
	if err == nil {
		return session, "latest-open", true, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return Session{}, "", false, err
	}

	session, err = latestSessionTx(ctx, tx)
	if err == nil {
		return session, "latest-session", true, nil
	}
	if errors.Is(err, sql.ErrNoRows) {
		return Session{}, "", false, nil
	}
	return Session{}, "", false, err
}
