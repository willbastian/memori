package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

const (
	worktreeStatusActive   = "Active"
	worktreeStatusArchived = "Archived"
)

func normalizeWorktreeStatus(raw string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", "active":
		return worktreeStatusActive, nil
	case "archived":
		return worktreeStatusArchived, nil
	default:
		return "", fmt.Errorf("invalid worktree status %q", raw)
	}
}

func (s *Store) RegisterWorktree(ctx context.Context, p RegisterWorktreeParams) (Worktree, Event, bool, error) {
	path := strings.TrimSpace(p.Path)
	if path == "" {
		return Worktree{}, Event{}, false, errors.New("worktree path is required")
	}
	repoRoot := strings.TrimSpace(p.RepoRoot)
	if repoRoot == "" {
		return Worktree{}, Event{}, false, errors.New("repo root is required")
	}
	actor := strings.TrimSpace(p.Actor)
	if actor == "" {
		actor = defaultActor()
	}
	commandID := strings.TrimSpace(p.CommandID)
	if commandID == "" {
		return Worktree{}, Event{}, false, errors.New("--command-id is required")
	}
	worktreeID := strings.TrimSpace(p.WorktreeID)
	if worktreeID == "" {
		worktreeID = newID("wt")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Worktree{}, Event{}, false, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	payload := worktreeRegisteredPayload{
		WorktreeID:   worktreeID,
		Path:         path,
		RepoRoot:     repoRoot,
		Branch:       strings.TrimSpace(p.Branch),
		HeadOID:      strings.TrimSpace(p.HeadOID),
		Status:       worktreeStatusActive,
		RegisteredAt: nowUTC(),
	}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return Worktree{}, Event{}, false, fmt.Errorf("marshal worktree.registered payload: %w", err)
	}

	appendRes, err := s.appendEventTx(ctx, tx, appendEventRequest{
		EntityType:          entityTypeWorktree,
		EntityID:            worktreeID,
		EventType:           eventTypeWorktreeRegistered,
		PayloadJSON:         string(payloadBytes),
		Actor:               actor,
		CommandID:           commandID,
		EventPayloadVersion: 1,
	})
	if err != nil {
		return Worktree{}, Event{}, false, err
	}
	if appendRes.Event.EventType != eventTypeWorktreeRegistered {
		return Worktree{}, Event{}, false, fmt.Errorf("command id already used by %q", appendRes.Event.EventType)
	}
	if err := applyWorktreeRegisteredProjectionTx(ctx, tx, appendRes.Event); err != nil {
		return Worktree{}, Event{}, false, err
	}

	worktree, err := worktreeByIDTx(ctx, tx, appendRes.Event.EntityID)
	if err != nil {
		return Worktree{}, Event{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return Worktree{}, Event{}, false, fmt.Errorf("commit tx: %w", err)
	}
	return worktree, appendRes.Event, appendRes.AlreadyExists, nil
}

func (s *Store) AttachWorktree(ctx context.Context, p AttachWorktreeParams) (Worktree, Event, bool, error) {
	worktreeID := strings.TrimSpace(p.WorktreeID)
	if worktreeID == "" {
		return Worktree{}, Event{}, false, errors.New("worktree id is required")
	}
	issueID, err := normalizeIssueKey(p.IssueID)
	if err != nil {
		return Worktree{}, Event{}, false, err
	}
	actor := strings.TrimSpace(p.Actor)
	if actor == "" {
		actor = defaultActor()
	}
	commandID := strings.TrimSpace(p.CommandID)
	if commandID == "" {
		return Worktree{}, Event{}, false, errors.New("--command-id is required")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Worktree{}, Event{}, false, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	worktree, err := worktreeByIDTx(ctx, tx, worktreeID)
	if err != nil {
		return Worktree{}, Event{}, false, err
	}
	if worktree.Status == worktreeStatusArchived {
		return Worktree{}, Event{}, false, fmt.Errorf("worktree %q is archived", worktreeID)
	}
	if _, err := getIssueTx(ctx, tx, issueID); err != nil {
		return Worktree{}, Event{}, false, err
	}
	if active, found, err := activeWorktreeForIssueTx(ctx, tx, issueID); err != nil {
		return Worktree{}, Event{}, false, err
	} else if found && active.WorktreeID != worktreeID {
		return Worktree{}, Event{}, false, fmt.Errorf("issue %q already has active worktree %q attached", issueID, active.WorktreeID)
	}

	payload := worktreeAttachedPayload{
		WorktreeID:  worktreeID,
		IssueIDFrom: worktree.IssueID,
		IssueIDTo:   issueID,
		AttachedAt:  nowUTC(),
	}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return Worktree{}, Event{}, false, fmt.Errorf("marshal worktree.attached payload: %w", err)
	}
	appendRes, err := s.appendEventTx(ctx, tx, appendEventRequest{
		EntityType:          entityTypeWorktree,
		EntityID:            worktreeID,
		EventType:           eventTypeWorktreeAttached,
		PayloadJSON:         string(payloadBytes),
		Actor:               actor,
		CommandID:           commandID,
		EventPayloadVersion: 1,
	})
	if err != nil {
		return Worktree{}, Event{}, false, err
	}
	if appendRes.Event.EventType != eventTypeWorktreeAttached {
		return Worktree{}, Event{}, false, fmt.Errorf("command id already used by %q", appendRes.Event.EventType)
	}
	if err := applyWorktreeAttachedProjectionTx(ctx, tx, appendRes.Event); err != nil {
		return Worktree{}, Event{}, false, err
	}

	worktree, err = worktreeByIDTx(ctx, tx, worktreeID)
	if err != nil {
		return Worktree{}, Event{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return Worktree{}, Event{}, false, fmt.Errorf("commit tx: %w", err)
	}
	return worktree, appendRes.Event, appendRes.AlreadyExists, nil
}

func (s *Store) DetachWorktree(ctx context.Context, p DetachWorktreeParams) (Worktree, Event, bool, error) {
	worktreeID := strings.TrimSpace(p.WorktreeID)
	if worktreeID == "" {
		return Worktree{}, Event{}, false, errors.New("worktree id is required")
	}
	actor := strings.TrimSpace(p.Actor)
	if actor == "" {
		actor = defaultActor()
	}
	commandID := strings.TrimSpace(p.CommandID)
	if commandID == "" {
		return Worktree{}, Event{}, false, errors.New("--command-id is required")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Worktree{}, Event{}, false, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	worktree, err := worktreeByIDTx(ctx, tx, worktreeID)
	if err != nil {
		return Worktree{}, Event{}, false, err
	}

	payload := worktreeDetachedPayload{
		WorktreeID:  worktreeID,
		IssueIDFrom: worktree.IssueID,
		DetachedAt:  nowUTC(),
	}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return Worktree{}, Event{}, false, fmt.Errorf("marshal worktree.detached payload: %w", err)
	}
	appendRes, err := s.appendEventTx(ctx, tx, appendEventRequest{
		EntityType:          entityTypeWorktree,
		EntityID:            worktreeID,
		EventType:           eventTypeWorktreeDetached,
		PayloadJSON:         string(payloadBytes),
		Actor:               actor,
		CommandID:           commandID,
		EventPayloadVersion: 1,
	})
	if err != nil {
		return Worktree{}, Event{}, false, err
	}
	if appendRes.Event.EventType != eventTypeWorktreeDetached {
		return Worktree{}, Event{}, false, fmt.Errorf("command id already used by %q", appendRes.Event.EventType)
	}
	if err := applyWorktreeDetachedProjectionTx(ctx, tx, appendRes.Event); err != nil {
		return Worktree{}, Event{}, false, err
	}

	worktree, err = worktreeByIDTx(ctx, tx, worktreeID)
	if err != nil {
		return Worktree{}, Event{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return Worktree{}, Event{}, false, fmt.Errorf("commit tx: %w", err)
	}
	return worktree, appendRes.Event, appendRes.AlreadyExists, nil
}

func (s *Store) ArchiveWorktree(ctx context.Context, p ArchiveWorktreeParams) (Worktree, Event, bool, error) {
	worktreeID := strings.TrimSpace(p.WorktreeID)
	if worktreeID == "" {
		return Worktree{}, Event{}, false, errors.New("worktree id is required")
	}
	actor := strings.TrimSpace(p.Actor)
	if actor == "" {
		actor = defaultActor()
	}
	commandID := strings.TrimSpace(p.CommandID)
	if commandID == "" {
		return Worktree{}, Event{}, false, errors.New("--command-id is required")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Worktree{}, Event{}, false, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	if _, err := worktreeByIDTx(ctx, tx, worktreeID); err != nil {
		return Worktree{}, Event{}, false, err
	}

	payload := worktreeArchivedPayload{
		WorktreeID: worktreeID,
		ArchivedAt: nowUTC(),
	}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return Worktree{}, Event{}, false, fmt.Errorf("marshal worktree.archived payload: %w", err)
	}
	appendRes, err := s.appendEventTx(ctx, tx, appendEventRequest{
		EntityType:          entityTypeWorktree,
		EntityID:            worktreeID,
		EventType:           eventTypeWorktreeArchived,
		PayloadJSON:         string(payloadBytes),
		Actor:               actor,
		CommandID:           commandID,
		EventPayloadVersion: 1,
	})
	if err != nil {
		return Worktree{}, Event{}, false, err
	}
	if appendRes.Event.EventType != eventTypeWorktreeArchived {
		return Worktree{}, Event{}, false, fmt.Errorf("command id already used by %q", appendRes.Event.EventType)
	}
	if err := applyWorktreeArchivedProjectionTx(ctx, tx, appendRes.Event); err != nil {
		return Worktree{}, Event{}, false, err
	}

	worktree, err := worktreeByIDTx(ctx, tx, worktreeID)
	if err != nil {
		return Worktree{}, Event{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return Worktree{}, Event{}, false, fmt.Errorf("commit tx: %w", err)
	}
	return worktree, appendRes.Event, appendRes.AlreadyExists, nil
}

func worktreeByIDTx(ctx context.Context, tx *sql.Tx, worktreeID string) (Worktree, error) {
	row := tx.QueryRowContext(ctx, `
		SELECT
			worktree_id, path, repo_root, COALESCE(branch, ''), COALESCE(head_oid, ''),
			COALESCE(issue_id, ''), status, created_at, updated_at, last_event_id
		FROM worktrees
		WHERE worktree_id = ?
	`, worktreeID)
	worktree, err := scanWorktree(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Worktree{}, fmt.Errorf("worktree %q not found", worktreeID)
		}
		return Worktree{}, err
	}
	return worktree, nil
}

func activeWorktreeForIssueTx(ctx context.Context, tx *sql.Tx, issueID string) (Worktree, bool, error) {
	row := tx.QueryRowContext(ctx, `
		SELECT
			worktree_id, path, repo_root, COALESCE(branch, ''), COALESCE(head_oid, ''),
			COALESCE(issue_id, ''), status, created_at, updated_at, last_event_id
		FROM worktrees
		WHERE issue_id = ?
			AND status = ?
		ORDER BY updated_at DESC, worktree_id DESC
		LIMIT 1
	`, issueID, worktreeStatusActive)
	worktree, err := scanWorktree(row)
	if err == nil {
		return worktree, true, nil
	}
	if errors.Is(err, sql.ErrNoRows) {
		return Worktree{}, false, nil
	}
	return Worktree{}, false, fmt.Errorf("query active worktree for issue %q: %w", issueID, err)
}

func scanWorktree(scanner interface{ Scan(dest ...any) error }) (Worktree, error) {
	var worktree Worktree
	if err := scanner.Scan(
		&worktree.WorktreeID,
		&worktree.Path,
		&worktree.RepoRoot,
		&worktree.Branch,
		&worktree.HeadOID,
		&worktree.IssueID,
		&worktree.Status,
		&worktree.CreatedAt,
		&worktree.UpdatedAt,
		&worktree.LastEventID,
	); err != nil {
		return Worktree{}, err
	}
	return worktree, nil
}

func buildWorktreePacketValue(worktree Worktree) map[string]any {
	value := map[string]any{
		"kind":        "worktree",
		"worktree_id": worktree.WorktreeID,
		"path":        worktree.Path,
		"repo_root":   worktree.RepoRoot,
		"status":      worktree.Status,
	}
	if strings.TrimSpace(worktree.Branch) != "" {
		value["branch"] = worktree.Branch
	}
	if strings.TrimSpace(worktree.HeadOID) != "" {
		value["head_oid"] = worktree.HeadOID
	}
	if strings.TrimSpace(worktree.IssueID) != "" {
		value["issue_id"] = worktree.IssueID
	}
	return value
}
