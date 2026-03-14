package store

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
)

func (s *Store) ListOpenLoops(ctx context.Context, p ListOpenLoopsParams) ([]OpenLoop, error) {
	var (
		args         []any
		conditions   []string
		normalizedID string
	)
	if strings.TrimSpace(p.IssueID) != "" {
		issueID, err := normalizeIssueKey(p.IssueID)
		if err != nil {
			return nil, err
		}
		normalizedID = issueID
		conditions = append(conditions, "issue_id = ?")
		args = append(args, issueID)
	}
	if p.CycleNo != nil {
		if *p.CycleNo <= 0 {
			return nil, errors.New("--cycle must be > 0")
		}
		conditions = append(conditions, "cycle_no = ?")
		args = append(args, *p.CycleNo)
	}

	query := `
		SELECT loop_id, issue_id, cycle_no, loop_type, status,
			COALESCE(owner, ''), COALESCE(priority, ''), COALESCE(source_event_id, ''), updated_at
		FROM open_loops
	`
	if len(conditions) > 0 {
		query += " WHERE " + strings.Join(conditions, " AND ")
	}
	query += " ORDER BY issue_id ASC, cycle_no ASC, updated_at DESC, loop_id ASC"

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		if normalizedID != "" {
			return nil, fmt.Errorf("list open loops for issue %q: %w", normalizedID, err)
		}
		return nil, fmt.Errorf("list open loops: %w", err)
	}
	defer rows.Close()

	loops := make([]OpenLoop, 0)
	for rows.Next() {
		var item OpenLoop
		if err := rows.Scan(
			&item.LoopID,
			&item.IssueID,
			&item.CycleNo,
			&item.LoopType,
			&item.Status,
			&item.Owner,
			&item.Priority,
			&item.SourceEventID,
			&item.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan open loop row: %w", err)
		}
		loops = append(loops, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate open loops: %w", err)
	}
	return loops, nil
}

func (s *Store) ListIssues(ctx context.Context, p ListIssuesParams) ([]Issue, error) {
	args := make([]any, 0, 3)
	clauses := make([]string, 0, 3)

	if strings.TrimSpace(p.Type) != "" {
		issueType, err := normalizeIssueType(p.Type)
		if err != nil {
			return nil, err
		}
		clauses = append(clauses, "type = ?")
		args = append(args, issueType)
	}

	if strings.TrimSpace(p.Status) != "" {
		status, err := normalizeIssueStatus(p.Status)
		if err != nil {
			return nil, err
		}
		clauses = append(clauses, "status = ?")
		args = append(args, status)
	}

	if strings.TrimSpace(p.ParentID) != "" {
		clauses = append(clauses, "parent_id = ?")
		args = append(args, strings.TrimSpace(p.ParentID))
	}

	query := `
		SELECT
			id, type, title, COALESCE(parent_id, ''), status,
			COALESCE(priority, ''), COALESCE(labels_json, '[]'),
			COALESCE(description, ''), COALESCE(acceptance_criteria, ''), COALESCE(references_json, '[]'),
			created_at, updated_at, last_event_id
		FROM work_items
	`
	if len(clauses) > 0 {
		query += " WHERE " + strings.Join(clauses, " AND ")
	}
	query += " ORDER BY created_at ASC, id ASC"

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query work_items backlog: %w", err)
	}
	defer rows.Close()

	issues := make([]Issue, 0)
	for rows.Next() {
		var issue Issue
		var labelsJSON string
		var referencesJSON string
		if err := rows.Scan(
			&issue.ID,
			&issue.Type,
			&issue.Title,
			&issue.ParentID,
			&issue.Status,
			&issue.Priority,
			&labelsJSON,
			&issue.Description,
			&issue.Acceptance,
			&referencesJSON,
			&issue.CreatedAt,
			&issue.UpdatedAt,
			&issue.LastEventID,
		); err != nil {
			return nil, fmt.Errorf("scan work_item row: %w", err)
		}
		labels, err := parseLabelsJSON(labelsJSON)
		if err != nil {
			return nil, fmt.Errorf("scan work_item row labels: %w", err)
		}
		issue.Labels = labels
		references, err := parseReferencesJSON(referencesJSON)
		if err != nil {
			return nil, fmt.Errorf("scan work_item row references: %w", err)
		}
		issue.References = references
		issues = append(issues, issue)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate work_items rows: %w", err)
	}

	return issues, nil
}

func (s *Store) NextIssue(ctx context.Context, agent string) (IssueNextResult, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return IssueNextResult{}, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	agent = strings.TrimSpace(agent)
	focus, focusFound, err := agentFocusByAgentTx(ctx, tx, agent)
	if err != nil {
		return IssueNextResult{}, err
	}
	if !focusFound {
		focus = AgentFocus{}
	}

	rows, err := tx.QueryContext(ctx, `
		SELECT
			id, type, title, COALESCE(parent_id, ''), status,
			COALESCE(priority, ''), COALESCE(labels_json, '[]'),
			COALESCE(description, ''), COALESCE(acceptance_criteria, ''), COALESCE(references_json, '[]'),
			created_at, updated_at, last_event_id, current_cycle_no
		FROM work_items
		WHERE status IN ('Todo', 'InProgress')
		ORDER BY id ASC
	`)
	if err != nil {
		return IssueNextResult{}, fmt.Errorf("query next-issue candidates: %w", err)
	}

	type nextIssueSeed struct {
		Issue          Issue
		CurrentCycleNo int
	}
	seeds := make([]nextIssueSeed, 0)
	candidates := make([]IssueNextCandidate, 0)
	for rows.Next() {
		var (
			issue          Issue
			labelsJSON     string
			referencesJSON string
			currentCycleNo int
		)
		if err := rows.Scan(
			&issue.ID,
			&issue.Type,
			&issue.Title,
			&issue.ParentID,
			&issue.Status,
			&issue.Priority,
			&labelsJSON,
			&issue.Description,
			&issue.Acceptance,
			&referencesJSON,
			&issue.CreatedAt,
			&issue.UpdatedAt,
			&issue.LastEventID,
			&currentCycleNo,
		); err != nil {
			return IssueNextResult{}, fmt.Errorf("scan next-issue candidate row: %w", err)
		}
		labels, err := parseLabelsJSON(labelsJSON)
		if err != nil {
			return IssueNextResult{}, fmt.Errorf("decode candidate labels for issue %q: %w", issue.ID, err)
		}
		issue.Labels = labels
		references, err := parseReferencesJSON(referencesJSON)
		if err != nil {
			return IssueNextResult{}, fmt.Errorf("decode candidate references for issue %q: %w", issue.ID, err)
		}
		issue.References = references
		seeds = append(seeds, nextIssueSeed{
			Issue:          issue,
			CurrentCycleNo: currentCycleNo,
		})
	}
	if err := rows.Close(); err != nil {
		return IssueNextResult{}, fmt.Errorf("close next-issue candidate rows: %w", err)
	}
	if err := rows.Err(); err != nil {
		return IssueNextResult{}, fmt.Errorf("iterate next-issue candidate rows: %w", err)
	}
	for _, seed := range seeds {
		signals, err := loadIssueNextContinuitySignalsTx(ctx, tx, seed.Issue.ID, seed.Issue.LastEventID, seed.CurrentCycleNo, focus)
		if err != nil {
			return IssueNextResult{}, err
		}
		score, reasons := scoreIssueCandidate(seed.Issue, seed.Issue.Priority, signals)
		candidates = append(candidates, IssueNextCandidate{
			Issue:   seed.Issue,
			Score:   score,
			Reasons: reasons,
		})
	}
	if len(candidates) == 0 {
		return IssueNextResult{}, errors.New("no actionable issues found (Todo/InProgress)")
	}

	sort.SliceStable(candidates, func(i, j int) bool {
		if candidates[i].Score != candidates[j].Score {
			return candidates[i].Score > candidates[j].Score
		}
		if candidates[i].Issue.UpdatedAt != candidates[j].Issue.UpdatedAt {
			return candidates[i].Issue.UpdatedAt < candidates[j].Issue.UpdatedAt
		}
		return candidates[i].Issue.ID < candidates[j].Issue.ID
	})

	result := IssueNextResult{
		Agent:      agent,
		Candidate:  candidates[0],
		Candidates: candidates,
		Considered: len(candidates),
	}
	if err := tx.Commit(); err != nil {
		return IssueNextResult{}, fmt.Errorf("commit tx: %w", err)
	}
	return result, nil
}

func (s *Store) ListEventsForEntity(ctx context.Context, entityType, entityID string) ([]Event, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT
			event_id, event_order, entity_type, entity_id, entity_seq,
			event_type, payload_json, actor, command_id, causation_id,
			correlation_id, created_at, hash, prev_hash, event_payload_version
		FROM events
		WHERE entity_type = ? AND entity_id = ?
		ORDER BY event_order ASC
	`, entityType, entityID)
	if err != nil {
		return nil, fmt.Errorf("query events: %w", err)
	}
	defer rows.Close()

	events := make([]Event, 0)
	for rows.Next() {
		event, err := scanEvent(rows)
		if err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate events: %w", err)
	}
	return events, nil
}
