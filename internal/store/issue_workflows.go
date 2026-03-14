package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

func validateIssueStatusTransition(from, to string) error {
	fromStatus, err := normalizeIssueStatus(from)
	if err != nil {
		return fmt.Errorf("invalid current status %q: %w", from, err)
	}
	toStatus, err := normalizeIssueStatus(to)
	if err != nil {
		return err
	}
	if fromStatus == toStatus {
		return fmt.Errorf("issue is already in status %q", toStatus)
	}

	allowed := map[string]map[string]bool{
		"Todo":       {"InProgress": true, "Blocked": true, "WontDo": true},
		"InProgress": {"Blocked": true, "Done": true, "WontDo": true},
		"Blocked":    {"InProgress": true, "WontDo": true},
		"Done":       {"InProgress": true, "WontDo": true},
		"WontDo":     {"Todo": true},
	}
	if !allowed[fromStatus][toStatus] {
		return fmt.Errorf("invalid status transition %q -> %q", fromStatus, toStatus)
	}
	return nil
}

type lockedGateSet struct {
	GateSetID   string
	GateSetHash string
	CycleNo     int
	LockedAt    string
}

func lockedGateSetForIssueTx(ctx context.Context, tx *sql.Tx, issueID string) (lockedGateSet, bool, error) {
	var gateSet lockedGateSet
	err := tx.QueryRowContext(ctx, `
		SELECT gs.gate_set_id, gs.gate_set_hash, gs.cycle_no, gs.locked_at
		FROM gate_sets gs
		INNER JOIN work_items wi
			ON wi.id = gs.issue_id
			AND wi.current_cycle_no = gs.cycle_no
		WHERE gs.issue_id = ?
			AND gs.locked_at IS NOT NULL
		ORDER BY gs.cycle_no DESC
		LIMIT 1
	`, issueID).Scan(&gateSet.GateSetID, &gateSet.GateSetHash, &gateSet.CycleNo, &gateSet.LockedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return lockedGateSet{}, false, nil
	}
	if err != nil {
		return lockedGateSet{}, false, fmt.Errorf("query locked gate set for issue %q: %w", issueID, err)
	}
	return gateSet, true, nil
}

func lockedGateSetForIssueCycleTx(ctx context.Context, tx *sql.Tx, issueID string, cycleNo int) (lockedGateSet, bool, error) {
	var gateSet lockedGateSet
	err := tx.QueryRowContext(ctx, `
		SELECT gate_set_id, gate_set_hash, cycle_no, locked_at
		FROM gate_sets
		WHERE issue_id = ?
			AND cycle_no = ?
			AND locked_at IS NOT NULL
	`, issueID, cycleNo).Scan(&gateSet.GateSetID, &gateSet.GateSetHash, &gateSet.CycleNo, &gateSet.LockedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return lockedGateSet{}, false, nil
	}
	if err != nil {
		return lockedGateSet{}, false, fmt.Errorf("query locked gate set for issue %q cycle %d: %w", issueID, cycleNo, err)
	}
	return gateSet, true, nil
}

func validateIssueCloseEligibilityTx(ctx context.Context, tx *sql.Tx, issueID string) (*IssueCloseAuthorization, error) {
	openChildren, err := listIncompleteChildIssuesTx(ctx, tx, issueID)
	if err != nil {
		return nil, fmt.Errorf("close validation %w", err)
	}
	if len(openChildren) > 0 {
		return nil, fmt.Errorf(
			"close validation failed for issue %q: child issues must be Done first: %s",
			issueID,
			strings.Join(openChildren, ", "),
		)
	}

	gateSet, found, err := lockedGateSetForIssueTx(ctx, tx, issueID)
	if err != nil {
		return nil, fmt.Errorf("close validation %w", err)
	}
	if !found {
		return nil, fmt.Errorf("close validation failed for issue %q: no locked gate set for current cycle", issueID)
	}

	rows, err := tx.QueryContext(ctx, `
		SELECT
			r.gate_id,
			r.criteria_json,
			COALESCE((
				SELECT e.payload_json
				FROM events e
				WHERE e.entity_type = ?
					AND e.entity_id = ?
					AND e.event_type = ?
					AND json_extract(e.payload_json, '$.gate_set_id') = ?
					AND json_extract(e.payload_json, '$.gate_id') = r.gate_id
				ORDER BY e.event_order DESC
				LIMIT 1
			), '')
		FROM gate_set_items r
		WHERE r.gate_set_id = ?
			AND r.required = 1
		ORDER BY r.gate_id ASC
	`,
		entityTypeIssue, issueID, eventTypeGateEval, gateSet.GateSetID,
		gateSet.GateSetID,
	)
	if err != nil {
		return nil, fmt.Errorf("close validation list required gates for issue %q: %w", issueID, err)
	}
	defer rows.Close()

	failures := make([]string, 0)
	closeProof := &IssueCloseAuthorization{
		GateSetID:   gateSet.GateSetID,
		GateSetHash: gateSet.GateSetHash,
		Gates:       make([]IssueCloseGateProof, 0),
	}
	for rows.Next() {
		var (
			gateID       string
			criteriaJSON string
			payloadJSON  string
		)
		if err := rows.Scan(&gateID, &criteriaJSON, &payloadJSON); err != nil {
			return nil, fmt.Errorf("close validation scan required gate for issue %q: %w", issueID, err)
		}
		var criteria any
		if err := json.Unmarshal([]byte(criteriaJSON), &criteria); err != nil {
			return nil, fmt.Errorf("close validation decode required gate criteria %q for issue %q: %w", gateID, issueID, err)
		}
		if strings.TrimSpace(payloadJSON) == "" {
			failures = append(failures, gateID+"=MISSING")
			continue
		}
		payload, err := decodeGateEvaluatedPayload(payloadJSON)
		if err != nil {
			return nil, fmt.Errorf("close validation decode required gate %q for issue %q: %w", gateID, issueID, err)
		}
		normalizedResult := strings.ToUpper(strings.TrimSpace(payload.Result))
		if normalizedResult != "PASS" {
			failures = append(failures, gateID+"="+normalizedResult)
			continue
		}
		if len(payload.EvidenceRefs) == 0 {
			failures = append(failures, gateID+"=PASS_NO_PROOF")
			continue
		}
		if gateCriteriaRefMatches(criteria, "manual-validation") {
			closeProof.Gates = append(closeProof.Gates, IssueCloseGateProof{
				GateID:       payload.GateID,
				Result:       payload.Result,
				EvidenceRefs: copyStringSlice(payload.EvidenceRefs),
				Proof:        payload.Proof,
			})
			continue
		}
		if payload.Proof == nil ||
			strings.TrimSpace(payload.Proof.Runner) == "" ||
			strings.TrimSpace(payload.Proof.GateSetHash) != gateSet.GateSetHash ||
			payload.Proof.ExitCode != 0 {
			failures = append(failures, gateID+"=PASS_UNVERIFIED")
			continue
		}
		closeProof.Gates = append(closeProof.Gates, IssueCloseGateProof{
			GateID:       payload.GateID,
			Result:       payload.Result,
			EvidenceRefs: copyStringSlice(payload.EvidenceRefs),
			Proof:        payload.Proof,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("close validation iterate required gates for issue %q: %w", issueID, err)
	}

	if len(failures) > 0 {
		return nil, fmt.Errorf(
			"close validation failed for issue %q (gate_set %q): required gates not PASS: %s",
			issueID,
			gateSet.GateSetID,
			strings.Join(failures, ", "),
		)
	}
	return closeProof, nil
}

func listIncompleteChildIssuesTx(ctx context.Context, tx *sql.Tx, parentID string) ([]string, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT id, status
		FROM work_items
		WHERE parent_id = ?
			AND status NOT IN ('Done', 'WontDo')
		ORDER BY id ASC
	`, parentID)
	if err != nil {
		return nil, fmt.Errorf("list incomplete child issues for %q: %w", parentID, err)
	}
	defer rows.Close()

	children := make([]string, 0)
	for rows.Next() {
		var childID, status string
		if err := rows.Scan(&childID, &status); err != nil {
			return nil, fmt.Errorf("scan incomplete child issue for %q: %w", parentID, err)
		}
		children = append(children, childID+"="+status)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate incomplete child issues for %q: %w", parentID, err)
	}
	return children, nil
}

func validateIssueLinkForNewIssueTx(
	ctx context.Context,
	tx *sql.Tx,
	childID, childType string,
	parentIssue Issue,
) error {
	if childID == parentIssue.ID {
		return fmt.Errorf("invalid issue link %q -> %q: issue cannot be its own parent", childID, parentIssue.ID)
	}
	if err := validateParentChildTypeConstraint(parentIssue.Type, childType); err != nil {
		return err
	}
	if createsCycle, err := wouldCreateIssueLinkCycleTx(ctx, tx, childID, parentIssue.ID); err != nil {
		return err
	} else if createsCycle {
		return fmt.Errorf("invalid issue link %q -> %q: cycle detected", childID, parentIssue.ID)
	}
	return nil
}

func validateIssueLinkTx(ctx context.Context, tx *sql.Tx, childIssue, parentIssue Issue) error {
	if childIssue.ID == parentIssue.ID {
		return fmt.Errorf("invalid issue link %q -> %q: issue cannot be its own parent", childIssue.ID, parentIssue.ID)
	}
	if childIssue.ParentID == parentIssue.ID {
		return fmt.Errorf("issue %q is already linked to parent %q", childIssue.ID, parentIssue.ID)
	}
	if err := validateParentChildTypeConstraint(parentIssue.Type, childIssue.Type); err != nil {
		return err
	}
	if createsCycle, err := wouldCreateIssueLinkCycleTx(ctx, tx, childIssue.ID, parentIssue.ID); err != nil {
		return err
	} else if createsCycle {
		return fmt.Errorf("invalid issue link %q -> %q: cycle detected", childIssue.ID, parentIssue.ID)
	}
	return nil
}

func validateParentChildTypeConstraint(parentType, childType string) error {
	switch parentType {
	case "Epic":
		if childType != "Story" {
			return fmt.Errorf("invalid issue link type: parent Epic requires child Story (got %s)", childType)
		}
	case "Story":
		if childType != "Task" && childType != "Bug" {
			return fmt.Errorf("invalid issue link type: parent Story requires child Task|Bug (got %s)", childType)
		}
	default:
		return fmt.Errorf("invalid issue link type: parent %s cannot have children", parentType)
	}
	return nil
}

func wouldCreateIssueLinkCycleTx(ctx context.Context, tx *sql.Tx, childID, proposedParentID string) (bool, error) {
	current := strings.TrimSpace(proposedParentID)
	for current != "" {
		if current == childID {
			return true, nil
		}
		parentID, err := parentIDForIssueTx(ctx, tx, current)
		if err != nil {
			return false, err
		}
		current = strings.TrimSpace(parentID)
	}
	return false, nil
}

func parentIDForIssueTx(ctx context.Context, tx *sql.Tx, issueID string) (string, error) {
	var parentID sql.NullString
	err := tx.QueryRowContext(ctx, `SELECT parent_id FROM work_items WHERE id = ?`, issueID).Scan(&parentID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", fmt.Errorf("issue %q not found", issueID)
	}
	if err != nil {
		return "", fmt.Errorf("query issue parent for %q: %w", issueID, err)
	}
	if parentID.Valid {
		return parentID.String, nil
	}
	return "", nil
}
