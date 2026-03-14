package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

func (s *Store) EvaluateGate(ctx context.Context, p EvaluateGateParams) (GateEvaluation, Event, bool, error) {
	if p.Actor == "" {
		p.Actor = defaultActor()
	}
	if strings.TrimSpace(p.CommandID) == "" {
		return GateEvaluation{}, Event{}, false, errors.New("--command-id is required")
	}
	if len(normalizeReferences(p.EvidenceRefs)) == 0 {
		return GateEvaluation{}, Event{}, false, errors.New("--evidence is required")
	}

	issueID, err := normalizeIssueKey(p.IssueID)
	if err != nil {
		return GateEvaluation{}, Event{}, false, err
	}
	gateID := strings.TrimSpace(p.GateID)
	if gateID == "" {
		return GateEvaluation{}, Event{}, false, errors.New("--gate is required")
	}
	result, err := normalizeGateResult(p.Result)
	if err != nil {
		return GateEvaluation{}, Event{}, false, err
	}
	evidenceRefs := normalizeReferences(p.EvidenceRefs)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return GateEvaluation{}, Event{}, false, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	if existingEvent, found, err := findEventByActorCommandTx(ctx, tx, p.Actor, p.CommandID); err != nil {
		return GateEvaluation{}, Event{}, false, err
	} else if found {
		if existingEvent.EventType != eventTypeGateEval {
			return GateEvaluation{}, Event{}, false, fmt.Errorf("command id already used by %q", existingEvent.EventType)
		}
		payload, err := decodeGateEvaluatedPayload(existingEvent.PayloadJSON)
		if err != nil {
			return GateEvaluation{}, Event{}, false, err
		}
		if err := tx.Commit(); err != nil {
			return GateEvaluation{}, Event{}, false, fmt.Errorf("commit tx: %w", err)
		}
		return GateEvaluation{
			IssueID:      payload.IssueID,
			GateSetID:    payload.GateSetID,
			GateID:       payload.GateID,
			Result:       payload.Result,
			EvidenceRefs: payload.EvidenceRefs,
			Proof:        payload.Proof,
			EvaluatedAt:  payload.EvaluatedAt,
		}, existingEvent, true, nil
	}

	if _, err := getIssueTx(ctx, tx, issueID); err != nil {
		return GateEvaluation{}, Event{}, false, err
	}

	gateSet, found, err := lockedGateSetForIssueTx(ctx, tx, issueID)
	if err != nil {
		return GateEvaluation{}, Event{}, false, err
	}
	if !found {
		return GateEvaluation{}, Event{}, false, fmt.Errorf("no locked gate set found for issue %q", issueID)
	}
	gateSetEvent, _, err := latestEventForEntityTx(ctx, tx, entityTypeGateSet, gateSet.GateSetID)
	if err != nil {
		return GateEvaluation{}, Event{}, false, err
	}
	proof := normalizeGateEvaluationProof(p.Proof)
	if proof != nil && strings.TrimSpace(proof.GateSetHash) == "" {
		proof.GateSetHash = gateSet.GateSetHash
	}

	var (
		requiredInt  int
		criteriaJSON string
	)
	if err := tx.QueryRowContext(ctx, `
		SELECT required, criteria_json
		FROM gate_set_items
		WHERE gate_set_id = ? AND gate_id = ?
	`, gateSet.GateSetID, gateID).Scan(&requiredInt, &criteriaJSON); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return GateEvaluation{}, Event{}, false, fmt.Errorf(
				"gate %q is not defined in locked gate_set %q for issue %q",
				gateID,
				gateSet.GateSetID,
				issueID,
			)
		}
		return GateEvaluation{}, Event{}, false, fmt.Errorf("lookup gate %q in gate_set %q: %w", gateID, gateSet.GateSetID, err)
	}

	var criteria any
	if err := json.Unmarshal([]byte(criteriaJSON), &criteria); err != nil {
		return GateEvaluation{}, Event{}, false, fmt.Errorf("decode criteria_json for gate %q in gate_set %q: %w", gateID, gateSet.GateSetID, err)
	}
	if result == "PASS" && gateCriteriaCommand(criteria) != "" && proof == nil {
		return GateEvaluation{}, Event{}, false, fmt.Errorf(
			"gate %q uses executable criteria.command; use memori gate verify --issue %s --gate %s to record PASS",
			gateID,
			issueID,
			gateID,
		)
	}
	if requiredInt == 0 {
	} else if result == "PASS" && gateCriteriaCommand(criteria) == "" && proof != nil {
		return GateEvaluation{}, Event{}, false, fmt.Errorf("gate %q has no executable criteria.command and cannot accept verifier proof", gateID)
	}

	if strings.TrimSpace(criteriaJSON) == "" {
		return GateEvaluation{}, Event{}, false, fmt.Errorf(
			"gate %q is not defined in locked gate_set %q for issue %q",
			gateID,
			gateSet.GateSetID,
			issueID,
		)
	}

	payload := gateEvaluatedPayload{
		IssueID:      issueID,
		GateSetID:    gateSet.GateSetID,
		GateID:       gateID,
		Result:       result,
		EvidenceRefs: evidenceRefs,
		Proof:        proof,
		EvaluatedAt:  nowUTC(),
	}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return GateEvaluation{}, Event{}, false, fmt.Errorf("marshal payload: %w", err)
	}

	appendRes, err := s.appendEventTx(ctx, tx, appendEventRequest{
		EntityType:          entityTypeIssue,
		EntityID:            issueID,
		EventType:           eventTypeGateEval,
		PayloadJSON:         string(payloadBytes),
		Actor:               p.Actor,
		CommandID:           p.CommandID,
		CausationID:         gateSetEvent.EventID,
		CorrelationID:       gateCycleCorrelationID(issueID, gateSet.CycleNo),
		EventPayloadVersion: 1,
	})
	if err != nil {
		return GateEvaluation{}, Event{}, false, err
	}
	if appendRes.Event.EventType != eventTypeGateEval {
		return GateEvaluation{}, Event{}, false, fmt.Errorf("command id already used by %q", appendRes.Event.EventType)
	}

	if !appendRes.AlreadyExists {
		if err := applyGateEvaluatedProjectionTx(ctx, tx, appendRes.Event); err != nil {
			return GateEvaluation{}, Event{}, false, err
		}
	}

	if err := tx.Commit(); err != nil {
		return GateEvaluation{}, Event{}, false, fmt.Errorf("commit tx: %w", err)
	}

	return GateEvaluation{
		IssueID:      payload.IssueID,
		GateSetID:    payload.GateSetID,
		GateID:       payload.GateID,
		Result:       payload.Result,
		EvidenceRefs: payload.EvidenceRefs,
		Proof:        payload.Proof,
		EvaluatedAt:  payload.EvaluatedAt,
	}, appendRes.Event, appendRes.AlreadyExists, nil
}

func (s *Store) LookupGateEvaluationByCommand(ctx context.Context, actor, commandID string) (GateEvaluation, Event, bool, error) {
	actor = strings.TrimSpace(actor)
	if actor == "" {
		return GateEvaluation{}, Event{}, false, errors.New("--actor is required")
	}
	commandID = strings.TrimSpace(commandID)
	if commandID == "" {
		return GateEvaluation{}, Event{}, false, errors.New("--command-id is required")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return GateEvaluation{}, Event{}, false, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	existingEvent, found, err := findEventByActorCommandTx(ctx, tx, actor, commandID)
	if err != nil {
		return GateEvaluation{}, Event{}, false, err
	}
	if !found {
		if err := tx.Commit(); err != nil {
			return GateEvaluation{}, Event{}, false, fmt.Errorf("commit tx: %w", err)
		}
		return GateEvaluation{}, Event{}, false, nil
	}
	if existingEvent.EventType != eventTypeGateEval {
		return GateEvaluation{}, Event{}, false, fmt.Errorf("command id already used by %q", existingEvent.EventType)
	}

	payload, err := decodeGateEvaluatedPayload(existingEvent.PayloadJSON)
	if err != nil {
		return GateEvaluation{}, Event{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return GateEvaluation{}, Event{}, false, fmt.Errorf("commit tx: %w", err)
	}

	return GateEvaluation{
		IssueID:      payload.IssueID,
		GateSetID:    payload.GateSetID,
		GateID:       payload.GateID,
		Result:       payload.Result,
		EvidenceRefs: payload.EvidenceRefs,
		Proof:        payload.Proof,
		EvaluatedAt:  payload.EvaluatedAt,
	}, existingEvent, true, nil
}

func (s *Store) LookupGateVerificationSpec(ctx context.Context, issueID, gateID string) (GateVerificationSpec, error) {
	normalizedIssueID, err := normalizeIssueKey(issueID)
	if err != nil {
		return GateVerificationSpec{}, err
	}
	normalizedGateID := strings.TrimSpace(gateID)
	if normalizedGateID == "" {
		return GateVerificationSpec{}, errors.New("--gate is required")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return GateVerificationSpec{}, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	if _, err := getIssueTx(ctx, tx, normalizedIssueID); err != nil {
		return GateVerificationSpec{}, err
	}
	gateSet, found, err := lockedGateSetForIssueTx(ctx, tx, normalizedIssueID)
	if err != nil {
		return GateVerificationSpec{}, err
	}
	if !found {
		return GateVerificationSpec{}, fmt.Errorf("no locked gate set found for issue %q", normalizedIssueID)
	}
	fullGateSet, found, err := gateSetByIDTx(ctx, tx, gateSet.GateSetID)
	if err != nil {
		return GateVerificationSpec{}, err
	}
	if !found {
		return GateVerificationSpec{}, fmt.Errorf("gate set %q not found", gateSet.GateSetID)
	}

	var criteriaJSON string
	if err := tx.QueryRowContext(ctx, `
		SELECT criteria_json
		FROM gate_set_items
		WHERE gate_set_id = ? AND gate_id = ?
	`, gateSet.GateSetID, normalizedGateID).Scan(&criteriaJSON); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return GateVerificationSpec{}, fmt.Errorf("gate %q is not defined in locked gate_set %q for issue %q", normalizedGateID, gateSet.GateSetID, normalizedIssueID)
		}
		return GateVerificationSpec{}, fmt.Errorf("lookup verification criteria for gate %q: %w", normalizedGateID, err)
	}

	var criteria map[string]any
	if err := json.Unmarshal([]byte(criteriaJSON), &criteria); err != nil {
		return GateVerificationSpec{}, fmt.Errorf("decode criteria_json for gate %q: %w", normalizedGateID, err)
	}
	command, _ := criteria["command"].(string)
	command = strings.TrimSpace(command)
	if command == "" {
		return GateVerificationSpec{}, fmt.Errorf("gate %q has no executable verifier command in criteria.command", normalizedGateID)
	}
	if err := validateExecutableGateVerificationGovernanceTx(ctx, tx, fullGateSet, normalizedGateID, command); err != nil {
		return GateVerificationSpec{}, err
	}

	if err := tx.Commit(); err != nil {
		return GateVerificationSpec{}, fmt.Errorf("commit tx: %w", err)
	}
	return GateVerificationSpec{
		IssueID:     normalizedIssueID,
		GateSetID:   gateSet.GateSetID,
		GateSetHash: gateSet.GateSetHash,
		GateID:      normalizedGateID,
		Command:     command,
	}, nil
}

func (s *Store) GetGateStatus(ctx context.Context, issueID string) (GateStatus, error) {
	return s.GetGateStatusForCycle(ctx, GetGateStatusParams{IssueID: issueID})
}

func (s *Store) GetGateStatusForCycle(ctx context.Context, p GetGateStatusParams) (GateStatus, error) {
	normalizedIssueID, err := normalizeIssueKey(p.IssueID)
	if err != nil {
		return GateStatus{}, err
	}
	if p.CycleNo != nil && *p.CycleNo <= 0 {
		return GateStatus{}, errors.New("--cycle must be > 0")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return GateStatus{}, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	if _, err := getIssueTx(ctx, tx, normalizedIssueID); err != nil {
		return GateStatus{}, err
	}

	var (
		gateSet lockedGateSet
		found   bool
	)
	if p.CycleNo != nil {
		gateSet, found, err = lockedGateSetForIssueCycleTx(ctx, tx, normalizedIssueID, *p.CycleNo)
	} else {
		gateSet, found, err = lockedGateSetForIssueTx(ctx, tx, normalizedIssueID)
	}
	if err != nil {
		return GateStatus{}, err
	}
	if !found {
		if p.CycleNo != nil {
			return GateStatus{}, fmt.Errorf(
				"no locked gate set found for issue %q cycle %d",
				normalizedIssueID,
				*p.CycleNo,
			)
		}
		return GateStatus{}, fmt.Errorf("no locked gate set found for issue %q", normalizedIssueID)
	}

	rows, err := tx.QueryContext(ctx, `
		SELECT
			i.gate_id,
			i.kind,
			i.required,
			COALESCE(gs.result, ''),
			COALESCE(gs.evidence_refs_json, '[]'),
			COALESCE(gs.evaluated_at, ''),
			COALESCE(gs.last_event_id, '')
		FROM gate_set_items i
		LEFT JOIN gate_status_projection gs
			ON gs.issue_id = ?
			AND gs.gate_set_id = i.gate_set_id
			AND gs.gate_id = i.gate_id
		WHERE i.gate_set_id = ?
		ORDER BY i.gate_id ASC
	`, normalizedIssueID, gateSet.GateSetID)
	if err != nil {
		return GateStatus{}, fmt.Errorf("query gate status for issue %q: %w", normalizedIssueID, err)
	}
	defer rows.Close()

	gates := make([]GateStatusItem, 0)
	for rows.Next() {
		var (
			item         GateStatusItem
			requiredInt  int
			rawResult    string
			evidenceJSON string
		)
		if err := rows.Scan(
			&item.GateID,
			&item.Kind,
			&requiredInt,
			&rawResult,
			&evidenceJSON,
			&item.EvaluatedAt,
			&item.LastEventID,
		); err != nil {
			return GateStatus{}, fmt.Errorf("scan gate status row for issue %q: %w", normalizedIssueID, err)
		}
		item.Required = requiredInt == 1
		if strings.TrimSpace(rawResult) == "" {
			item.Result = "MISSING"
		} else if normalizedResult, err := normalizeGateResult(rawResult); err == nil {
			item.Result = normalizedResult
		} else {
			item.Result = strings.ToUpper(strings.TrimSpace(rawResult))
		}
		evidenceRefs, err := parseReferencesJSON(evidenceJSON)
		if err != nil {
			return GateStatus{}, fmt.Errorf("decode gate status evidence for issue %q: %w", normalizedIssueID, err)
		}
		item.EvidenceRefs = evidenceRefs
		gates = append(gates, item)
	}
	if err := rows.Err(); err != nil {
		return GateStatus{}, fmt.Errorf("iterate gate status rows for issue %q: %w", normalizedIssueID, err)
	}

	if err := tx.Commit(); err != nil {
		return GateStatus{}, fmt.Errorf("commit tx: %w", err)
	}

	return GateStatus{
		IssueID:   normalizedIssueID,
		GateSetID: gateSet.GateSetID,
		CycleNo:   gateSet.CycleNo,
		LockedAt:  gateSet.LockedAt,
		Gates:     gates,
	}, nil
}
