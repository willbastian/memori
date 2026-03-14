package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

func (s *Store) CreateGateTemplate(ctx context.Context, p CreateGateTemplateParams) (GateTemplate, bool, error) {
	if p.Actor == "" {
		p.Actor = defaultActor()
	}
	if strings.TrimSpace(p.CommandID) == "" {
		return GateTemplate{}, false, errors.New("--command-id is required")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return GateTemplate{}, false, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	if existingEvent, found, err := findEventByActorCommandTx(ctx, tx, p.Actor, p.CommandID); err != nil {
		return GateTemplate{}, false, err
	} else if found {
		if existingEvent.EventType != eventTypeGateTemplateCreate {
			return GateTemplate{}, false, fmt.Errorf("command id already used by %q", existingEvent.EventType)
		}
		payload, err := decodeGateTemplateCreatedPayload(existingEvent.PayloadJSON)
		if err != nil {
			return GateTemplate{}, false, err
		}
		template, found, err := gateTemplateByIDVersionTx(ctx, tx, payload.TemplateID, payload.Version)
		if err != nil {
			return GateTemplate{}, false, err
		}
		if !found {
			if err := applyGateTemplateCreatedProjectionTx(ctx, tx, existingEvent); err != nil {
				return GateTemplate{}, false, err
			}
			template, found, err = gateTemplateByIDVersionTx(ctx, tx, payload.TemplateID, payload.Version)
			if err != nil {
				return GateTemplate{}, false, err
			}
			if !found {
				return GateTemplate{}, false, fmt.Errorf("gate template %s@%d not found after replaying event %s", payload.TemplateID, payload.Version, existingEvent.EventID)
			}
		}
		if err := tx.Commit(); err != nil {
			return GateTemplate{}, false, fmt.Errorf("commit tx: %w", err)
		}
		return template, true, nil
	}

	templateID, err := normalizeGateTemplateID(p.TemplateID)
	if err != nil {
		return GateTemplate{}, false, err
	}
	if p.Version <= 0 {
		return GateTemplate{}, false, errors.New("--version must be > 0")
	}
	appliesTo, err := normalizeGateAppliesTo(p.AppliesTo)
	if err != nil {
		return GateTemplate{}, false, err
	}
	definitionJSON, definitionHash, err := canonicalizeGateDefinition(p.DefinitionJSON)
	if err != nil {
		return GateTemplate{}, false, err
	}

	var (
		existingAppliesToJSON string
		existingDefinition    string
		existingHash          string
	)
	err = tx.QueryRowContext(ctx, `
		SELECT applies_to_json, definition_json, definition_hash
		FROM gate_templates
		WHERE template_id = ? AND version = ?
	`, templateID, p.Version).Scan(
		&existingAppliesToJSON,
		&existingDefinition,
		&existingHash,
	)
	if err == nil {
		existingAppliesTo, parseErr := parseAppliesToJSON(existingAppliesToJSON)
		if parseErr != nil {
			return GateTemplate{}, false, parseErr
		}
		if existingHash == definitionHash &&
			existingDefinition == definitionJSON &&
			equalStringSlices(existingAppliesTo, appliesTo) {
			template, found, err := gateTemplateByIDVersionTx(ctx, tx, templateID, p.Version)
			if err != nil {
				return GateTemplate{}, false, err
			}
			if !found {
				return GateTemplate{}, false, fmt.Errorf("gate template %s@%d not found after idempotent lookup", templateID, p.Version)
			}
			if err := tx.Commit(); err != nil {
				return GateTemplate{}, false, fmt.Errorf("commit tx: %w", err)
			}
			return template, true, nil
		}
		return GateTemplate{}, false, fmt.Errorf("template %s@%d already exists (create a new version to change it)", templateID, p.Version)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return GateTemplate{}, false, fmt.Errorf("query gate template %s@%d: %w", templateID, p.Version, err)
	}

	payload := gateTemplateCreatedPayload{
		TemplateID:     templateID,
		Version:        p.Version,
		AppliesTo:      appliesTo,
		DefinitionJSON: definitionJSON,
		DefinitionHash: definitionHash,
		CreatedAt:      nowUTC(),
		CreatedBy:      p.Actor,
	}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return GateTemplate{}, false, fmt.Errorf("marshal payload: %w", err)
	}

	appendRes, err := s.appendEventTx(ctx, tx, appendEventRequest{
		EntityType:          entityTypeGateTemplate,
		EntityID:            gateTemplateEntityID(templateID, p.Version),
		EventType:           eventTypeGateTemplateCreate,
		PayloadJSON:         string(payloadBytes),
		Actor:               p.Actor,
		CommandID:           p.CommandID,
		CorrelationID:       gateTemplateCorrelationID(templateID, p.Version),
		EventPayloadVersion: 1,
	})
	if err != nil {
		return GateTemplate{}, false, err
	}
	if appendRes.Event.EventType != eventTypeGateTemplateCreate {
		return GateTemplate{}, false, fmt.Errorf("command id already used by %q", appendRes.Event.EventType)
	}

	if !appendRes.AlreadyExists {
		if err := applyGateTemplateCreatedProjectionTx(ctx, tx, appendRes.Event); err != nil {
			return GateTemplate{}, false, err
		}
	}

	template, found, err := gateTemplateByIDVersionTx(ctx, tx, templateID, p.Version)
	if err != nil {
		return GateTemplate{}, false, err
	}
	if !found {
		return GateTemplate{}, false, fmt.Errorf("gate template %s@%d not found after projection", templateID, p.Version)
	}

	if err := tx.Commit(); err != nil {
		return GateTemplate{}, false, fmt.Errorf("commit tx: %w", err)
	}

	return template, appendRes.AlreadyExists, nil
}

func (s *Store) ApproveGateTemplate(ctx context.Context, p ApproveGateTemplateParams) (GateTemplate, bool, error) {
	if p.Actor == "" {
		p.Actor = defaultActor()
	}
	if strings.TrimSpace(p.CommandID) == "" {
		return GateTemplate{}, false, errors.New("--command-id is required")
	}
	if !actorIsHumanGoverned(p.Actor) {
		return GateTemplate{}, false, errors.New("executable gate template approval requires a human-governed actor")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return GateTemplate{}, false, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	if existingEvent, found, err := findEventByActorCommandTx(ctx, tx, p.Actor, p.CommandID); err != nil {
		return GateTemplate{}, false, err
	} else if found {
		if existingEvent.EventType != eventTypeGateTemplateApprove {
			return GateTemplate{}, false, fmt.Errorf("command id already used by %q", existingEvent.EventType)
		}
		payload, err := decodeGateTemplateApprovedPayload(existingEvent.PayloadJSON)
		if err != nil {
			return GateTemplate{}, false, err
		}
		template, found, err := gateTemplateByIDVersionTx(ctx, tx, payload.TemplateID, payload.Version)
		if err != nil {
			return GateTemplate{}, false, err
		}
		if !found || !actorIsHumanGoverned(template.ApprovedBy) {
			if err := applyGateTemplateApprovedProjectionTx(ctx, tx, existingEvent); err != nil {
				return GateTemplate{}, false, err
			}
			template, found, err = gateTemplateByIDVersionTx(ctx, tx, payload.TemplateID, payload.Version)
			if err != nil {
				return GateTemplate{}, false, err
			}
			if !found {
				return GateTemplate{}, false, fmt.Errorf("gate template %s@%d not found after replaying event %s", payload.TemplateID, payload.Version, existingEvent.EventID)
			}
		}
		if err := tx.Commit(); err != nil {
			return GateTemplate{}, false, fmt.Errorf("commit tx: %w", err)
		}
		return template, true, nil
	}

	templateID, err := normalizeGateTemplateID(p.TemplateID)
	if err != nil {
		return GateTemplate{}, false, err
	}
	if p.Version <= 0 {
		return GateTemplate{}, false, errors.New("--version must be > 0")
	}

	template, found, err := gateTemplateByIDVersionTx(ctx, tx, templateID, p.Version)
	if err != nil {
		return GateTemplate{}, false, err
	}
	if !found {
		return GateTemplate{}, false, fmt.Errorf("gate template %s@%d not found", templateID, p.Version)
	}
	if !template.Executable {
		return GateTemplate{}, false, fmt.Errorf("gate template %s@%d has no executable criteria.command and does not require approval", templateID, p.Version)
	}
	if actorIsHumanGoverned(template.ApprovedBy) {
		if err := tx.Commit(); err != nil {
			return GateTemplate{}, false, fmt.Errorf("commit tx: %w", err)
		}
		return template, true, nil
	}

	payload := gateTemplateApprovedPayload{
		TemplateID:     templateID,
		Version:        p.Version,
		DefinitionHash: template.DefinitionHash,
		ApprovedAt:     nowUTC(),
		ApprovedBy:     p.Actor,
	}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return GateTemplate{}, false, fmt.Errorf("marshal payload: %w", err)
	}

	appendRes, err := s.appendEventTx(ctx, tx, appendEventRequest{
		EntityType:          entityTypeGateTemplate,
		EntityID:            gateTemplateEntityID(templateID, p.Version),
		EventType:           eventTypeGateTemplateApprove,
		PayloadJSON:         string(payloadBytes),
		Actor:               p.Actor,
		CommandID:           p.CommandID,
		CorrelationID:       gateTemplateCorrelationID(templateID, p.Version),
		EventPayloadVersion: 1,
	})
	if err != nil {
		return GateTemplate{}, false, err
	}
	if appendRes.Event.EventType != eventTypeGateTemplateApprove {
		return GateTemplate{}, false, fmt.Errorf("command id already used by %q", appendRes.Event.EventType)
	}

	if !appendRes.AlreadyExists {
		if err := applyGateTemplateApprovedProjectionTx(ctx, tx, appendRes.Event); err != nil {
			return GateTemplate{}, false, err
		}
	}

	template, found, err = gateTemplateByIDVersionTx(ctx, tx, templateID, p.Version)
	if err != nil {
		return GateTemplate{}, false, err
	}
	if !found {
		return GateTemplate{}, false, fmt.Errorf("gate template %s@%d not found after approval", templateID, p.Version)
	}

	if err := tx.Commit(); err != nil {
		return GateTemplate{}, false, fmt.Errorf("commit tx: %w", err)
	}
	return template, appendRes.AlreadyExists, nil
}

func (s *Store) ListGateTemplates(ctx context.Context, p ListGateTemplatesParams) ([]GateTemplate, error) {
	var issueTypeFilter string
	if strings.TrimSpace(p.IssueType) != "" {
		normalizedType, err := normalizeIssueType(p.IssueType)
		if err != nil {
			return nil, err
		}
		issueTypeFilter = normalizedType
	}

	rows, err := s.db.QueryContext(ctx, `
		SELECT t.template_id, t.version, t.applies_to_json, t.definition_json, t.definition_hash,
			COALESCE(a.approved_at, ''), COALESCE(a.approved_by, ''), t.created_at, t.created_by
		FROM gate_templates
		AS t
		LEFT JOIN gate_template_approvals AS a
			ON a.template_id = t.template_id
			AND a.version = t.version
		ORDER BY t.template_id ASC, t.version ASC
	`)
	if err != nil {
		return nil, fmt.Errorf("query gate_templates: %w", err)
	}
	defer rows.Close()

	templates := make([]GateTemplate, 0)
	for rows.Next() {
		var (
			template      GateTemplate
			appliesToJSON string
		)
		if err := rows.Scan(
			&template.TemplateID,
			&template.Version,
			&appliesToJSON,
			&template.DefinitionJSON,
			&template.DefinitionHash,
			&template.ApprovedAt,
			&template.ApprovedBy,
			&template.CreatedAt,
			&template.CreatedBy,
		); err != nil {
			return nil, fmt.Errorf("scan gate template: %w", err)
		}
		appliesTo, err := parseAppliesToJSON(appliesToJSON)
		if err != nil {
			return nil, err
		}
		template.AppliesTo = appliesTo
		if issueTypeFilter != "" && !stringSliceContains(appliesTo, issueTypeFilter) {
			continue
		}
		template.Executable = gateDefinitionContainsExecutableCommand(template.DefinitionJSON)
		templates = append(templates, template)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate gate_templates rows: %w", err)
	}
	return templates, nil
}

func (s *Store) ListPendingExecutableGateTemplates(ctx context.Context) ([]GateTemplate, error) {
	templates, err := s.ListGateTemplates(ctx, ListGateTemplatesParams{})
	if err != nil {
		return nil, err
	}

	pending := make([]GateTemplate, 0, len(templates))
	for _, template := range templates {
		if !template.Executable {
			continue
		}
		if actorIsHumanGoverned(template.ApprovedBy) {
			continue
		}
		pending = append(pending, template)
	}
	return pending, nil
}

func (s *Store) InstantiateGateSet(ctx context.Context, p InstantiateGateSetParams) (GateSet, bool, error) {
	if p.Actor == "" {
		p.Actor = defaultActor()
	}
	if strings.TrimSpace(p.CommandID) == "" {
		return GateSet{}, false, errors.New("--command-id is required")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return GateSet{}, false, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	if existingEvent, found, err := findEventByActorCommandTx(ctx, tx, p.Actor, p.CommandID); err != nil {
		return GateSet{}, false, err
	} else if found {
		if existingEvent.EventType != eventTypeGateSetCreate {
			return GateSet{}, false, fmt.Errorf("command id already used by %q", existingEvent.EventType)
		}
		payload, err := decodeGateSetInstantiatedPayload(existingEvent.PayloadJSON)
		if err != nil {
			return GateSet{}, false, err
		}
		gateSet, found, err := gateSetByIDTx(ctx, tx, payload.GateSetID)
		if err != nil {
			return GateSet{}, false, err
		}
		if !found {
			if err := applyGateSetInstantiatedProjectionTx(ctx, tx, existingEvent); err != nil {
				return GateSet{}, false, err
			}
			gateSet, found, err = gateSetByIDTx(ctx, tx, payload.GateSetID)
			if err != nil {
				return GateSet{}, false, err
			}
			if !found {
				return GateSet{}, false, fmt.Errorf("gate set %q not found after replaying event %s", payload.GateSetID, existingEvent.EventID)
			}
		}
		if err := tx.Commit(); err != nil {
			return GateSet{}, false, fmt.Errorf("commit tx: %w", err)
		}
		return gateSet, true, nil
	}

	issueID, err := normalizeIssueKey(p.IssueID)
	if err != nil {
		return GateSet{}, false, err
	}
	templateRefs, parsedRefs, err := normalizeGateTemplateRefs(p.TemplateRefs)
	if err != nil {
		return GateSet{}, false, err
	}

	issue, err := getIssueTx(ctx, tx, issueID)
	if err != nil {
		return GateSet{}, false, err
	}

	var cycleNo int
	if err := tx.QueryRowContext(ctx, `SELECT current_cycle_no FROM work_items WHERE id = ?`, issueID).Scan(&cycleNo); err != nil {
		return GateSet{}, false, fmt.Errorf("read current cycle for issue %q: %w", issueID, err)
	}

	existing, found, err := gateSetForIssueCycleTx(ctx, tx, issueID, cycleNo)
	if err != nil {
		return GateSet{}, false, err
	}
	if found {
		existingRefs := normalizeReferences(existing.TemplateRefs)
		if equalStringSlices(existingRefs, templateRefs) {
			if err := tx.Commit(); err != nil {
				return GateSet{}, false, fmt.Errorf("commit tx: %w", err)
			}
			return existing, true, nil
		}
		return GateSet{}, false, fmt.Errorf(
			"gate set already exists for issue %q cycle %d (existing gate_set_id %q)",
			issueID,
			cycleNo,
			existing.GateSetID,
		)
	}

	gates, err := buildGateSetDefinitionsTx(ctx, tx, issue.Type, parsedRefs)
	if err != nil {
		return GateSet{}, false, err
	}
	frozenJSON, _, err := buildFrozenGateDefinition(templateRefs, gates)
	if err != nil {
		return GateSet{}, false, err
	}
	hash := sha256.Sum256([]byte(frozenJSON))
	gateSetHash := hex.EncodeToString(hash[:])
	gateSetID := newID("gset")
	createdAt := nowUTC()

	var frozenObjCopy map[string]any
	if err := json.Unmarshal([]byte(frozenJSON), &frozenObjCopy); err != nil {
		return GateSet{}, false, fmt.Errorf("decode frozen gate definition: %w", err)
	}
	payload := gateSetInstantiatedPayload{
		GateSetID:        gateSetID,
		IssueID:          issueID,
		CycleNo:          cycleNo,
		TemplateRefs:     templateRefs,
		FrozenDefinition: frozenObjCopy,
		GateSetHash:      gateSetHash,
		CreatedAt:        createdAt,
		CreatedBy:        p.Actor,
		Items:            gates,
	}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return GateSet{}, false, fmt.Errorf("marshal payload: %w", err)
	}

	appendRes, err := s.appendEventTx(ctx, tx, appendEventRequest{
		EntityType:          entityTypeGateSet,
		EntityID:            gateSetID,
		EventType:           eventTypeGateSetCreate,
		PayloadJSON:         string(payloadBytes),
		Actor:               p.Actor,
		CommandID:           p.CommandID,
		CorrelationID:       gateCycleCorrelationID(issueID, cycleNo),
		EventPayloadVersion: 1,
	})
	if err != nil {
		return GateSet{}, false, err
	}
	if appendRes.Event.EventType != eventTypeGateSetCreate {
		return GateSet{}, false, fmt.Errorf("command id already used by %q", appendRes.Event.EventType)
	}
	if !appendRes.AlreadyExists {
		if err := applyGateSetInstantiatedProjectionTx(ctx, tx, appendRes.Event); err != nil {
			return GateSet{}, false, err
		}
	}

	gateSet, found, err := gateSetByIDTx(ctx, tx, gateSetID)
	if err != nil {
		return GateSet{}, false, err
	}
	if !found {
		return GateSet{}, false, fmt.Errorf("gate set %q not found after projection", gateSetID)
	}

	if err := tx.Commit(); err != nil {
		return GateSet{}, false, fmt.Errorf("commit tx: %w", err)
	}

	return gateSet, appendRes.AlreadyExists, nil
}

func (s *Store) LockGateSet(ctx context.Context, p LockGateSetParams) (GateSet, bool, error) {
	if p.Actor == "" {
		p.Actor = defaultActor()
	}
	if strings.TrimSpace(p.CommandID) == "" {
		return GateSet{}, false, errors.New("--command-id is required")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return GateSet{}, false, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	if existingEvent, found, err := findEventByActorCommandTx(ctx, tx, p.Actor, p.CommandID); err != nil {
		return GateSet{}, false, err
	} else if found {
		if existingEvent.EventType != eventTypeGateSetLock {
			return GateSet{}, false, fmt.Errorf("command id already used by %q", existingEvent.EventType)
		}
		payload, err := decodeGateSetLockedPayload(existingEvent.PayloadJSON)
		if err != nil {
			return GateSet{}, false, err
		}
		gateSet, found, err := gateSetByIDTx(ctx, tx, payload.GateSetID)
		if err != nil {
			return GateSet{}, false, err
		}
		if !found {
			if err := applyGateSetLockedProjectionTx(ctx, tx, existingEvent); err != nil {
				return GateSet{}, false, err
			}
			gateSet, found, err = gateSetByIDTx(ctx, tx, payload.GateSetID)
			if err != nil {
				return GateSet{}, false, err
			}
			if !found {
				return GateSet{}, false, fmt.Errorf("gate set %q not found after replaying event %s", payload.GateSetID, existingEvent.EventID)
			}
		}
		if err := tx.Commit(); err != nil {
			return GateSet{}, false, fmt.Errorf("commit tx: %w", err)
		}
		return gateSet, false, nil
	}

	issueID, err := normalizeIssueKey(p.IssueID)
	if err != nil {
		return GateSet{}, false, err
	}

	if _, err := getIssueTx(ctx, tx, issueID); err != nil {
		return GateSet{}, false, err
	}

	cycleNo := 0
	if p.CycleNo != nil {
		if *p.CycleNo <= 0 {
			return GateSet{}, false, errors.New("--cycle must be > 0")
		}
		cycleNo = *p.CycleNo
	} else {
		if err := tx.QueryRowContext(ctx, `SELECT current_cycle_no FROM work_items WHERE id = ?`, issueID).Scan(&cycleNo); err != nil {
			return GateSet{}, false, fmt.Errorf("read current cycle for issue %q: %w", issueID, err)
		}
	}

	gateSet, found, err := gateSetForIssueCycleTx(ctx, tx, issueID, cycleNo)
	if err != nil {
		return GateSet{}, false, err
	}
	if !found {
		return GateSet{}, false, fmt.Errorf("no gate set found for issue %q cycle %d", issueID, cycleNo)
	}
	if len(gateSet.Items) == 0 {
		return GateSet{}, false, fmt.Errorf("cannot lock gate set %q: no gate items defined", gateSet.GateSetID)
	}

	lockedNow := false
	lockTime := strings.TrimSpace(gateSet.LockedAt)
	if lockTime == "" {
		lockTime = nowUTC()
		payloadBytes, err := json.Marshal(gateSetLockedPayload{
			GateSetID: gateSet.GateSetID,
			IssueID:   issueID,
			CycleNo:   cycleNo,
			LockedAt:  lockTime,
		})
		if err != nil {
			return GateSet{}, false, fmt.Errorf("marshal payload: %w", err)
		}
		appendRes, err := s.appendEventTx(ctx, tx, appendEventRequest{
			EntityType:          entityTypeGateSet,
			EntityID:            gateSet.GateSetID,
			EventType:           eventTypeGateSetLock,
			PayloadJSON:         string(payloadBytes),
			Actor:               p.Actor,
			CommandID:           p.CommandID,
			CorrelationID:       gateCycleCorrelationID(issueID, cycleNo),
			EventPayloadVersion: 1,
		})
		if err != nil {
			return GateSet{}, false, err
		}
		if appendRes.Event.EventType != eventTypeGateSetLock {
			return GateSet{}, false, fmt.Errorf("command id already used by %q", appendRes.Event.EventType)
		}
		if !appendRes.AlreadyExists {
			if err := applyGateSetLockedProjectionTx(ctx, tx, appendRes.Event); err != nil {
				return GateSet{}, false, err
			}
		}
		lockedNow = true
	} else {
		if _, err := tx.ExecContext(ctx, `
			UPDATE work_items
			SET active_gate_set_id = ?, updated_at = ?
			WHERE id = ?
		`, gateSet.GateSetID, nowUTC(), issueID); err != nil {
			return GateSet{}, false, fmt.Errorf("set active gate set for issue %q: %w", issueID, err)
		}
	}

	gateSet.LockedAt = lockTime
	if err := tx.Commit(); err != nil {
		return GateSet{}, false, fmt.Errorf("commit tx: %w", err)
	}
	return gateSet, lockedNow, nil
}
