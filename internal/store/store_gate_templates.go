package store

import (
	"context"
	"database/sql"
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
		template, err := replayCreatedGateTemplateTx(ctx, tx, existingEvent, payload.TemplateID, payload.Version)
		if err != nil {
			return GateTemplate{}, false, err
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
		template, err := replayApprovedGateTemplateTx(ctx, tx, existingEvent, payload.TemplateID, payload.Version)
		if err != nil {
			return GateTemplate{}, false, err
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

func replayCreatedGateTemplateTx(ctx context.Context, tx *sql.Tx, existingEvent Event, templateID string, version int) (GateTemplate, error) {
	template, found, err := gateTemplateByIDVersionTx(ctx, tx, templateID, version)
	if err != nil {
		return GateTemplate{}, err
	}
	if !found {
		if err := applyGateTemplateCreatedProjectionTx(ctx, tx, existingEvent); err != nil {
			return GateTemplate{}, err
		}
		template, found, err = gateTemplateByIDVersionTx(ctx, tx, templateID, version)
		if err != nil {
			return GateTemplate{}, err
		}
		if !found {
			return GateTemplate{}, fmt.Errorf("gate template %s@%d not found after replaying event %s", templateID, version, existingEvent.EventID)
		}
	}
	return template, nil
}

func replayApprovedGateTemplateTx(ctx context.Context, tx *sql.Tx, existingEvent Event, templateID string, version int) (GateTemplate, error) {
	template, found, err := gateTemplateByIDVersionTx(ctx, tx, templateID, version)
	if err != nil {
		return GateTemplate{}, err
	}
	if found && actorIsHumanGoverned(template.ApprovedBy) {
		return template, nil
	}
	if err := applyGateTemplateApprovedProjectionTx(ctx, tx, existingEvent); err != nil {
		return GateTemplate{}, err
	}
	template, found, err = gateTemplateByIDVersionTx(ctx, tx, templateID, version)
	if err != nil {
		return GateTemplate{}, err
	}
	if !found {
		return GateTemplate{}, fmt.Errorf("gate template %s@%d not found after replaying event %s", templateID, version, existingEvent.EventID)
	}
	return template, nil
}
