package store

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

func normalizeGateResult(raw string) (string, error) {
	switch strings.ToUpper(strings.TrimSpace(raw)) {
	case "PASS":
		return "PASS", nil
	case "FAIL":
		return "FAIL", nil
	case "BLOCKED":
		return "BLOCKED", nil
	default:
		return "", fmt.Errorf("invalid --result %q (expected PASS|FAIL|BLOCKED)", raw)
	}
}

func decodeGateEvaluatedPayload(payloadJSON string) (gateEvaluatedPayload, error) {
	var payload gateEvaluatedPayload
	if err := decodeGatePayload(payloadJSON, &payload); err != nil {
		return gateEvaluatedPayload{}, err
	}

	issueID, err := normalizeIssueKey(payload.IssueID)
	if err != nil {
		return gateEvaluatedPayload{}, fmt.Errorf("invalid issue_id: %w", err)
	}
	payload.IssueID = issueID

	payload.GateSetID = strings.TrimSpace(payload.GateSetID)
	if payload.GateSetID == "" {
		return gateEvaluatedPayload{}, errors.New("gate_set_id is required")
	}
	payload.GateID = strings.TrimSpace(payload.GateID)
	if payload.GateID == "" {
		return gateEvaluatedPayload{}, errors.New("gate_id is required")
	}
	result, err := normalizeGateResult(payload.Result)
	if err != nil {
		return gateEvaluatedPayload{}, err
	}
	payload.Result = result
	payload.EvidenceRefs = normalizeReferences(payload.EvidenceRefs)
	payload.EvaluatedAt = strings.TrimSpace(payload.EvaluatedAt)

	return payload, nil
}

func decodeGateTemplateCreatedPayload(payloadJSON string) (gateTemplateCreatedPayload, error) {
	var payload gateTemplateCreatedPayload
	if err := decodeGatePayload(payloadJSON, &payload); err != nil {
		return gateTemplateCreatedPayload{}, err
	}
	return normalizeGateTemplateCreatedPayload(payload)
}

func decodeGateTemplateApprovedPayload(payloadJSON string) (gateTemplateApprovedPayload, error) {
	var payload gateTemplateApprovedPayload
	if err := decodeGatePayload(payloadJSON, &payload); err != nil {
		return gateTemplateApprovedPayload{}, err
	}
	return normalizeGateTemplateApprovedPayload(payload)
}

func decodeGateSetInstantiatedPayload(payloadJSON string) (gateSetInstantiatedPayload, error) {
	var payload gateSetInstantiatedPayload
	if err := decodeGatePayload(payloadJSON, &payload); err != nil {
		return gateSetInstantiatedPayload{}, err
	}
	return normalizeGateSetInstantiatedPayload(payload)
}

func decodeGateSetLockedPayload(payloadJSON string) (gateSetLockedPayload, error) {
	var payload gateSetLockedPayload
	if err := decodeGatePayload(payloadJSON, &payload); err != nil {
		return gateSetLockedPayload{}, err
	}
	return normalizeGateSetLockedPayload(payload)
}

func decodeGatePayload(payloadJSON string, dst any) error {
	return json.Unmarshal([]byte(payloadJSON), dst)
}

func normalizeGateTemplateCreatedPayload(payload gateTemplateCreatedPayload) (gateTemplateCreatedPayload, error) {
	templateID, err := normalizeGateTemplateID(payload.TemplateID)
	if err != nil {
		return gateTemplateCreatedPayload{}, fmt.Errorf("invalid template_id: %w", err)
	}
	if err := validateGateTemplateVersion(payload.Version); err != nil {
		return gateTemplateCreatedPayload{}, err
	}
	appliesTo, err := normalizeGateAppliesTo(payload.AppliesTo)
	if err != nil {
		return gateTemplateCreatedPayload{}, err
	}
	definitionJSON, definitionHash, err := canonicalizeGateDefinition(payload.DefinitionJSON)
	if err != nil {
		return gateTemplateCreatedPayload{}, err
	}
	if payload.DefinitionHash != "" && payload.DefinitionHash != definitionHash {
		return gateTemplateCreatedPayload{}, errors.New("definition_hash does not match definition_json")
	}

	payload.TemplateID = templateID
	payload.AppliesTo = appliesTo
	payload.DefinitionJSON = definitionJSON
	payload.DefinitionHash = definitionHash
	payload.CreatedAt, err = requireGatePayloadField(payload.CreatedAt, "created_at")
	if err != nil {
		return gateTemplateCreatedPayload{}, err
	}
	payload.CreatedBy, err = requireGatePayloadField(payload.CreatedBy, "created_by")
	if err != nil {
		return gateTemplateCreatedPayload{}, err
	}
	return payload, nil
}

func normalizeGateTemplateApprovedPayload(payload gateTemplateApprovedPayload) (gateTemplateApprovedPayload, error) {
	templateID, err := normalizeGateTemplateID(payload.TemplateID)
	if err != nil {
		return gateTemplateApprovedPayload{}, fmt.Errorf("invalid template_id: %w", err)
	}
	if err := validateGateTemplateVersion(payload.Version); err != nil {
		return gateTemplateApprovedPayload{}, err
	}

	payload.TemplateID = templateID
	payload.DefinitionHash, err = requireGatePayloadField(payload.DefinitionHash, "definition_hash")
	if err != nil {
		return gateTemplateApprovedPayload{}, err
	}
	payload.ApprovedAt, err = requireGatePayloadField(payload.ApprovedAt, "approved_at")
	if err != nil {
		return gateTemplateApprovedPayload{}, err
	}
	payload.ApprovedBy = strings.TrimSpace(payload.ApprovedBy)
	if !actorIsHumanGoverned(payload.ApprovedBy) {
		return gateTemplateApprovedPayload{}, errors.New("approved_by must be human-governed")
	}
	return payload, nil
}

func normalizeGateSetInstantiatedPayload(payload gateSetInstantiatedPayload) (gateSetInstantiatedPayload, error) {
	issueID, err := normalizeIssueKey(payload.IssueID)
	if err != nil {
		return gateSetInstantiatedPayload{}, fmt.Errorf("invalid issue_id: %w", err)
	}
	if payload.CycleNo <= 0 {
		return gateSetInstantiatedPayload{}, errors.New("cycle_no must be > 0")
	}
	templateRefs, _, err := normalizeGateTemplateRefs(payload.TemplateRefs)
	if err != nil {
		return gateSetInstantiatedPayload{}, err
	}
	payload.GateSetID, err = requireGatePayloadField(payload.GateSetID, "gate_set_id")
	if err != nil {
		return gateSetInstantiatedPayload{}, err
	}
	payload.GateSetHash, err = requireGatePayloadField(payload.GateSetHash, "gate_set_hash")
	if err != nil {
		return gateSetInstantiatedPayload{}, err
	}
	payload.CreatedAt, err = requireGatePayloadField(payload.CreatedAt, "created_at")
	if err != nil {
		return gateSetInstantiatedPayload{}, err
	}
	payload.CreatedBy, err = requireGatePayloadField(payload.CreatedBy, "created_by")
	if err != nil {
		return gateSetInstantiatedPayload{}, err
	}

	frozenJSON, frozenObj, err := buildFrozenGateDefinition(templateRefs, payload.Items)
	if err != nil {
		return gateSetInstantiatedPayload{}, err
	}
	hash := sha256.Sum256([]byte(frozenJSON))
	if payload.GateSetHash != hex.EncodeToString(hash[:]) {
		return gateSetInstantiatedPayload{}, errors.New("gate_set_hash does not match frozen definition")
	}

	payload.IssueID = issueID
	payload.TemplateRefs = templateRefs
	payload.FrozenDefinition = frozenObj
	return payload, nil
}

func normalizeGateSetLockedPayload(payload gateSetLockedPayload) (gateSetLockedPayload, error) {
	issueID, err := normalizeIssueKey(payload.IssueID)
	if err != nil {
		return gateSetLockedPayload{}, fmt.Errorf("invalid issue_id: %w", err)
	}
	payload.GateSetID, err = requireGatePayloadField(payload.GateSetID, "gate_set_id")
	if err != nil {
		return gateSetLockedPayload{}, err
	}
	if payload.CycleNo <= 0 {
		return gateSetLockedPayload{}, errors.New("cycle_no must be > 0")
	}
	payload.IssueID = issueID
	payload.LockedAt, err = requireGatePayloadField(payload.LockedAt, "locked_at")
	if err != nil {
		return gateSetLockedPayload{}, err
	}
	return payload, nil
}

func requireGatePayloadField(value, field string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", fmt.Errorf("%s is required", field)
	}
	return value, nil
}

func validateGateTemplateVersion(version int) error {
	if version <= 0 {
		return errors.New("version must be > 0")
	}
	return nil
}
