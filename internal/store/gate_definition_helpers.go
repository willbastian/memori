package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

type gateTemplateRef struct {
	TemplateID string
	Version    int
	Ref        string
}

func gateTemplateEntityID(templateID string, version int) string {
	return fmt.Sprintf("%s@%d", templateID, version)
}

func normalizeGateTemplateID(raw string) (string, error) {
	templateID := strings.ToLower(strings.TrimSpace(raw))
	if templateID == "" {
		return "", errors.New("--id is required")
	}
	if len(templateID) < 2 || len(templateID) > 64 {
		return "", fmt.Errorf("invalid template id %q (must be 2-64 chars)", raw)
	}
	for i, r := range templateID {
		if i == 0 && (r < 'a' || r > 'z') {
			return "", fmt.Errorf("invalid template id %q (must start with a lowercase letter)", raw)
		}
		if (r < 'a' || r > 'z') && (r < '0' || r > '9') && r != '-' && r != '_' {
			return "", fmt.Errorf("invalid template id %q (allowed: lowercase letters, digits, -, _)", raw)
		}
	}
	return templateID, nil
}

func normalizeGateAppliesTo(values []string) ([]string, error) {
	if len(values) == 0 {
		return nil, errors.New("--applies-to is required")
	}
	normalized := make([]string, 0, len(values))
	seen := make(map[string]bool, len(values))
	for _, value := range values {
		issueType, err := normalizeIssueType(value)
		if err != nil {
			return nil, fmt.Errorf("invalid --applies-to %q: %w", value, err)
		}
		if seen[issueType] {
			continue
		}
		seen[issueType] = true
		normalized = append(normalized, issueType)
	}
	sort.Strings(normalized)
	return normalized, nil
}

func parseAppliesToJSON(raw string) ([]string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, errors.New("applies_to_json is empty")
	}
	var appliesTo []string
	if err := json.Unmarshal([]byte(raw), &appliesTo); err != nil {
		return nil, fmt.Errorf("decode applies_to_json: %w", err)
	}
	return normalizeGateAppliesTo(appliesTo)
}

func canonicalizeGateDefinition(raw string) (string, string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", "", errors.New("--file must contain JSON")
	}

	var decoded any
	if err := json.Unmarshal([]byte(raw), &decoded); err != nil {
		return "", "", fmt.Errorf("invalid gate definition JSON: %w", err)
	}

	canonicalBytes, err := json.Marshal(decoded)
	if err != nil {
		return "", "", fmt.Errorf("canonicalize gate definition JSON: %w", err)
	}
	hash := sha256.Sum256(canonicalBytes)
	return string(canonicalBytes), hex.EncodeToString(hash[:]), nil
}

func parseGateTemplateRef(raw string) (gateTemplateRef, error) {
	ref := strings.TrimSpace(raw)
	templateIDRaw, versionRaw, ok := strings.Cut(ref, "@")
	if !ok {
		return gateTemplateRef{}, fmt.Errorf("invalid --template %q (expected <template_id>@<version>)", raw)
	}
	templateID, err := normalizeGateTemplateID(templateIDRaw)
	if err != nil {
		return gateTemplateRef{}, err
	}
	version, err := strconv.Atoi(strings.TrimSpace(versionRaw))
	if err != nil || version <= 0 {
		return gateTemplateRef{}, fmt.Errorf("invalid --template %q (version must be > 0)", raw)
	}
	return gateTemplateRef{
		TemplateID: templateID,
		Version:    version,
		Ref:        fmt.Sprintf("%s@%d", templateID, version),
	}, nil
}

func normalizeGateTemplateRefs(rawRefs []string) ([]string, []gateTemplateRef, error) {
	if len(rawRefs) == 0 {
		return nil, nil, errors.New("--template is required")
	}
	parsed := make([]gateTemplateRef, 0, len(rawRefs))
	seen := make(map[string]bool, len(rawRefs))
	for _, raw := range rawRefs {
		ref, err := parseGateTemplateRef(raw)
		if err != nil {
			return nil, nil, err
		}
		if seen[ref.Ref] {
			continue
		}
		seen[ref.Ref] = true
		parsed = append(parsed, ref)
	}
	sort.Slice(parsed, func(i, j int) bool {
		return parsed[i].Ref < parsed[j].Ref
	})
	refs := make([]string, 0, len(parsed))
	for _, ref := range parsed {
		refs = append(refs, ref.Ref)
	}
	return refs, parsed, nil
}

func buildGateSetDefinitionsTx(ctx context.Context, tx *sql.Tx, issueType string, refs []gateTemplateRef) ([]GateSetDefinition, error) {
	gatesByID := make(map[string]GateSetDefinition)
	for _, ref := range refs {
		defs, err := gateDefinitionsForTemplateRefTx(ctx, tx, issueType, ref)
		if err != nil {
			return nil, err
		}
		if err := mergeGateDefinitionsByID(gatesByID, defs, ref.Ref); err != nil {
			return nil, err
		}
	}

	gates := sortGateDefinitionsByID(gatesByID)
	if len(gates) == 0 {
		return nil, errors.New("instantiated gate set has no gates")
	}
	if err := validateRequiredGateDefinitionsForCLIClosure(gates); err != nil {
		return nil, err
	}
	return gates, nil
}

func gateDefinitionsForTemplateRefTx(ctx context.Context, tx *sql.Tx, issueType string, ref gateTemplateRef) ([]GateSetDefinition, error) {
	appliesToJSON, definitionJSON, approvedBy, err := loadGateTemplateProjectionTx(ctx, tx, ref)
	if err != nil {
		return nil, err
	}

	appliesTo, err := parseAppliesToJSON(appliesToJSON)
	if err != nil {
		return nil, err
	}
	if !stringSliceContains(appliesTo, issueType) {
		return nil, fmt.Errorf("gate template %s@%d does not apply to issue type %s", ref.TemplateID, ref.Version, issueType)
	}

	defs, err := extractGateDefinitions(definitionJSON)
	if err != nil {
		return nil, fmt.Errorf("invalid gate definition in template %s@%d: %w", ref.TemplateID, ref.Version, err)
	}
	if gateDefinitionsIncludeExecutableCommand(defs) && !actorIsHumanGoverned(approvedBy) {
		return nil, fmt.Errorf("gate template %s@%d contains executable criteria.command but is pending human approval", ref.TemplateID, ref.Version)
	}
	return defs, nil
}

func loadGateTemplateProjectionTx(ctx context.Context, tx *sql.Tx, ref gateTemplateRef) (string, string, string, error) {
	var (
		appliesToJSON  string
		definitionJSON string
		approvedBy     string
	)
	err := tx.QueryRowContext(ctx, `
		SELECT t.applies_to_json, t.definition_json, COALESCE(a.approved_by, '')
		FROM gate_templates AS t
		LEFT JOIN gate_template_approvals AS a
			ON a.template_id = t.template_id
			AND a.version = t.version
		WHERE t.template_id = ? AND t.version = ?
	`, ref.TemplateID, ref.Version).Scan(&appliesToJSON, &definitionJSON, &approvedBy)
	if errors.Is(err, sql.ErrNoRows) {
		return "", "", "", fmt.Errorf("gate template %s@%d not found", ref.TemplateID, ref.Version)
	}
	if err != nil {
		return "", "", "", fmt.Errorf("query gate template %s@%d: %w", ref.TemplateID, ref.Version, err)
	}
	return appliesToJSON, definitionJSON, approvedBy, nil
}

func mergeGateDefinitionsByID(gatesByID map[string]GateSetDefinition, defs []GateSetDefinition, ref string) error {
	for _, gate := range defs {
		if existing, exists := gatesByID[gate.GateID]; exists {
			return fmt.Errorf("duplicate gate id %q across templates (%s conflicts with existing %s)", gate.GateID, ref, existing.GateID)
		}
		gatesByID[gate.GateID] = gate
	}
	return nil
}

func sortGateDefinitionsByID(gatesByID map[string]GateSetDefinition) []GateSetDefinition {
	gates := make([]GateSetDefinition, 0, len(gatesByID))
	for _, gate := range gatesByID {
		gates = append(gates, gate)
	}
	sort.Slice(gates, func(i, j int) bool {
		return gates[i].GateID < gates[j].GateID
	})
	return gates
}

func extractGateDefinitions(definitionJSON string) ([]GateSetDefinition, error) {
	var parsed struct {
		Gates []struct {
			ID       string          `json:"id"`
			Kind     string          `json:"kind"`
			Required *bool           `json:"required"`
			Criteria json.RawMessage `json:"criteria"`
		} `json:"gates"`
	}
	if err := json.Unmarshal([]byte(definitionJSON), &parsed); err != nil {
		return nil, err
	}
	if len(parsed.Gates) == 0 {
		return nil, errors.New("definition must contain at least one gate")
	}

	defs := make([]GateSetDefinition, 0, len(parsed.Gates))
	seen := make(map[string]bool, len(parsed.Gates))
	for _, gate := range parsed.Gates {
		gateID := strings.TrimSpace(gate.ID)
		if gateID == "" {
			return nil, errors.New("each gate requires a non-empty id")
		}
		if seen[gateID] {
			return nil, fmt.Errorf("duplicate gate id %q in template definition", gateID)
		}
		seen[gateID] = true

		kind := strings.TrimSpace(gate.Kind)
		if kind == "" {
			kind = "check"
		}
		required := true
		if gate.Required != nil {
			required = *gate.Required
		}

		var criteria any = map[string]any{}
		if len(gate.Criteria) > 0 {
			if err := json.Unmarshal(gate.Criteria, &criteria); err != nil {
				return nil, fmt.Errorf("decode criteria for gate %q: %w", gateID, err)
			}
		}

		defs = append(defs, GateSetDefinition{
			GateID:   gateID,
			Kind:     kind,
			Required: required,
			Criteria: criteria,
		})
	}

	sort.Slice(defs, func(i, j int) bool {
		return defs[i].GateID < defs[j].GateID
	})
	return defs, nil
}

func gateDefinitionsIncludeExecutableCommand(defs []GateSetDefinition) bool {
	for _, def := range defs {
		if gateCriteriaCommand(def.Criteria) != "" {
			return true
		}
	}
	return false
}

func gateDefinitionContainsExecutableCommand(definitionJSON string) bool {
	defs, err := extractGateDefinitions(definitionJSON)
	if err != nil {
		return false
	}
	return gateDefinitionsIncludeExecutableCommand(defs)
}

func validateRequiredGateDefinitionsForCLIClosure(defs []GateSetDefinition) error {
	nonExecutableRequired := make([]string, 0)
	for _, def := range defs {
		if !def.Required {
			continue
		}
		if gateCriteriaCommand(def.Criteria) == "" && !gateCriteriaRefMatches(def.Criteria, "manual-validation") {
			nonExecutableRequired = append(nonExecutableRequired, def.GateID)
		}
	}
	if len(nonExecutableRequired) == 0 {
		return nil
	}
	return fmt.Errorf(
		"required gate(s) lack executable criteria.command and cannot be closed through the CLI: %s",
		strings.Join(nonExecutableRequired, ", "),
	)
}

func gateCriteriaCommand(criteria any) string {
	switch typed := criteria.(type) {
	case map[string]any:
		command, _ := typed["command"].(string)
		return strings.TrimSpace(command)
	case map[string]string:
		return strings.TrimSpace(typed["command"])
	default:
		return ""
	}
}

func gateCriteriaRefMatches(criteria any, want string) bool {
	want = strings.TrimSpace(want)
	if want == "" {
		return false
	}
	switch typed := criteria.(type) {
	case map[string]any:
		ref, _ := typed["ref"].(string)
		return strings.TrimSpace(ref) == want
	case map[string]string:
		return strings.TrimSpace(typed["ref"]) == want
	default:
		return false
	}
}

func actorIsHumanGoverned(actor string) bool {
	actor = strings.TrimSpace(strings.ToLower(actor))
	return actor != "" && !strings.HasPrefix(actor, "llm:")
}

func actorIsHuman(actor string) bool {
	actor = strings.TrimSpace(strings.ToLower(actor))
	return strings.HasPrefix(actor, "human:")
}

func buildFrozenGateDefinition(templateRefs []string, gates []GateSetDefinition) (string, map[string]any, error) {
	frozen := struct {
		Templates []string            `json:"templates"`
		Gates     []GateSetDefinition `json:"gates"`
	}{
		Templates: templateRefs,
		Gates:     gates,
	}
	frozenBytes, err := json.Marshal(frozen)
	if err != nil {
		return "", nil, fmt.Errorf("encode frozen gate definition: %w", err)
	}
	var frozenObj map[string]any
	if err := json.Unmarshal(frozenBytes, &frozenObj); err != nil {
		return "", nil, fmt.Errorf("decode frozen gate definition: %w", err)
	}
	return string(frozenBytes), frozenObj, nil
}
