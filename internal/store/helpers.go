package store

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/user"
	"sort"
	"strconv"
	"strings"
	"time"
)

var currentOSUser = user.Current

type gateTemplateRef struct {
	TemplateID string
	Version    int
	Ref        string
}

func normalizeIssueType(raw string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "epic":
		return "Epic", nil
	case "story":
		return "Story", nil
	case "task":
		return "Task", nil
	case "bug":
		return "Bug", nil
	default:
		return "", fmt.Errorf("invalid --type %q (expected epic|story|task|bug)", raw)
	}
}

func normalizeIssueStatus(raw string) (string, error) {
	switch canonicalIssueStatusToken(raw) {
	case "todo":
		return "Todo", nil
	case "inprogress":
		return "InProgress", nil
	case "blocked":
		return "Blocked", nil
	case "done":
		return "Done", nil
	case "wontdo":
		return "WontDo", nil
	default:
		return "", fmt.Errorf("invalid --status %q (expected todo|inprogress|blocked|done|wontdo)", raw)
	}
}

func canonicalIssueStatusToken(raw string) string {
	token := strings.ToLower(strings.TrimSpace(raw))
	replacer := strings.NewReplacer("'", "", "’", "", "-", "", "_", "", " ", "")
	return replacer.Replace(token)
}

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
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
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
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		return gateTemplateCreatedPayload{}, err
	}
	templateID, err := normalizeGateTemplateID(payload.TemplateID)
	if err != nil {
		return gateTemplateCreatedPayload{}, fmt.Errorf("invalid template_id: %w", err)
	}
	if payload.Version <= 0 {
		return gateTemplateCreatedPayload{}, errors.New("version must be > 0")
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
	payload.CreatedAt = strings.TrimSpace(payload.CreatedAt)
	payload.CreatedBy = strings.TrimSpace(payload.CreatedBy)
	if payload.CreatedAt == "" {
		return gateTemplateCreatedPayload{}, errors.New("created_at is required")
	}
	if payload.CreatedBy == "" {
		return gateTemplateCreatedPayload{}, errors.New("created_by is required")
	}
	return payload, nil
}

func decodeGateTemplateApprovedPayload(payloadJSON string) (gateTemplateApprovedPayload, error) {
	var payload gateTemplateApprovedPayload
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		return gateTemplateApprovedPayload{}, err
	}
	templateID, err := normalizeGateTemplateID(payload.TemplateID)
	if err != nil {
		return gateTemplateApprovedPayload{}, fmt.Errorf("invalid template_id: %w", err)
	}
	if payload.Version <= 0 {
		return gateTemplateApprovedPayload{}, errors.New("version must be > 0")
	}

	payload.TemplateID = templateID
	payload.DefinitionHash = strings.TrimSpace(payload.DefinitionHash)
	payload.ApprovedAt = strings.TrimSpace(payload.ApprovedAt)
	payload.ApprovedBy = strings.TrimSpace(payload.ApprovedBy)
	if payload.DefinitionHash == "" {
		return gateTemplateApprovedPayload{}, errors.New("definition_hash is required")
	}
	if payload.ApprovedAt == "" {
		return gateTemplateApprovedPayload{}, errors.New("approved_at is required")
	}
	if !actorIsHumanGoverned(payload.ApprovedBy) {
		return gateTemplateApprovedPayload{}, errors.New("approved_by must be human-governed")
	}
	return payload, nil
}

func decodeGateSetInstantiatedPayload(payloadJSON string) (gateSetInstantiatedPayload, error) {
	var payload gateSetInstantiatedPayload
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		return gateSetInstantiatedPayload{}, err
	}
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
	if strings.TrimSpace(payload.GateSetID) == "" {
		return gateSetInstantiatedPayload{}, errors.New("gate_set_id is required")
	}
	if strings.TrimSpace(payload.GateSetHash) == "" {
		return gateSetInstantiatedPayload{}, errors.New("gate_set_hash is required")
	}
	if strings.TrimSpace(payload.CreatedAt) == "" {
		return gateSetInstantiatedPayload{}, errors.New("created_at is required")
	}
	if strings.TrimSpace(payload.CreatedBy) == "" {
		return gateSetInstantiatedPayload{}, errors.New("created_by is required")
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

func decodeGateSetLockedPayload(payloadJSON string) (gateSetLockedPayload, error) {
	var payload gateSetLockedPayload
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		return gateSetLockedPayload{}, err
	}
	issueID, err := normalizeIssueKey(payload.IssueID)
	if err != nil {
		return gateSetLockedPayload{}, fmt.Errorf("invalid issue_id: %w", err)
	}
	if strings.TrimSpace(payload.GateSetID) == "" {
		return gateSetLockedPayload{}, errors.New("gate_set_id is required")
	}
	if payload.CycleNo <= 0 {
		return gateSetLockedPayload{}, errors.New("cycle_no must be > 0")
	}
	payload.IssueID = issueID
	payload.LockedAt = strings.TrimSpace(payload.LockedAt)
	if payload.LockedAt == "" {
		return gateSetLockedPayload{}, errors.New("locked_at is required")
	}
	return payload, nil
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
			return nil, fmt.Errorf("gate template %s@%d not found", ref.TemplateID, ref.Version)
		}
		if err != nil {
			return nil, fmt.Errorf("query gate template %s@%d: %w", ref.TemplateID, ref.Version, err)
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
		for _, gate := range defs {
			if existing, exists := gatesByID[gate.GateID]; exists {
				return nil, fmt.Errorf("duplicate gate id %q across templates (%s conflicts with existing %s)", gate.GateID, ref.Ref, existing.GateID)
			}
			gatesByID[gate.GateID] = gate
		}
	}

	gates := make([]GateSetDefinition, 0, len(gatesByID))
	for _, gate := range gatesByID {
		gates = append(gates, gate)
	}
	sort.Slice(gates, func(i, j int) bool {
		return gates[i].GateID < gates[j].GateID
	})
	if len(gates) == 0 {
		return nil, errors.New("instantiated gate set has no gates")
	}
	if err := validateRequiredGateDefinitionsForCLIClosure(gates); err != nil {
		return nil, err
	}
	return gates, nil
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

func normalizeIssueKey(raw string) (string, error) {
	key := strings.ToLower(strings.TrimSpace(raw))
	parts := strings.Split(key, "-")
	if len(parts) != 2 {
		return "", fmt.Errorf("invalid issue key %q (expected {prefix}-{shortSHA})", raw)
	}

	prefix, err := normalizeIssueKeyPrefix(parts[0])
	if err != nil {
		return "", err
	}

	shortSHA := parts[1]
	if len(shortSHA) < 7 || len(shortSHA) > 12 {
		return "", fmt.Errorf("invalid issue key %q (shortSHA must be 7-12 hex chars)", raw)
	}
	for _, r := range shortSHA {
		if !isHexRune(r) {
			return "", fmt.Errorf("invalid issue key %q (shortSHA must be hex)", raw)
		}
	}

	return prefix + "-" + shortSHA, nil
}

func normalizeIssueKeyPrefix(raw string) (string, error) {
	prefix := strings.ToLower(strings.TrimSpace(raw))
	if prefix == "" {
		prefix = DefaultIssueKeyPrefix
	}
	if len(prefix) < 2 || len(prefix) > 16 {
		return "", fmt.Errorf("invalid issue key prefix %q (must be 2-16 lowercase letters/digits)", raw)
	}
	for i, r := range prefix {
		if i == 0 && (r < 'a' || r > 'z') {
			return "", fmt.Errorf("invalid issue key prefix %q (must start with a letter)", raw)
		}
		if (r < 'a' || r > 'z') && (r < '0' || r > '9') {
			return "", fmt.Errorf("invalid issue key prefix %q (must use lowercase letters/digits)", raw)
		}
	}
	return prefix, nil
}

func validateIssueTypeNotEmbeddedInKeyPrefix(issueKey string) error {
	parts := strings.Split(issueKey, "-")
	if len(parts) != 2 {
		return fmt.Errorf("invalid issue key %q (expected {prefix}-{shortSHA})", issueKey)
	}
	switch parts[0] {
	case "epic", "story", "task", "bug":
		return fmt.Errorf("invalid issue key %q (type must be in --type, not key prefix)", issueKey)
	default:
		return nil
	}
}

func validateIssueKeyPrefixMatchesProject(issueKey, projectPrefix string) error {
	parts := strings.Split(issueKey, "-")
	if len(parts) != 2 {
		return fmt.Errorf("invalid issue key %q (expected {prefix}-{shortSHA})", issueKey)
	}
	if parts[0] != projectPrefix {
		return fmt.Errorf(
			"invalid issue key %q (prefix must match project prefix %q)",
			issueKey,
			projectPrefix,
		)
	}
	return nil
}

func isHexRune(r rune) bool {
	return (r >= '0' && r <= '9') || (r >= 'a' && r <= 'f')
}

func normalizeReferences(references []string) []string {
	if len(references) == 0 {
		return []string{}
	}
	normalized := make([]string, 0, len(references))
	seen := make(map[string]bool, len(references))
	for _, reference := range references {
		ref := strings.TrimSpace(reference)
		if ref == "" || seen[ref] {
			continue
		}
		seen[ref] = true
		normalized = append(normalized, ref)
	}
	return normalized
}

func parseReferencesJSON(raw string) ([]string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return []string{}, nil
	}
	var references []string
	if err := json.Unmarshal([]byte(raw), &references); err != nil {
		return nil, fmt.Errorf("decode references_json: %w", err)
	}
	return normalizeReferences(references), nil
}

func normalizeLabels(labels []string) []string {
	return normalizeReferences(labels)
}

func normalizeGateEvaluationProof(proof *GateEvaluationProof) *GateEvaluationProof {
	if proof == nil {
		return nil
	}
	normalized := *proof
	normalized.Verifier = strings.TrimSpace(normalized.Verifier)
	normalized.Runner = strings.TrimSpace(normalized.Runner)
	normalized.RunnerVersion = strings.TrimSpace(normalized.RunnerVersion)
	normalized.StartedAt = strings.TrimSpace(normalized.StartedAt)
	normalized.FinishedAt = strings.TrimSpace(normalized.FinishedAt)
	normalized.GateSetHash = strings.TrimSpace(normalized.GateSetHash)
	return &normalized
}

func parseLabelsJSON(raw string) ([]string, error) {
	labels, err := parseReferencesJSON(raw)
	if err != nil {
		return nil, fmt.Errorf("decode labels_json: %w", err)
	}
	return labels, nil
}

func normalizePriority(raw string) (string, error) {
	priority := strings.ToUpper(strings.TrimSpace(raw))
	if priority == "" {
		return "", nil
	}
	if len(priority) > 32 {
		return "", fmt.Errorf("invalid --priority %q (max length 32)", raw)
	}
	for _, ch := range priority {
		if (ch < 'A' || ch > 'Z') && (ch < '0' || ch > '9') && ch != '-' && ch != '_' {
			return "", fmt.Errorf("invalid --priority %q", raw)
		}
	}
	return priority, nil
}

func equalStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func copyStringSlice(values []string) []string {
	if len(values) == 0 {
		return []string{}
	}
	out := make([]string, len(values))
	copy(out, values)
	return out
}

func nowUTC() string {
	return time.Now().UTC().Format(time.RFC3339Nano)
}

func defaultActor() string {
	if current, err := currentOSUser(); err == nil {
		if current.Username != "" {
			return current.Username
		}
	}
	if fromEnv := os.Getenv("USER"); fromEnv != "" {
		return fromEnv
	}
	return "local"
}

func newID(prefix string) string {
	buf := make([]byte, 8)
	if _, err := rand.Read(buf); err != nil {
		return fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())
	}
	return fmt.Sprintf("%s_%s", prefix, hex.EncodeToString(buf))
}

func newIssueKey(prefix string) string {
	random := make([]byte, 16)
	if _, err := rand.Read(random); err != nil {
		now := strconv.FormatInt(time.Now().UnixNano(), 10)
		sum := sha256.Sum256([]byte(prefix + ":" + now))
		return fmt.Sprintf("%s-%s", prefix, hex.EncodeToString(sum[:])[:7])
	}
	now := strconv.FormatInt(time.Now().UnixNano(), 10)
	input := append(random, []byte(now)...)
	sum := sha256.Sum256(input)
	return fmt.Sprintf("%s-%s", prefix, hex.EncodeToString(sum[:])[:7])
}

func (s *Store) projectIssueKeyPrefixTx(ctx context.Context, tx *sql.Tx) (string, error) {
	var prefix string
	err := tx.QueryRowContext(ctx, `SELECT value FROM schema_meta WHERE key = 'issue_key_prefix'`).Scan(&prefix)
	if errors.Is(err, sql.ErrNoRows) {
		prefix = DefaultIssueKeyPrefix
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO schema_meta(key, value, updated_at) VALUES('issue_key_prefix', ?, ?)
		`, prefix, nowUTC()); err != nil {
			return "", fmt.Errorf("insert missing issue_key_prefix: %w", err)
		}
		return prefix, nil
	}
	if err != nil {
		return "", fmt.Errorf("read issue_key_prefix: %w", err)
	}
	normalized, err := normalizeIssueKeyPrefix(prefix)
	if err != nil {
		return "", fmt.Errorf("invalid stored issue_key_prefix %q: %w", prefix, err)
	}
	return normalized, nil
}

func nullIfEmpty(s string) any {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	return s
}
