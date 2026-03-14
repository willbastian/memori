package store

import (
	"encoding/json"
	"fmt"
	"os"
	"os/user"
	"strings"
	"time"
)

var currentOSUser = user.Current

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

func nullIfEmpty(s string) any {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	return s
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
