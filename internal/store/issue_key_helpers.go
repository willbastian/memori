package store

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

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
