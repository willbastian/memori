package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"os/user"
	"reflect"
	"strings"
	"testing"
)

type helperCoverageStringer string

func (s helperCoverageStringer) String() string {
	return string(s)
}

func TestGetIssueAndGetIssueTxReturnNotFoundForMissingRows(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	if _, err := s.GetIssue(ctx, "mem-deadbee"); err == nil || !strings.Contains(err.Error(), `issue "mem-deadbee" not found`) {
		t.Fatalf("expected GetIssue not found error, got %v", err)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.Rollback()

	if _, err := getIssueTx(ctx, tx, "mem-deadbee"); err == nil || !strings.Contains(err.Error(), `issue "mem-deadbee" not found`) {
		t.Fatalf("expected getIssueTx not found error, got %v", err)
	}
}

func TestProjectIssueKeyPrefixTxBackfillsNormalizesAndRejectsInvalidValues(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	if _, err := s.db.ExecContext(ctx, `DELETE FROM schema_meta WHERE key = 'issue_key_prefix'`); err != nil {
		t.Fatalf("delete issue_key_prefix: %v", err)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx for backfill: %v", err)
	}
	prefix, err := s.projectIssueKeyPrefixTx(ctx, tx)
	if err != nil {
		t.Fatalf("project backfilled issue key prefix: %v", err)
	}
	if prefix != DefaultIssueKeyPrefix {
		t.Fatalf("expected backfilled prefix %q, got %q", DefaultIssueKeyPrefix, prefix)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit backfilled prefix: %v", err)
	}

	var storedPrefix string
	if err := s.db.QueryRowContext(ctx, `SELECT value FROM schema_meta WHERE key = 'issue_key_prefix'`).Scan(&storedPrefix); err != nil {
		t.Fatalf("read stored issue_key_prefix: %v", err)
	}
	if storedPrefix != DefaultIssueKeyPrefix {
		t.Fatalf("expected stored backfilled prefix %q, got %q", DefaultIssueKeyPrefix, storedPrefix)
	}

	if _, err := s.db.ExecContext(ctx, `UPDATE schema_meta SET value = '  AbC42  ' WHERE key = 'issue_key_prefix'`); err != nil {
		t.Fatalf("update issue_key_prefix to mixed case: %v", err)
	}
	tx, err = s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx for normalization: %v", err)
	}
	prefix, err = s.projectIssueKeyPrefixTx(ctx, tx)
	if err != nil {
		t.Fatalf("project normalized issue key prefix: %v", err)
	}
	if prefix != "abc42" {
		t.Fatalf("expected normalized prefix %q, got %q", "abc42", prefix)
	}
	if err := tx.Rollback(); err != nil && err != sql.ErrTxDone {
		t.Fatalf("rollback normalization tx: %v", err)
	}

	if _, err := s.db.ExecContext(ctx, `UPDATE schema_meta SET value = '1bad' WHERE key = 'issue_key_prefix'`); err != nil {
		t.Fatalf("update issue_key_prefix to invalid value: %v", err)
	}
	tx, err = s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx for invalid prefix: %v", err)
	}
	defer tx.Rollback()

	if _, err := s.projectIssueKeyPrefixTx(ctx, tx); err == nil || !strings.Contains(err.Error(), `invalid stored issue_key_prefix "1bad"`) {
		t.Fatalf("expected invalid stored prefix error, got %v", err)
	}
}

func TestIssueLinkValidationHelpersCoverSelfParentAndLookupEdges(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.Rollback()

	parent := Issue{ID: "mem-a1b2c3d", Type: "Epic"}
	if err := validateIssueLinkForNewIssueTx(ctx, tx, "mem-a1b2c3d", "Story", parent); err == nil || !strings.Contains(err.Error(), "issue cannot be its own parent") {
		t.Fatalf("expected self-parent validation error, got %v", err)
	}

	if _, err := parentIDForIssueTx(ctx, tx, "mem-deadbee"); err == nil || !strings.Contains(err.Error(), `issue "mem-deadbee" not found`) {
		t.Fatalf("expected missing parent lookup error, got %v", err)
	}
}

func TestStoreHelperNormalizationFunctions(t *testing.T) {
	t.Parallel()

	if got, err := normalizeIssueKey(" MEM-AbCdEf1 "); err != nil || got != "mem-abcdef1" {
		t.Fatalf("expected normalized issue key, got %q err=%v", got, err)
	}
	if _, err := normalizeIssueKey("mem-abc"); err == nil || !strings.Contains(err.Error(), "shortSHA must be 7-12 hex chars") {
		t.Fatalf("expected short issue key validation error, got %v", err)
	}
	if _, err := normalizeIssueKey("mem-zzzzzzz"); err == nil || !strings.Contains(err.Error(), "shortSHA must be hex") {
		t.Fatalf("expected hex issue key validation error, got %v", err)
	}
	if _, err := normalizeIssueKey("1bad-abcdef1"); err == nil || !strings.Contains(err.Error(), "must start with a letter") {
		t.Fatalf("expected invalid issue key prefix error, got %v", err)
	}
	if _, err := normalizeIssueKey("memabcdef1"); err == nil || !strings.Contains(err.Error(), "expected {prefix}-{shortSHA}") {
		t.Fatalf("expected issue key shape validation error, got %v", err)
	}

	if got, err := normalizeIssueKeyPrefix(""); err != nil || got != DefaultIssueKeyPrefix {
		t.Fatalf("expected default issue key prefix, got %q err=%v", got, err)
	}
	if got, err := normalizeIssueKeyPrefix(" AbC42 "); err != nil || got != "abc42" {
		t.Fatalf("expected normalized issue key prefix, got %q err=%v", got, err)
	}
	if _, err := normalizeIssueKeyPrefix("a"); err == nil || !strings.Contains(err.Error(), "must be 2-16") {
		t.Fatalf("expected short prefix validation error, got %v", err)
	}
	if _, err := normalizeIssueKeyPrefix("bad-prefix"); err == nil || !strings.Contains(err.Error(), "must use lowercase letters/digits") {
		t.Fatalf("expected prefix character validation error, got %v", err)
	}

	if err := validateIssueTypeNotEmbeddedInKeyPrefix("mem-abcdef1"); err != nil {
		t.Fatalf("expected non-reserved issue key prefix to pass, got %v", err)
	}
	if err := validateIssueTypeNotEmbeddedInKeyPrefix("task-abcdef1"); err == nil || !strings.Contains(err.Error(), "type must be in --type, not key prefix") {
		t.Fatalf("expected embedded issue type validation error, got %v", err)
	}
	if err := validateIssueTypeNotEmbeddedInKeyPrefix("badkey"); err == nil || !strings.Contains(err.Error(), "expected {prefix}-{shortSHA}") {
		t.Fatalf("expected malformed embedded type validation error, got %v", err)
	}

	if err := validateIssueKeyPrefixMatchesProject("mem-abcdef1", "mem"); err != nil {
		t.Fatalf("expected matching project prefix to pass, got %v", err)
	}
	if err := validateIssueKeyPrefixMatchesProject("wrk-abcdef1", "mem"); err == nil || !strings.Contains(err.Error(), `prefix must match project prefix "mem"`) {
		t.Fatalf("expected project prefix mismatch error, got %v", err)
	}
	if err := validateIssueKeyPrefixMatchesProject("not-a-key", "mem"); err == nil || !strings.Contains(err.Error(), "expected {prefix}-{shortSHA}") {
		t.Fatalf("expected malformed project prefix key error, got %v", err)
	}

	if got, err := parseReferencesJSON(""); err != nil || !reflect.DeepEqual(got, []string{}) {
		t.Fatalf("expected empty references for blank json, got %#v err=%v", got, err)
	}
	if got, err := parseReferencesJSON(`[" alpha ","beta","alpha",""," gamma "]`); err != nil || !reflect.DeepEqual(got, []string{"alpha", "beta", "gamma"}) {
		t.Fatalf("unexpected normalized references: %#v err=%v", got, err)
	}
	if _, err := parseReferencesJSON(`{`); err == nil || !strings.Contains(err.Error(), "decode references_json") {
		t.Fatalf("expected references json decode error, got %v", err)
	}
	if _, err := parseLabelsJSON(`{`); err == nil || !strings.Contains(err.Error(), "decode labels_json") {
		t.Fatalf("expected labels json decode error, got %v", err)
	}

	if got := normalizeLabels(nil); !reflect.DeepEqual(got, []string{}) {
		t.Fatalf("expected empty labels slice for nil input, got %#v", got)
	}
	if got := normalizeLabels([]string{" alpha ", "beta", "alpha", "", "beta", " gamma "}); !reflect.DeepEqual(got, []string{"alpha", "beta", "gamma"}) {
		t.Fatalf("unexpected normalized labels: %#v", got)
	}

	if got, err := normalizePriority(" p1 "); err != nil || got != "P1" {
		t.Fatalf("expected normalized priority, got %q err=%v", got, err)
	}
	if got, err := normalizePriority(" "); err != nil || got != "" {
		t.Fatalf("expected blank priority to normalize to empty string, got %q err=%v", got, err)
	}
	if _, err := normalizePriority(strings.Repeat("a", 33)); err == nil || !strings.Contains(err.Error(), "max length 32") {
		t.Fatalf("expected priority length validation error, got %v", err)
	}
	if _, err := normalizePriority("p1!"); err == nil || !strings.Contains(err.Error(), "invalid --priority") {
		t.Fatalf("expected priority character validation error, got %v", err)
	}

	if normalizeGateEvaluationProof(nil) != nil {
		t.Fatal("expected nil proof to stay nil")
	}
	proof := &GateEvaluationProof{
		Verifier:      " verifier ",
		Runner:        " runner ",
		RunnerVersion: " 1.2.3 ",
		ExitCode:      17,
		StartedAt:     " start ",
		FinishedAt:    " finish ",
		GateSetHash:   " hash ",
	}
	normalized := normalizeGateEvaluationProof(proof)
	if normalized == proof {
		t.Fatal("expected normalizeGateEvaluationProof to return a copy")
	}
	if normalized.Verifier != "verifier" || normalized.Runner != "runner" || normalized.RunnerVersion != "1.2.3" || normalized.StartedAt != "start" || normalized.FinishedAt != "finish" || normalized.GateSetHash != "hash" {
		t.Fatalf("unexpected normalized proof: %#v", normalized)
	}
	if proof.Verifier != " verifier " {
		t.Fatalf("expected original proof to stay untouched, got %#v", proof)
	}

	if equalStringSlices([]string{"a"}, []string{"a", "b"}) {
		t.Fatal("expected slices with different lengths to differ")
	}
	if equalStringSlices([]string{"a", "b"}, []string{"a", "c"}) {
		t.Fatal("expected slices with different values to differ")
	}
	if !equalStringSlices([]string{"a", "b"}, []string{"a", "b"}) {
		t.Fatal("expected equal slices to compare equal")
	}

	if got := anyToInt(json.Number("7")); got != 7 {
		t.Fatalf("expected json.Number to parse as 7, got %d", got)
	}
	if got := anyToInt(" 9 "); got != 9 {
		t.Fatalf("expected string to parse as 9, got %d", got)
	}
	if got := anyToInt(struct{}{}); got != 0 {
		t.Fatalf("expected unsupported anyToInt value to return 0, got %d", got)
	}

	if got := anyToString(helperCoverageStringer("hello")); got != "hello" {
		t.Fatalf("expected Stringer to convert to %q, got %q", "hello", got)
	}
	if got := anyToString(nil); got != "" {
		t.Fatalf("expected nil anyToString to return empty string, got %q", got)
	}

	if got := newID("evt"); !strings.HasPrefix(got, "evt_") {
		t.Fatalf("expected new id prefix, got %q", got)
	}
	if got := newIssueKey("mem"); !strings.HasPrefix(got, "mem-") || len(strings.TrimPrefix(got, "mem-")) != 7 {
		t.Fatalf("expected new issue key prefix and short sha length, got %q", got)
	}

	if got := strings.TrimSpace(defaultActor()); got == "" {
		t.Fatal("expected defaultActor to return a non-empty actor")
	}

	if err := validateParentChildTypeConstraint("Task", "Bug"); err == nil || !strings.Contains(err.Error(), "parent Task cannot have children") {
		t.Fatalf("expected non-parent type validation error, got %v", err)
	}
}

func TestDefaultActorPrefersCurrentUserThenEnvThenLocal(t *testing.T) {
	originalCurrentOSUser := currentOSUser
	currentOSUser = originalCurrentOSUser
	t.Cleanup(func() {
		currentOSUser = originalCurrentOSUser
	})

	originalUser, hadUser := os.LookupEnv("USER")
	t.Cleanup(func() {
		if hadUser {
			_ = os.Setenv("USER", originalUser)
		} else {
			_ = os.Unsetenv("USER")
		}
	})

	currentOSUser = func() (*user.User, error) {
		return &user.User{Username: "system-user"}, nil
	}
	_ = os.Setenv("USER", "env-user")
	if got := defaultActor(); got != "system-user" {
		t.Fatalf("expected current user to win, got %q", got)
	}

	currentOSUser = func() (*user.User, error) {
		return nil, os.ErrNotExist
	}
	if got := defaultActor(); got != "env-user" {
		t.Fatalf("expected USER fallback, got %q", got)
	}

	_ = os.Unsetenv("USER")
	currentOSUser = func() (*user.User, error) {
		return &user.User{}, nil
	}
	if got := defaultActor(); got != "local" {
		t.Fatalf("expected final local fallback, got %q", got)
	}
}
