package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
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

func TestStoreHelperNormalizationFunctions(t *testing.T) {
	t.Parallel()

	if got := normalizeLabels(nil); !reflect.DeepEqual(got, []string{}) {
		t.Fatalf("expected empty labels slice for nil input, got %#v", got)
	}
	if got := normalizeLabels([]string{" alpha ", "beta", "alpha", "", "beta", " gamma "}); !reflect.DeepEqual(got, []string{"alpha", "beta", "gamma"}) {
		t.Fatalf("unexpected normalized labels: %#v", got)
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

	if got := strings.TrimSpace(defaultActor()); got == "" {
		t.Fatal("expected defaultActor to return a non-empty actor")
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

func TestGateHelperDecodersAndNormalizers(t *testing.T) {
	t.Parallel()

	if got := defaultCorrelationID(" issue ", " mem-a1b2c3d "); got != "issue:mem-a1b2c3d" {
		t.Fatalf("unexpected default correlation id: %q", got)
	}
	if got := defaultCorrelationID("", "mem-a1b2c3d"); got != "" {
		t.Fatalf("expected empty default correlation id, got %q", got)
	}
	if got := gateTemplateCorrelationID("tmpl", 2); got != "gate-template:tmpl@2" {
		t.Fatalf("unexpected gate template correlation id: %q", got)
	}
	if got := gateTemplateCorrelationID("", 2); got != "" {
		t.Fatalf("expected empty gate template correlation id, got %q", got)
	}
	if got := gateCycleCorrelationID("mem-a1b2c3d", 3); got != "gate-cycle:mem-a1b2c3d:3" {
		t.Fatalf("unexpected gate cycle correlation id: %q", got)
	}
	if got := gateCycleCorrelationID("mem-a1b2c3d", 0); got != "" {
		t.Fatalf("expected empty gate cycle correlation id, got %q", got)
	}
	if got := packetScopeCorrelationID(" issue ", " mem-a1b2c3d "); got != "packet-scope:issue:mem-a1b2c3d" {
		t.Fatalf("unexpected packet scope correlation id: %q", got)
	}

	if got, err := normalizeGateResult(" pass "); err != nil || got != "PASS" {
		t.Fatalf("expected PASS normalization, got %q err=%v", got, err)
	}
	if _, err := normalizeGateResult("maybe"); err == nil || !strings.Contains(err.Error(), "expected PASS|FAIL|BLOCKED") {
		t.Fatalf("expected invalid gate result error, got %v", err)
	}

	if got, err := normalizeGateTemplateID(" Release_Check "); err != nil || got != "release_check" {
		t.Fatalf("expected normalized template id, got %q err=%v", got, err)
	}
	if _, err := normalizeGateTemplateID("1bad"); err == nil || !strings.Contains(err.Error(), "must start with a lowercase letter") {
		t.Fatalf("expected invalid template id error, got %v", err)
	}
	if _, err := normalizeGateTemplateID("a!"); err == nil || !strings.Contains(err.Error(), "allowed: lowercase letters, digits, -, _") {
		t.Fatalf("expected invalid character error, got %v", err)
	}

	appliesTo, err := normalizeGateAppliesTo([]string{"task", "Bug", "task"})
	if err != nil {
		t.Fatalf("normalize gate applies_to: %v", err)
	}
	if !reflect.DeepEqual(appliesTo, []string{"Bug", "Task"}) {
		t.Fatalf("unexpected normalized applies_to: %#v", appliesTo)
	}
	if _, err := normalizeGateAppliesTo(nil); err == nil || !strings.Contains(err.Error(), "--applies-to is required") {
		t.Fatalf("expected applies_to required error, got %v", err)
	}
	if _, err := parseAppliesToJSON(`["task","bug"]`); err != nil {
		t.Fatalf("parse applies_to json: %v", err)
	}
	if _, err := parseAppliesToJSON(``); err == nil || !strings.Contains(err.Error(), "applies_to_json is empty") {
		t.Fatalf("expected empty applies_to_json error, got %v", err)
	}

	definitionJSON, definitionHash, err := canonicalizeGateDefinition(" \n{\n  \"gates\": [{\"id\":\"build\"}]\n}\n")
	if err != nil {
		t.Fatalf("canonicalize gate definition: %v", err)
	}
	if definitionJSON != `{"gates":[{"id":"build"}]}` || definitionHash == "" {
		t.Fatalf("unexpected canonicalized gate definition: %q hash=%q", definitionJSON, definitionHash)
	}
	if _, _, err := canonicalizeGateDefinition(" "); err == nil || !strings.Contains(err.Error(), "--file must contain JSON") {
		t.Fatalf("expected blank gate definition error, got %v", err)
	}

	ref, err := parseGateTemplateRef(" Release_Check @ 2 ")
	if err != nil {
		t.Fatalf("parse gate template ref: %v", err)
	}
	if ref.Ref != "release_check@2" {
		t.Fatalf("unexpected normalized gate template ref: %#v", ref)
	}
	if _, err := parseGateTemplateRef("release-check"); err == nil || !strings.Contains(err.Error(), "expected <template_id>@<version>") {
		t.Fatalf("expected malformed ref error, got %v", err)
	}
	refs, parsedRefs, err := normalizeGateTemplateRefs([]string{"beta@2", "alpha@1", "beta@2"})
	if err != nil {
		t.Fatalf("normalize gate template refs: %v", err)
	}
	if !reflect.DeepEqual(refs, []string{"alpha@1", "beta@2"}) || len(parsedRefs) != 2 {
		t.Fatalf("unexpected normalized refs=%#v parsed=%#v", refs, parsedRefs)
	}
	if _, _, err := normalizeGateTemplateRefs(nil); err == nil || !strings.Contains(err.Error(), "--template is required") {
		t.Fatalf("expected missing template refs error, got %v", err)
	}

	if got := gateCriteriaCommand(map[string]any{"command": " go test ./... "}); got != "go test ./..." {
		t.Fatalf("unexpected gate criteria command: %q", got)
	}
	if got := gateCriteriaCommand(map[string]string{"command": " echo ok "}); got != "echo ok" {
		t.Fatalf("unexpected string map criteria command: %q", got)
	}
	if got := gateCriteriaCommand("not-a-map"); got != "" {
		t.Fatalf("expected empty command for unsupported criteria, got %q", got)
	}
	if !gateCriteriaRefMatches(map[string]any{"ref": " manual-validation "}, "manual-validation") {
		t.Fatal("expected gate criteria ref match for map[string]any")
	}
	if !gateCriteriaRefMatches(map[string]string{"ref": "manual-validation"}, "manual-validation") {
		t.Fatal("expected gate criteria ref match for map[string]string")
	}
	if gateCriteriaRefMatches(map[string]any{"ref": "other"}, "manual-validation") {
		t.Fatal("expected gate criteria ref mismatch")
	}

	frozenGates := []GateSetDefinition{{GateID: "build", Kind: "check", Required: true, Criteria: map[string]any{"command": "go test ./..."}}}
	frozenJSON, frozenObj, err := buildFrozenGateDefinition([]string{"alpha@1"}, frozenGates)
	if err != nil {
		t.Fatalf("build frozen gate definition: %v", err)
	}
	if !strings.Contains(frozenJSON, `"templates":["alpha@1"]`) || len(frozenObj) == 0 {
		t.Fatalf("unexpected frozen gate definition: %q %#v", frozenJSON, frozenObj)
	}
	if !gateDefinitionContainsExecutableCommand(`{"gates":[{"id":"build","criteria":{"command":"go test ./..."}}]}`) {
		t.Fatal("expected frozen definition to contain executable command")
	}
	if gateDefinitionContainsExecutableCommand(`{"gates":[{"id":"docs","criteria":{"ref":"manual-validation"}}]}`) {
		t.Fatal("expected manual-validation-only definition not to be executable")
	}
	if gateDefinitionContainsExecutableCommand(`{"gates":"invalid"}`) {
		t.Fatal("expected invalid definition to be non-executable")
	}
}

func TestGatePayloadDecodersValidateRequiredFields(t *testing.T) {
	t.Parallel()

	gateEval, err := decodeGateEvaluatedPayload(`{
		"issue_id":" mem-a1b2c3d ",
		"gate_set_id":" gset_123 ",
		"gate_id":" build ",
		"result":" pass ",
		"evidence_refs":[" docs://one "," docs://one ","docs://two"],
		"evaluated_at":" 2026-03-08T00:00:00Z "
	}`)
	if err != nil {
		t.Fatalf("decode gate evaluated payload: %v", err)
	}
	if gateEval.IssueID != "mem-a1b2c3d" || gateEval.GateSetID != "gset_123" || gateEval.GateID != "build" || gateEval.Result != "PASS" {
		t.Fatalf("unexpected gate evaluation payload: %#v", gateEval)
	}
	if !reflect.DeepEqual(gateEval.EvidenceRefs, []string{"docs://one", "docs://two"}) {
		t.Fatalf("unexpected normalized evidence refs: %#v", gateEval.EvidenceRefs)
	}
	if _, err := decodeGateEvaluatedPayload(`{"issue_id":"mem-a1b2c3d","gate_id":"build","result":"PASS"}`); err == nil || !strings.Contains(err.Error(), "gate_set_id is required") {
		t.Fatalf("expected missing gate_set_id error, got %v", err)
	}

	created, err := decodeGateTemplateCreatedPayload(`{
		"template_id":" Release_Check ",
		"version":1,
		"applies_to":["task","bug","task"],
		"definition_json":"{\"gates\":[{\"id\":\"build\",\"criteria\":{\"command\":\"go test ./...\"}}]}",
		"created_at":" 2026-03-08T00:00:00Z ",
		"created_by":" human:alice "
	}`)
	if err != nil {
		t.Fatalf("decode gate template created payload: %v", err)
	}
	if created.TemplateID != "release_check" || created.CreatedBy != "human:alice" || created.DefinitionHash == "" {
		t.Fatalf("unexpected created payload: %#v", created)
	}
	if _, err := decodeGateTemplateCreatedPayload(`{
		"template_id":"release-check",
		"version":1,
		"applies_to":["task"],
		"definition_json":"{\"gates\":[]}",
		"definition_hash":"wrong",
		"created_at":"2026-03-08T00:00:00Z",
		"created_by":"human:alice"
	}`); err == nil || !strings.Contains(err.Error(), "definition_hash does not match definition_json") {
		t.Fatalf("expected gate template hash mismatch error, got %v", err)
	}

	approved, err := decodeGateTemplateApprovedPayload(`{
		"template_id":"release-check",
		"version":1,
		"definition_hash":"abc123",
		"approved_at":" 2026-03-08T00:00:00Z ",
		"approved_by":" human:alice "
	}`)
	if err != nil {
		t.Fatalf("decode gate template approved payload: %v", err)
	}
	if approved.ApprovedBy != "human:alice" || approved.DefinitionHash != "abc123" {
		t.Fatalf("unexpected approved payload: %#v", approved)
	}
	if _, err := decodeGateTemplateApprovedPayload(`{
		"template_id":"release-check",
		"version":1,
		"definition_hash":"abc123",
		"approved_at":"2026-03-08T00:00:00Z",
		"approved_by":"llm:openai:gpt-5"
	}`); err == nil || !strings.Contains(err.Error(), "approved_by must be human-governed") {
		t.Fatalf("expected human-governed approval error, got %v", err)
	}

	frozenItems := []GateSetDefinition{{GateID: "build", Kind: "check", Required: true, Criteria: map[string]any{"command": "go test ./..."}}}
	frozenDefinitionJSON, _, err := buildFrozenGateDefinition([]string{"release-check@1"}, frozenItems)
	if err != nil {
		t.Fatalf("build frozen definition for decode: %v", err)
	}
	frozenHash := sha256.Sum256([]byte(frozenDefinitionJSON))
	instantiated, err := decodeGateSetInstantiatedPayload(`{
		"gate_set_id":"gset_123",
		"issue_id":" mem-a1b2c3d ",
		"cycle_no":1,
		"template_refs":["release-check@1"],
		"frozen_definition":{"ignored":true},
		"gate_set_hash":"` + hex.EncodeToString(frozenHash[:]) + `",
		"created_at":"2026-03-08T00:00:00Z",
		"created_by":"agent-1",
		"items":[{"gate_id":"build","kind":"check","required":true,"criteria":{"command":"go test ./..."}}]
	}`)
	if err != nil {
		t.Fatalf("decode gate set instantiated payload: %v", err)
	}
	if instantiated.IssueID != "mem-a1b2c3d" || instantiated.GateSetID != "gset_123" || len(instantiated.Items) != 1 {
		t.Fatalf("unexpected instantiated payload: %#v", instantiated)
	}
	if _, err := decodeGateSetInstantiatedPayload(`{
		"gate_set_id":"gset_123",
		"issue_id":"mem-a1b2c3d",
		"cycle_no":1,
		"template_refs":["release-check@1"],
		"gate_set_hash":"wrong",
		"created_at":"2026-03-08T00:00:00Z",
		"created_by":"agent-1",
		"items":[{"gate_id":"build","kind":"check","required":true,"criteria":{"command":"go test ./..."}}]
	}`); err == nil || !strings.Contains(err.Error(), "gate_set_hash does not match frozen definition") {
		t.Fatalf("expected gate set hash mismatch error, got %v", err)
	}

	locked, err := decodeGateSetLockedPayload(`{
		"gate_set_id":"gset_123",
		"issue_id":" mem-a1b2c3d ",
		"cycle_no":1,
		"locked_at":" 2026-03-08T00:00:00Z "
	}`)
	if err != nil {
		t.Fatalf("decode gate set locked payload: %v", err)
	}
	if locked.IssueID != "mem-a1b2c3d" || locked.LockedAt != "2026-03-08T00:00:00Z" {
		t.Fatalf("unexpected locked payload: %#v", locked)
	}
	if _, err := decodeGateSetLockedPayload(`{"gate_set_id":"gset_123","issue_id":"mem-a1b2c3d","cycle_no":1}`); err == nil || !strings.Contains(err.Error(), "locked_at is required") {
		t.Fatalf("expected missing locked_at error, got %v", err)
	}
}

func TestPacketIssueIDAndIssuePacketCycleNoFallbacks(t *testing.T) {
	t.Parallel()

	if got := packetScopeID(RehydratePacket{ScopeID: " mem-a1b2c3d "}); got != "mem-a1b2c3d" {
		t.Fatalf("expected direct packet scope id, got %q", got)
	}
	if got := packetScopeID(RehydratePacket{Packet: map[string]any{"scope_id": " mem-b2c3d4e "}}); got != "mem-b2c3d4e" {
		t.Fatalf("expected packet payload scope id fallback, got %q", got)
	}

	issueCases := []struct {
		name   string
		packet RehydratePacket
		wantID string
	}{
		{
			name:   "direct issue id wins",
			packet: RehydratePacket{IssueID: " mem-a1b2c3d ", ScopeID: "mem-ignored"},
			wantID: "mem-a1b2c3d",
		},
		{
			name:   "issue scope falls back to scope id",
			packet: RehydratePacket{Scope: "issue", ScopeID: " mem-b2c3d4e "},
			wantID: "mem-b2c3d4e",
		},
		{
			name:   "packet payload issue id fallback",
			packet: RehydratePacket{Packet: map[string]any{"issue_id": " mem-c3d4e5f "}},
			wantID: "mem-c3d4e5f",
		},
	}
	for _, tc := range issueCases {
		t.Run(tc.name, func(t *testing.T) {
			if got := packetIssueID(tc.packet); got != tc.wantID {
				t.Fatalf("expected packetIssueID %q, got %q", tc.wantID, got)
			}
		})
	}

	cycleCases := []struct {
		name      string
		packet    RehydratePacket
		wantCycle int
	}{
		{
			name:      "direct issue cycle wins",
			packet:    RehydratePacket{IssueCycleNo: 3, Packet: map[string]any{"provenance": map[string]any{"issue_cycle_no": 9}}},
			wantCycle: 3,
		},
		{
			name:      "provenance cycle fallback",
			packet:    RehydratePacket{Packet: map[string]any{"provenance": map[string]any{"issue_cycle_no": json.Number("4")}}},
			wantCycle: 4,
		},
		{
			name:      "state cycle fallback",
			packet:    RehydratePacket{Packet: map[string]any{"state": map[string]any{"cycle_no": "5"}}},
			wantCycle: 5,
		},
		{
			name:      "missing packet returns zero",
			packet:    RehydratePacket{},
			wantCycle: 0,
		},
	}
	for _, tc := range cycleCases {
		t.Run(tc.name, func(t *testing.T) {
			if got := issuePacketCycleNo(tc.packet); got != tc.wantCycle {
				t.Fatalf("expected issuePacketCycleNo %d, got %d", tc.wantCycle, got)
			}
		})
	}
}

func TestIssueToLinkedWorkItemAndLinkedWorkItemListing(t *testing.T) {
	t.Parallel()

	withParent := issueToLinkedWorkItem(Issue{
		ID:       "mem-child01",
		Type:     "Task",
		Title:    "Child work item",
		Status:   "Todo",
		ParentID: "mem-parent1",
	}, "child")
	if withParent["parent_id"] != "mem-parent1" {
		t.Fatalf("expected issueToLinkedWorkItem to include parent_id, got %#v", withParent)
	}
	withoutParent := issueToLinkedWorkItem(Issue{
		ID:     "mem-parent1",
		Type:   "Epic",
		Title:  "Parent work item",
		Status: "InProgress",
	}, "parent")
	if _, ok := withoutParent["parent_id"]; ok {
		t.Fatalf("did not expect parent_id for top-level linked item, got %#v", withoutParent)
	}

	ctx := context.Background()
	s := newTestStore(t)

	parentID := "mem-111aaaa"
	issueID := "mem-222bbbb"
	openChildID := "mem-333cccc"
	doneChildID := "mem-444dddd"

	if _, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   parentID,
		Type:      "epic",
		Title:     "Parent",
		Actor:     "agent-1",
		CommandID: "cmd-linked-parent-1",
	}); err != nil {
		t.Fatalf("create parent issue: %v", err)
	}
	if _, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   issueID,
		Type:      "story",
		Title:     "Current issue",
		ParentID:  parentID,
		Actor:     "agent-1",
		CommandID: "cmd-linked-current-1",
	}); err != nil {
		t.Fatalf("create current issue: %v", err)
	}
	if _, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   openChildID,
		Type:      "task",
		Title:     "Open child",
		ParentID:  issueID,
		Actor:     "agent-1",
		CommandID: "cmd-linked-open-child-1",
	}); err != nil {
		t.Fatalf("create open child issue: %v", err)
	}
	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO work_items(
			id, type, title, parent_id, status, priority, labels_json, current_cycle_no,
			active_gate_set_id, created_at, updated_at, last_event_id, description,
			acceptance_criteria, references_json
		) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, doneChildID, "Task", "Done child", issueID, "Done", nil, "[]", 1, nil, nowUTC(), nowUTC(), "evt_done_child", "", "", "[]"); err != nil {
		t.Fatalf("insert done child issue: %v", err)
	}

	issue, err := s.GetIssue(ctx, issueID)
	if err != nil {
		t.Fatalf("get current issue: %v", err)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx for linked work items: %v", err)
	}
	defer tx.Rollback()

	items, err := listLinkedWorkItemsForIssueTx(ctx, tx, issue)
	if err != nil {
		t.Fatalf("list linked work items: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("expected parent plus one open child, got %#v", items)
	}

	parentItem := findLinkedWorkItem(t, items, parentID)
	if parentItem["relation"] != "parent" || parentItem["title"] != "Parent" || parentItem["status"] != "Todo" {
		t.Fatalf("unexpected parent linked work item: %#v", parentItem)
	}

	childItem := findLinkedWorkItem(t, items, openChildID)
	if childItem["relation"] != "child" || childItem["title"] != "Open child" || childItem["status"] != "Todo" {
		t.Fatalf("unexpected child linked work item: %#v", childItem)
	}
	if _, ok := childItem["parent_id"]; ok {
		t.Fatalf("did not expect child row map to include parent_id, got %#v", childItem)
	}
}

func TestLookupGateEvaluationByCommandBranches(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	if _, _, _, err := s.LookupGateEvaluationByCommand(ctx, "", "cmd-lookup-missing-actor"); err == nil || !strings.Contains(err.Error(), "--actor is required") {
		t.Fatalf("expected missing actor error, got %v", err)
	}
	if _, _, _, err := s.LookupGateEvaluationByCommand(ctx, "agent-1", ""); err == nil || !strings.Contains(err.Error(), "--command-id is required") {
		t.Fatalf("expected missing command id error, got %v", err)
	}

	evaluation, event, found, err := s.LookupGateEvaluationByCommand(ctx, "agent-1", "cmd-lookup-not-found")
	if err != nil {
		t.Fatalf("lookup missing gate evaluation: %v", err)
	}
	if found || !reflect.DeepEqual(evaluation, GateEvaluation{}) || event != (Event{}) {
		t.Fatalf("expected zero values for missing lookup, got evaluation=%#v event=%#v found=%v", evaluation, event, found)
	}

	issueID := "mem-555eeee"
	if _, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   issueID,
		Type:      "task",
		Title:     "Gate lookup success",
		Actor:     "agent-1",
		CommandID: "cmd-lookup-create-1",
	}); err != nil {
		t.Fatalf("create issue: %v", err)
	}
	if _, _, _, err := s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
		IssueID:   issueID,
		Status:    "inprogress",
		Actor:     "agent-1",
		CommandID: "cmd-lookup-progress-1",
	}); err != nil {
		t.Fatalf("move issue to inprogress: %v", err)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx for lookup append: %v", err)
	}
	payloadBytes, err := json.Marshal(gateEvaluatedPayload{
		IssueID:   issueID,
		GateSetID: "gs_lookup_1",
		GateID:    "build",
		Result:    "PASS",
		EvidenceRefs: []string{
			"ci://run/123",
			"docs://build",
		},
		Proof: &GateEvaluationProof{
			Verifier:      "verifier",
			Runner:        "runner",
			RunnerVersion: "1.0",
			ExitCode:      0,
			StartedAt:     "start",
			FinishedAt:    "finish",
			GateSetHash:   "hash",
		},
		EvaluatedAt: nowUTC(),
	})
	if err != nil {
		t.Fatalf("marshal gate evaluation payload: %v", err)
	}
	appendRes, err := s.appendEventTx(ctx, tx, appendEventRequest{
		EntityType:          entityTypeIssue,
		EntityID:            issueID,
		EventType:           eventTypeGateEval,
		PayloadJSON:         string(payloadBytes),
		Actor:               "agent-1",
		CommandID:           "cmd-lookup-gate-1",
		EventPayloadVersion: 1,
	})
	if err != nil {
		t.Fatalf("append gate evaluation event: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit lookup append tx: %v", err)
	}

	lookup, lookupEvent, found, err := s.LookupGateEvaluationByCommand(ctx, "agent-1", "cmd-lookup-gate-1")
	if err != nil {
		t.Fatalf("lookup saved gate evaluation: %v", err)
	}
	if !found {
		t.Fatal("expected lookup to find gate evaluation")
	}
	if lookupEvent.EventID != appendRes.Event.EventID {
		t.Fatalf("expected lookup event id %q, got %q", appendRes.Event.EventID, lookupEvent.EventID)
	}
	if lookup.GateSetID != "gs_lookup_1" || lookup.GateID != "build" || lookup.Result != "PASS" {
		t.Fatalf("unexpected gate evaluation lookup payload: %#v", lookup)
	}
	if !reflect.DeepEqual(lookup.EvidenceRefs, []string{"ci://run/123", "docs://build"}) {
		t.Fatalf("unexpected lookup evidence refs %#v", lookup.EvidenceRefs)
	}
	if lookup.Proof == nil || lookup.Proof.Verifier != "verifier" || lookup.Proof.GateSetHash != "hash" {
		t.Fatalf("expected normalized proof in lookup result, got %#v", lookup.Proof)
	}

	if _, _, _, err := s.LookupGateEvaluationByCommand(ctx, "agent-1", "cmd-lookup-create-1"); err == nil || !strings.Contains(err.Error(), `command id already used by "issue.created"`) {
		t.Fatalf("expected wrong event type error, got %v", err)
	}
}

func findLinkedWorkItem(t *testing.T, items []any, issueID string) map[string]any {
	t.Helper()

	for _, item := range items {
		typed, ok := item.(map[string]any)
		if !ok {
			continue
		}
		if typed["issue_id"] == issueID {
			return typed
		}
	}
	t.Fatalf("linked work item %q not found in %#v", issueID, items)
	return nil
}
