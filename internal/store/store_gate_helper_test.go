package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

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
