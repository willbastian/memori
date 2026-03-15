package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"
)

func newTestStore(t *testing.T) *Store {
	return newTestStoreWithPrefix(t, DefaultIssueKeyPrefix)
}

func newTestStoreWithPrefix(t *testing.T, prefix string) *Store {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), "memori-test.db")
	s, err := Open(dbPath)
	if err != nil {
		t.Fatalf("open test store: %v", err)
	}
	t.Cleanup(func() {
		_ = s.Close()
	})

	if err := s.Initialize(context.Background(), InitializeParams{IssueKeyPrefix: prefix}); err != nil {
		t.Fatalf("initialize test store: %v", err)
	}

	return s
}

func seedLockedGateSetForTest(t *testing.T, s *Store, issueID, gateSetID string) {
	t.Helper()

	seedLockedGateSetWithProvenanceForTest(t, s, issueID, gateSetID, `["tmpl-default@1"]`, `{"gates":[{"id":"build"}]}`, "agent-1")
}

func seedLockedGateSetWithProvenanceForTest(t *testing.T, s *Store, issueID, gateSetID, templateRefsJSON, frozenDefinitionJSON, createdBy string) {
	t.Helper()

	ctx := context.Background()
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO gate_sets(
			gate_set_id, issue_id, cycle_no, template_refs_json, frozen_definition_json,
			gate_set_hash, locked_at, created_at, created_by
		) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, gateSetID, issueID, 1, templateRefsJSON, frozenDefinitionJSON, gateSetID+"_hash", nowUTC(), nowUTC(), createdBy)
	if err != nil {
		t.Fatalf("insert locked gate set %s: %v", gateSetID, err)
	}
}

func seedGateTemplateRowForTest(t *testing.T, s *Store, templateID string, version int, appliesTo []string, definitionJSON, createdBy string) {
	t.Helper()

	ctx := context.Background()
	canonicalDefinition, definitionHash, err := canonicalizeGateDefinition(definitionJSON)
	if err != nil {
		t.Fatalf("canonicalize gate template %s@%d: %v", templateID, version, err)
	}
	appliesToJSON, err := json.Marshal(appliesTo)
	if err != nil {
		t.Fatalf("marshal applies_to for gate template %s@%d: %v", templateID, version, err)
	}
	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO gate_templates(
			template_id, version, applies_to_json, definition_json,
			definition_hash, created_at, created_by
		) VALUES(?, ?, ?, ?, ?, ?, ?)
	`, templateID, version, string(appliesToJSON), canonicalDefinition, definitionHash, nowUTC(), createdBy); err != nil {
		t.Fatalf("insert gate template %s@%d: %v", templateID, version, err)
	}
	if gateDefinitionContainsExecutableCommand(canonicalDefinition) && actorIsHumanGoverned(createdBy) {
		if _, err := s.db.ExecContext(ctx, `
			INSERT INTO gate_template_approvals(template_id, version, approved_at, approved_by)
			VALUES(?, ?, ?, ?)
		`, templateID, version, nowUTC(), createdBy); err != nil {
			t.Fatalf("insert gate template approval %s@%d: %v", templateID, version, err)
		}
	}
}

func createLockedGateSetEventSourcedForTest(t *testing.T, s *Store, issueID, templateID, gateID, commandPrefix string) string {
	t.Helper()

	ctx := context.Background()
	definition := fmt.Sprintf(`{"gates":[{"id":%q,"kind":"check","required":true,"criteria":{"command":"echo verified"}}]}`, gateID)
	if _, _, err := s.CreateGateTemplate(ctx, CreateGateTemplateParams{
		TemplateID:     templateID,
		Version:        1,
		AppliesTo:      []string{"task"},
		DefinitionJSON: definition,
		Actor:          "human:alice",
		CommandID:      commandPrefix + "-template-1",
	}); err != nil {
		t.Fatalf("create event-sourced gate template %s@1: %v", templateID, err)
	}
	gateSet, _, err := s.InstantiateGateSet(ctx, InstantiateGateSetParams{
		IssueID:      issueID,
		TemplateRefs: []string{templateID + "@1"},
		Actor:        "agent-1",
		CommandID:    commandPrefix + "-instantiate-1",
	})
	if err != nil {
		t.Fatalf("instantiate event-sourced gate set for %s: %v", issueID, err)
	}
	if _, _, err := s.LockGateSet(ctx, LockGateSetParams{
		IssueID:   issueID,
		Actor:     "agent-1",
		CommandID: commandPrefix + "-lock-1",
	}); err != nil {
		t.Fatalf("lock event-sourced gate set for %s: %v", issueID, err)
	}
	return gateSet.GateSetID
}

func sqliteSchemaObjectsForTest(t *testing.T, db *sql.DB) []string {
	t.Helper()

	rows, err := db.Query(`
		SELECT type || ':' || name
		FROM sqlite_master
		WHERE type IN ('table', 'index', 'trigger')
			AND name NOT LIKE 'sqlite_%'
		ORDER BY type ASC, name ASC
	`)
	if err != nil {
		t.Fatalf("query sqlite schema objects: %v", err)
	}
	defer rows.Close()

	items := make([]string, 0)
	for rows.Next() {
		var item string
		if err := rows.Scan(&item); err != nil {
			t.Fatalf("scan sqlite schema object: %v", err)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate sqlite schema objects: %v", err)
	}
	return items
}

func seedGateSetItemForTest(t *testing.T, s *Store, gateSetID, gateID, kind string, required int) {
	t.Helper()

	ctx := context.Background()
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO gate_set_items(gate_set_id, gate_id, kind, required, criteria_json)
		VALUES(?, ?, ?, ?, ?)
	`, gateSetID, gateID, kind, required, `{"ref":"test"}`)
	if err != nil {
		t.Fatalf("insert gate_set_item %s/%s: %v", gateSetID, gateID, err)
	}
}

func appendGateEvaluationEventForTest(
	t *testing.T,
	s *Store,
	issueID, gateSetID, gateID, result, actor, commandID string,
) {
	t.Helper()

	ctx := context.Background()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx for gate evaluation event: %v", err)
	}
	defer tx.Rollback()

	var gateSetHash string
	if err := tx.QueryRowContext(ctx, `SELECT gate_set_hash FROM gate_sets WHERE gate_set_id = ?`, gateSetID).Scan(&gateSetHash); err != nil {
		t.Fatalf("lookup gate_set_hash for %s: %v", gateSetID, err)
	}

	payloadJSON := fmt.Sprintf(
		`{"issue_id":%q,"gate_set_id":%q,"gate_id":%q,"result":%q,"evidence_refs":["test://evidence"],"proof":{"verifier":"test-verifier","runner":"unit-test","runner_version":"1","exit_code":0,"gate_set_hash":%q},"evaluated_at":%q}`,
		issueID, gateSetID, gateID, result, gateSetHash, nowUTC(),
	)
	res, err := s.appendEventTx(ctx, tx, appendEventRequest{
		EntityType:          entityTypeIssue,
		EntityID:            issueID,
		EventType:           eventTypeGateEval,
		PayloadJSON:         payloadJSON,
		Actor:               actor,
		CommandID:           commandID,
		EventPayloadVersion: 1,
	})
	if err != nil {
		t.Fatalf("append gate evaluation event: %v", err)
	}
	if res.AlreadyExists {
		t.Fatalf("expected non-idempotent append for unique command_id %q", commandID)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("commit gate evaluation event: %v", err)
	}
}

func appendGateEvaluationEventWithoutEvidenceForTest(
	t *testing.T,
	s *Store,
	issueID, gateSetID, gateID, result, actor, commandID string,
) {
	t.Helper()

	ctx := context.Background()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx for gate evaluation event: %v", err)
	}
	defer tx.Rollback()

	payloadJSON := fmt.Sprintf(
		`{"issue_id":%q,"gate_set_id":%q,"gate_id":%q,"result":%q,"evaluated_at":%q}`,
		issueID, gateSetID, gateID, result, nowUTC(),
	)
	res, err := s.appendEventTx(ctx, tx, appendEventRequest{
		EntityType:          entityTypeIssue,
		EntityID:            issueID,
		EventType:           eventTypeGateEval,
		PayloadJSON:         payloadJSON,
		Actor:               actor,
		CommandID:           commandID,
		EventPayloadVersion: 1,
	})
	if err != nil {
		t.Fatalf("append gate evaluation event: %v", err)
	}
	if res.AlreadyExists {
		t.Fatalf("expected non-idempotent append for unique command_id %q", commandID)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("commit gate evaluation event: %v", err)
	}
}

func appendGateEvaluationEventWithEvidenceNoProofForTest(
	t *testing.T,
	s *Store,
	issueID, gateSetID, gateID, result, actor, commandID string,
) {
	t.Helper()

	ctx := context.Background()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx for gate evaluation event: %v", err)
	}
	defer tx.Rollback()

	payloadJSON := fmt.Sprintf(
		`{"issue_id":%q,"gate_set_id":%q,"gate_id":%q,"result":%q,"evidence_refs":["test://evidence"],"evaluated_at":%q}`,
		issueID, gateSetID, gateID, result, nowUTC(),
	)
	res, err := s.appendEventTx(ctx, tx, appendEventRequest{
		EntityType:          entityTypeIssue,
		EntityID:            issueID,
		EventType:           eventTypeGateEval,
		PayloadJSON:         payloadJSON,
		Actor:               actor,
		CommandID:           commandID,
		EventPayloadVersion: 1,
	})
	if err != nil {
		t.Fatalf("append gate evaluation event: %v", err)
	}
	if res.AlreadyExists {
		t.Fatalf("expected non-idempotent append for unique command_id %q", commandID)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("commit gate evaluation event: %v", err)
	}
}

func assertIssueEqual(t *testing.T, expected, actual Issue) {
	t.Helper()
	if !reflect.DeepEqual(expected, actual) {
		t.Fatalf("issue mismatch\nexpected: %#v\nactual:   %#v", expected, actual)
	}
}
