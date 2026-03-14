package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"github.com/willbastian/memori/internal/dbschema"
)

func TestCreateIssueKeyPolicyValidation(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	_, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   "task-a1b2c3d",
		Type:      "task",
		Title:     "Invalid key prefix",
		Actor:     "agent-1",
		CommandID: "cmd-key-1",
	})
	if err == nil || !strings.Contains(err.Error(), "type must be in --type") {
		t.Fatalf("expected type-in-prefix validation error, got: %v", err)
	}

	_, _, _, err = s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   "mem-nothexx",
		Type:      "task",
		Title:     "Invalid short sha",
		Actor:     "agent-1",
		CommandID: "cmd-key-2",
	})
	if err == nil || !strings.Contains(err.Error(), "shortSHA must be hex") {
		t.Fatalf("expected shortSHA validation error, got: %v", err)
	}
}

func TestCreateIssueGeneratedKeysFollowPrefixShortSHAPattern(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStoreWithPrefix(t, "ops")

	issue, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		Type:      "task",
		Title:     "Generated key",
		Actor:     "agent-1",
		CommandID: "cmd-key-3",
	})
	if err != nil {
		t.Fatalf("create with generated key: %v", err)
	}

	pattern := regexp.MustCompile(`^ops-[0-9a-f]{7}$`)
	if !pattern.MatchString(issue.ID) {
		t.Fatalf("expected generated key to match ops-shortSHA, got %q", issue.ID)
	}
}

func TestCreateIssuePersistsRichContextFields(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	created, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:            "mem-0f0f0f0",
		Type:               "task",
		Title:              "Rich context",
		Description:        "Implement richer ticket context rendering",
		AcceptanceCriteria: "Shows description and acceptance criteria in issue show",
		References:         []string{"https://example.com/spec", " https://example.com/spec ", "notes.md"},
		Actor:              "agent-1",
		CommandID:          "cmd-rich-create-1",
	})
	if err != nil {
		t.Fatalf("create issue with rich context: %v", err)
	}

	if created.Description != "Implement richer ticket context rendering" {
		t.Fatalf("unexpected description: %q", created.Description)
	}
	if created.Acceptance != "Shows description and acceptance criteria in issue show" {
		t.Fatalf("unexpected acceptance criteria: %q", created.Acceptance)
	}
	expectedRefs := []string{"https://example.com/spec", "notes.md"}
	if !reflect.DeepEqual(created.References, expectedRefs) {
		t.Fatalf("unexpected references: %#v", created.References)
	}
}

func TestUpdateIssueAllowsContextOnlyMutation(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	_, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   "mem-1f1f1f1",
		Type:      "task",
		Title:     "Context updates",
		Actor:     "agent-1",
		CommandID: "cmd-rich-update-create-1",
	})
	if err != nil {
		t.Fatalf("create issue: %v", err)
	}

	title := "Context updates renamed"
	description := "Track rich context as first-class metadata"
	acceptance := "Issue show surfaces context fields"
	references := []string{"https://example.com/rfc", "adr-001.md"}
	updated, event, idempotent, err := s.UpdateIssue(ctx, UpdateIssueParams{
		IssueID:            "mem-1f1f1f1",
		Title:              &title,
		Description:        &description,
		AcceptanceCriteria: &acceptance,
		References:         &references,
		Actor:              "agent-1",
		CommandID:          "cmd-rich-update-1",
	})
	if err != nil {
		t.Fatalf("update issue context: %v", err)
	}
	if idempotent {
		t.Fatalf("first context update should not be idempotent")
	}
	if event.EventType != "issue.updated" {
		t.Fatalf("expected issue.updated event, got %s", event.EventType)
	}
	if updated.Status != "Todo" {
		t.Fatalf("status should remain Todo when only context changes, got %s", updated.Status)
	}
	if updated.Title != title {
		t.Fatalf("unexpected title: %q", updated.Title)
	}
	if updated.Description != description {
		t.Fatalf("unexpected description: %q", updated.Description)
	}
	if updated.Acceptance != acceptance {
		t.Fatalf("unexpected acceptance criteria: %q", updated.Acceptance)
	}
	if !reflect.DeepEqual(updated.References, references) {
		t.Fatalf("unexpected references: %#v", updated.References)
	}
}

func TestUpdateIssueRejectsBlankTitle(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	_, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   "mem-2f2f2f2",
		Type:      "task",
		Title:     "Valid title",
		Actor:     "agent-1",
		CommandID: "cmd-blank-title-create-1",
	})
	if err != nil {
		t.Fatalf("create issue: %v", err)
	}

	blank := "   "
	if _, _, _, err := s.UpdateIssue(ctx, UpdateIssueParams{
		IssueID:   "mem-2f2f2f2",
		Title:     &blank,
		Actor:     "agent-1",
		CommandID: "cmd-blank-title-update-1",
	}); err == nil || !strings.Contains(err.Error(), "--title is required") {
		t.Fatalf("expected blank title validation error, got: %v", err)
	}
}

func TestUpdateIssueStatusValidTransitionsAndIdempotency(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := newTestStore(t)

	created, _, _, err := s.CreateIssue(ctx, CreateIssueParams{
		IssueID:   "mem-1111111",
		Type:      "task",
		Title:     "Status transition test",
		Actor:     "agent-1",
		CommandID: "cmd-update-create-1",
	})
	if err != nil {
		t.Fatalf("create issue: %v", err)
	}
	if created.Status != "Todo" {
		t.Fatalf("expected initial status Todo, got %s", created.Status)
	}

	updated, event, idempotent, err := s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
		IssueID:   "mem-1111111",
		Status:    "inprogress",
		Actor:     "agent-1",
		CommandID: "cmd-update-1",
	})
	if err != nil {
		t.Fatalf("update issue status: %v", err)
	}
	if idempotent {
		t.Fatalf("first update should not be idempotent")
	}
	if updated.Status != "InProgress" {
		t.Fatalf("expected status InProgress, got %s", updated.Status)
	}
	if event.EventType != "issue.updated" {
		t.Fatalf("expected issue.updated event, got %s", event.EventType)
	}

	retryIssue, retryEvent, retryIdempotent, err := s.UpdateIssueStatus(ctx, UpdateIssueStatusParams{
		IssueID:   "mem-1111111",
		Status:    "done",
		Actor:     "agent-1",
		CommandID: "cmd-update-1",
	})
	if err != nil {
		t.Fatalf("retry update with same command id should succeed idempotently: %v", err)
	}
	if !retryIdempotent {
		t.Fatalf("expected idempotent retry")
	}
	if retryEvent.EventID != event.EventID {
		t.Fatalf("expected same event id on retry, got %s vs %s", retryEvent.EventID, event.EventID)
	}
	if retryIssue.Status != "InProgress" {
		t.Fatalf("expected status to remain InProgress on idempotent retry, got %s", retryIssue.Status)
	}
}

func TestInitializeMatchesMigratedSchema(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	initStore := newTestStore(t)
	initSchema := sqliteSchemaObjectsForTest(t, initStore.DB())

	migratedPath := filepath.Join(t.TempDir(), "memori-migrated.db")
	migratedStore, err := Open(migratedPath)
	if err != nil {
		t.Fatalf("open migrated store: %v", err)
	}
	t.Cleanup(func() {
		_ = migratedStore.Close()
	})
	if _, err := dbschema.Migrate(ctx, migratedStore.DB(), nil); err != nil {
		t.Fatalf("migrate schema: %v", err)
	}
	migratedSchema := sqliteSchemaObjectsForTest(t, migratedStore.DB())

	if !reflect.DeepEqual(initSchema, migratedSchema) {
		t.Fatalf("expected Initialize schema to match migrated schema\ninit=%v\nmigrate=%v", initSchema, migratedSchema)
	}
}

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
