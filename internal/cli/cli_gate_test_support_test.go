package cli

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/willbastian/memori/internal/store"
)

func seedGateCommandTestDB(t *testing.T) string {
	t.Helper()

	s, dbPath := newGateCommandTestStore(t, "memori-cli-gate.db")
	defer s.Close()

	ctx := context.Background()
	seedGateCommandIssue(t, s, ctx, "Gate command issue", "cmd-cli-gate-create-1", "cmd-cli-gate-progress-1")
	createGateTemplateForTest(
		t,
		s,
		ctx,
		"tmpl-default",
		1,
		[]string{"task"},
		`{"gates":[{"id":"build","criteria":{"command":"go test ./..."}},{"id":"lint","criteria":{"command":"golangci-lint run"}}]}`,
		"human:alice",
		"cmd-cli-gate-template-1",
		"create gate template",
	)
	insertGateSetForTest(
		t,
		s,
		ctx,
		"gs_cli_1",
		1,
		`["tmpl-default@1"]`,
		`{"gates":[{"id":"build"},{"id":"lint"}]}`,
		"gs_cli_hash_1",
		"2026-03-06T12:00:00Z",
		"2026-03-06T12:00:00Z",
		"test",
	)
	insertGateSetItemForTest(t, s, ctx, "gs_cli_1", "build", "check", 1, `{"command":"go test ./..."}`, "insert build gate set item")
	insertGateSetItemForTest(t, s, ctx, "gs_cli_1", "lint", "check", 0, `{"command":"golangci-lint run"}`, "insert lint gate set item")

	return dbPath
}

func seedGateCommandHistoricalCycle(t *testing.T, dbPath string) {
	t.Helper()

	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer s.Close()

	ctx := context.Background()
	createGateTemplateForTest(
		t,
		s,
		ctx,
		"tmpl-default",
		2,
		[]string{"task"},
		`{"gates":[{"id":"security","criteria":{"command":"go test ./..."}}]}`,
		"human:alice",
		"cmd-cli-gate-template-2",
		"create historical gate template",
	)
	insertGateSetForTest(
		t,
		s,
		ctx,
		"gs_cli_2",
		2,
		`["tmpl-default@2"]`,
		`{"gates":[{"id":"security"}]}`,
		"gs_cli_hash_2",
		"2026-03-06T13:00:00Z",
		"2026-03-06T13:00:00Z",
		"test",
	)
	insertGateSetItemForTest(t, s, ctx, "gs_cli_2", "security", "check", 1, `{"command":"go test ./..."}`, "insert historical gate set item")
}

func seedGateVerifyCommandTestDB(t *testing.T) string {
	t.Helper()
	return seedGateVerifyCommandTestDBWithCommand(t, "echo verified")
}

func seedGateVerifyCommandTestDBWithCommand(t *testing.T, command string) string {
	t.Helper()

	s, dbPath := newGateCommandTestStore(t, "memori-cli-gate-verify.db")
	defer s.Close()

	ctx := context.Background()
	seedGateCommandIssue(t, s, ctx, "Gate verify command issue", "cmd-cli-gate-verify-create-1", "cmd-cli-gate-verify-progress-1")

	criteriaJSON, definitionJSON := marshalGateCommandDefinitionForTest(t, command)
	createGateTemplateForTest(
		t,
		s,
		ctx,
		"tmpl-default",
		1,
		[]string{"task"},
		definitionJSON,
		"human:alice",
		"cmd-cli-gate-verify-template-1",
		"create gate verify template",
	)
	insertGateSetForTest(
		t,
		s,
		ctx,
		"gs_cli_verify_1",
		1,
		`["tmpl-default@1"]`,
		`{"gates":[{"id":"build"}]}`,
		"gs_cli_verify_hash_1",
		"2026-03-06T12:00:00Z",
		"2026-03-06T12:00:00Z",
		"test",
	)
	insertGateSetItemForTest(t, s, ctx, "gs_cli_verify_1", "build", "check", 1, criteriaJSON, "insert gate set item")

	return dbPath
}

func seedUnsafeGateVerifyCommandTestDB(t *testing.T, command string) string {
	t.Helper()

	s, dbPath := newGateCommandTestStore(t, "memori-cli-gate-verify-unsafe.db")
	defer s.Close()

	ctx := context.Background()
	seedGateCommandIssue(t, s, ctx, "Unsafe gate verify command issue", "cmd-cli-gate-unsafe-create-1", "cmd-cli-gate-unsafe-progress-1")

	criteriaJSON, definitionJSON := marshalGateCommandDefinitionForTest(t, command)
	createGateTemplateForTest(
		t,
		s,
		ctx,
		"tmpl-unsafe",
		1,
		[]string{"task"},
		definitionJSON,
		"llm:openai:gpt-5",
		"cmd-cli-gate-unsafe-template-1",
		"create unsafe gate template",
	)
	insertGateSetForTest(
		t,
		s,
		ctx,
		"gs_cli_verify_unsafe",
		1,
		`["tmpl-unsafe@1"]`,
		definitionJSON,
		"gs_cli_verify_unsafe_hash",
		"2026-03-06T12:00:00Z",
		"2026-03-06T12:00:00Z",
		"llm:openai:gpt-5",
	)
	insertGateSetItemForTest(t, s, ctx, "gs_cli_verify_unsafe", "build", "check", 1, criteriaJSON, "insert unsafe gate set item")

	return dbPath
}

func newGateCommandTestStore(t *testing.T, dbName string) (*store.Store, string) {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), dbName)
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if err := s.Initialize(context.Background(), store.InitializeParams{IssueKeyPrefix: "mem"}); err != nil {
		_ = s.Close()
		t.Fatalf("initialize store: %v", err)
	}

	return s, dbPath
}

func seedGateCommandIssue(t *testing.T, s *store.Store, ctx context.Context, title, createCommandID, progressCommandID string) {
	t.Helper()

	if _, _, _, err := s.CreateIssue(ctx, store.CreateIssueParams{
		IssueID:   "mem-c111111",
		Type:      "task",
		Title:     title,
		Actor:     "test",
		CommandID: createCommandID,
	}); err != nil {
		t.Fatalf("create issue: %v", err)
	}
	if _, _, _, err := s.UpdateIssueStatus(ctx, store.UpdateIssueStatusParams{
		IssueID:   "mem-c111111",
		Status:    "inprogress",
		Actor:     "test",
		CommandID: progressCommandID,
	}); err != nil {
		t.Fatalf("move issue to inprogress: %v", err)
	}
}

func createGateTemplateForTest(
	t *testing.T,
	s *store.Store,
	ctx context.Context,
	templateID string,
	version int,
	appliesTo []string,
	definitionJSON string,
	actor string,
	commandID string,
	failurePrefix string,
) {
	t.Helper()

	if _, _, err := s.CreateGateTemplate(ctx, store.CreateGateTemplateParams{
		TemplateID:     templateID,
		Version:        version,
		AppliesTo:      appliesTo,
		DefinitionJSON: definitionJSON,
		Actor:          actor,
		CommandID:      commandID,
	}); err != nil {
		t.Fatalf("%s: %v", failurePrefix, err)
	}
}

func insertGateSetForTest(
	t *testing.T,
	s *store.Store,
	ctx context.Context,
	gateSetID string,
	cycleNo int,
	templateRefsJSON string,
	frozenDefinitionJSON string,
	gateSetHash string,
	lockedAt string,
	createdAt string,
	createdBy string,
) {
	t.Helper()

	if _, err := s.DB().ExecContext(ctx, `
		INSERT INTO gate_sets(
			gate_set_id, issue_id, cycle_no, template_refs_json, frozen_definition_json,
			gate_set_hash, locked_at, created_at, created_by
		) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, gateSetID, "mem-c111111", cycleNo, templateRefsJSON, frozenDefinitionJSON, gateSetHash, lockedAt, createdAt, createdBy); err != nil {
		t.Fatalf("insert gate set: %v", err)
	}
}

func insertGateSetItemForTest(
	t *testing.T,
	s *store.Store,
	ctx context.Context,
	gateSetID string,
	gateID string,
	kind string,
	required int,
	criteriaJSON string,
	failurePrefix string,
) {
	t.Helper()

	if _, err := s.DB().ExecContext(ctx, `
		INSERT INTO gate_set_items(gate_set_id, gate_id, kind, required, criteria_json)
		VALUES(?, ?, ?, ?, ?)
	`, gateSetID, gateID, kind, required, criteriaJSON); err != nil {
		t.Fatalf("%s: %v", failurePrefix, err)
	}
}

func marshalGateCommandDefinitionForTest(t *testing.T, command string) (criteriaJSON string, definitionJSON string) {
	t.Helper()

	criteriaBytes, err := json.Marshal(map[string]string{"command": command})
	if err != nil {
		t.Fatalf("marshal gate verification criteria: %v", err)
	}
	definitionBytes, err := json.Marshal(map[string]any{
		"gates": []map[string]any{
			{
				"id":       "build",
				"criteria": map[string]string{"command": command},
			},
		},
	})
	if err != nil {
		t.Fatalf("marshal gate verification template definition: %v", err)
	}

	return string(criteriaBytes), string(definitionBytes)
}
