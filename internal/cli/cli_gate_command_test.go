package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"memori/internal/store"
)

type gateEvaluateEnvelope struct {
	Command string `json:"command"`
	Data    struct {
		Evaluation store.GateEvaluation `json:"evaluation"`
		Idempotent bool                 `json:"idempotent"`
	} `json:"data"`
}

type gateStatusEnvelope struct {
	Command string `json:"command"`
	Data    struct {
		Status store.GateStatus `json:"status"`
	} `json:"data"`
}

type gateVerifyEnvelope struct {
	Command string `json:"command"`
	Data    struct {
		Evaluation store.GateEvaluation `json:"evaluation"`
		Command    string               `json:"command"`
		ExitCode   int                  `json:"exit_code"`
		OutputSHA  string               `json:"output_sha256"`
		Idempotent bool                 `json:"idempotent"`
	} `json:"data"`
}

func TestGateEvaluateAndStatusJSON(t *testing.T) {
	t.Parallel()

	dbPath := seedGateCommandTestDB(t)

	stdout, stderr, err := runMemoriForTest(
		"gate", "evaluate",
		"--db", dbPath,
		"--issue", "mem-c111111",
		"--gate", "build",
		"--result", "pass",
		"--evidence", "ci://run/123",
		"--command-id", "cmd-cli-gate-eval-1",
		"--json",
	)
	if err != nil {
		t.Fatalf("run gate evaluate command: %v\nstderr: %s", err, stderr)
	}

	var evalResp gateEvaluateEnvelope
	if err := json.Unmarshal([]byte(stdout), &evalResp); err != nil {
		t.Fatalf("decode gate evaluate json output: %v\nstdout: %s", err, stdout)
	}
	if evalResp.Command != "gate evaluate" {
		t.Fatalf("expected gate evaluate command, got %q", evalResp.Command)
	}
	if evalResp.Data.Idempotent {
		t.Fatalf("expected first gate evaluation to be non-idempotent")
	}
	if evalResp.Data.Evaluation.Result != "PASS" {
		t.Fatalf("expected normalized PASS result, got %q", evalResp.Data.Evaluation.Result)
	}

	stdout, stderr, err = runMemoriForTest(
		"gate", "status",
		"--db", dbPath,
		"--issue", "mem-c111111",
		"--json",
	)
	if err != nil {
		t.Fatalf("run gate status command: %v\nstderr: %s", err, stderr)
	}

	var statusResp gateStatusEnvelope
	if err := json.Unmarshal([]byte(stdout), &statusResp); err != nil {
		t.Fatalf("decode gate status json output: %v\nstdout: %s", err, stdout)
	}
	if statusResp.Command != "gate status" {
		t.Fatalf("expected gate status command, got %q", statusResp.Command)
	}
	if len(statusResp.Data.Status.Gates) != 2 {
		t.Fatalf("expected 2 gate status rows, got %d", len(statusResp.Data.Status.Gates))
	}

	var (
		buildResult string
		lintResult  string
	)
	for _, gate := range statusResp.Data.Status.Gates {
		switch gate.GateID {
		case "build":
			buildResult = gate.Result
		case "lint":
			lintResult = gate.Result
		}
	}
	if buildResult != "PASS" {
		t.Fatalf("expected build gate PASS, got %q", buildResult)
	}
	if lintResult != "MISSING" {
		t.Fatalf("expected lint gate MISSING, got %q", lintResult)
	}
}

func TestGateStatusHumanOutputShowsResults(t *testing.T) {
	t.Parallel()

	dbPath := seedGateCommandTestDB(t)

	if _, stderr, err := runMemoriForTest(
		"gate", "evaluate",
		"--db", dbPath,
		"--issue", "mem-c111111",
		"--gate", "build",
		"--result", "FAIL",
		"--evidence", "ci://run/456",
		"--command-id", "cmd-cli-gate-eval-human-1",
	); err != nil {
		t.Fatalf("run gate evaluate command: %v\nstderr: %s", err, stderr)
	}

	stdout, stderr, err := runMemoriForTest(
		"gate", "status",
		"--db", dbPath,
		"--issue", "mem-c111111",
	)
	if err != nil {
		t.Fatalf("run gate status command: %v\nstderr: %s", err, stderr)
	}
	if !strings.Contains(stdout, "build [required/check] FAIL") {
		t.Fatalf("expected FAIL build row in gate status output, got:\n%s", stdout)
	}
	if !strings.Contains(stdout, "lint [optional/check] MISSING") {
		t.Fatalf("expected MISSING lint row in gate status output, got:\n%s", stdout)
	}
}

func TestGateVerifyExecutesCriteriaCommandAndRecordsProof(t *testing.T) {
	t.Parallel()

	dbPath := seedGateVerifyCommandTestDB(t)

	stdout, stderr, err := runMemoriForTest(
		"gate", "verify",
		"--db", dbPath,
		"--issue", "mem-c111111",
		"--gate", "build",
		"--command-id", "cmd-cli-gate-verify-1",
		"--json",
	)
	if err != nil {
		t.Fatalf("run gate verify command: %v\nstderr: %s", err, stderr)
	}

	var verifyResp gateVerifyEnvelope
	if err := json.Unmarshal([]byte(stdout), &verifyResp); err != nil {
		t.Fatalf("decode gate verify json output: %v\nstdout: %s", err, stdout)
	}
	if verifyResp.Command != "gate verify" {
		t.Fatalf("expected gate verify command, got %q", verifyResp.Command)
	}
	if verifyResp.Data.ExitCode != 0 {
		t.Fatalf("expected exit code 0, got %d", verifyResp.Data.ExitCode)
	}
	if verifyResp.Data.Idempotent {
		t.Fatalf("expected first gate verify to be non-idempotent")
	}
	if verifyResp.Data.Evaluation.Result != "PASS" {
		t.Fatalf("expected PASS from verifier execution, got %q", verifyResp.Data.Evaluation.Result)
	}
	if verifyResp.Data.Evaluation.Proof == nil || verifyResp.Data.Evaluation.Proof.GateSetHash == "" {
		t.Fatalf("expected proof payload with gate_set_hash, got %#v", verifyResp.Data.Evaluation.Proof)
	}
	if verifyResp.Data.OutputSHA == "" {
		t.Fatalf("expected non-empty output hash from verifier command")
	}
}

func TestGateVerifyIdempotentRetryReplaysPersistedResultWithoutRerunningCommand(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	counterPath := filepath.Join(tempDir, "verify-count.txt")
	command := fmt.Sprintf(
		`count_file=%q; count=0; if [ -f "$count_file" ]; then count=$(wc -c < "$count_file"); fi; next=$((count+1)); printf x >> "$count_file"; echo run-$next`,
		counterPath,
	)
	dbPath := seedGateVerifyCommandTestDBWithCommand(t, command)

	stdout, stderr, err := runMemoriForTest(
		"gate", "verify",
		"--db", dbPath,
		"--issue", "mem-c111111",
		"--gate", "build",
		"--command-id", "cmd-cli-gate-verify-idempotent-1",
		"--json",
	)
	if err != nil {
		t.Fatalf("first gate verify: %v\nstderr: %s", err, stderr)
	}

	var firstResp gateVerifyEnvelope
	if err := json.Unmarshal([]byte(stdout), &firstResp); err != nil {
		t.Fatalf("decode first gate verify json output: %v\nstdout: %s", err, stdout)
	}
	if firstResp.Data.Idempotent {
		t.Fatalf("expected first gate verify to be non-idempotent")
	}

	counterBytes, err := os.ReadFile(counterPath)
	if err != nil {
		t.Fatalf("read verifier counter after first run: %v", err)
	}
	if got := len(counterBytes); got != 1 {
		t.Fatalf("expected verifier command to run once, counter size=%d", got)
	}

	stdout, stderr, err = runMemoriForTest(
		"gate", "verify",
		"--db", dbPath,
		"--issue", "mem-c111111",
		"--gate", "build",
		"--command-id", "cmd-cli-gate-verify-idempotent-1",
		"--json",
	)
	if err != nil {
		t.Fatalf("second gate verify: %v\nstderr: %s", err, stderr)
	}

	var retryResp gateVerifyEnvelope
	if err := json.Unmarshal([]byte(stdout), &retryResp); err != nil {
		t.Fatalf("decode retry gate verify json output: %v\nstdout: %s", err, stdout)
	}
	if !retryResp.Data.Idempotent {
		t.Fatalf("expected idempotent retry response")
	}
	if retryResp.Data.ExitCode != firstResp.Data.ExitCode {
		t.Fatalf("expected replayed exit code %d, got %d", firstResp.Data.ExitCode, retryResp.Data.ExitCode)
	}
	if retryResp.Data.OutputSHA != firstResp.Data.OutputSHA {
		t.Fatalf("expected replayed output sha %q, got %q", firstResp.Data.OutputSHA, retryResp.Data.OutputSHA)
	}
	if retryResp.Data.Command != firstResp.Data.Command {
		t.Fatalf("expected replayed command %q, got %q", firstResp.Data.Command, retryResp.Data.Command)
	}

	counterBytes, err = os.ReadFile(counterPath)
	if err != nil {
		t.Fatalf("read verifier counter after retry: %v", err)
	}
	if got := len(counterBytes); got != 1 {
		t.Fatalf("expected verifier command not to rerun on idempotent retry, counter size=%d", got)
	}
}

func TestGateStatusSupportsCycleFlag(t *testing.T) {
	t.Parallel()

	dbPath := seedGateCommandTestDB(t)
	seedGateCommandHistoricalCycle(t, dbPath)

	stdout, stderr, err := runMemoriForTest(
		"gate", "status",
		"--db", dbPath,
		"--issue", "mem-c111111",
		"--cycle", "2",
		"--json",
	)
	if err != nil {
		t.Fatalf("run gate status --cycle command: %v\nstderr: %s", err, stderr)
	}

	var statusResp gateStatusEnvelope
	if err := json.Unmarshal([]byte(stdout), &statusResp); err != nil {
		t.Fatalf("decode gate status --cycle json output: %v\nstdout: %s", err, stdout)
	}
	if statusResp.Data.Status.CycleNo != 2 {
		t.Fatalf("expected cycle 2 status, got %d", statusResp.Data.Status.CycleNo)
	}
	if statusResp.Data.Status.GateSetID != "gs_cli_2" {
		t.Fatalf("expected gate_set_id gs_cli_2, got %q", statusResp.Data.Status.GateSetID)
	}
	if len(statusResp.Data.Status.Gates) != 1 || statusResp.Data.Status.Gates[0].GateID != "security" {
		t.Fatalf("expected security gate for cycle 2, got %#v", statusResp.Data.Status.Gates)
	}
}

func seedGateCommandTestDB(t *testing.T) string {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), "memori-cli-gate.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer s.Close()

	ctx := context.Background()
	if err := s.Initialize(ctx, store.InitializeParams{IssueKeyPrefix: "mem"}); err != nil {
		t.Fatalf("initialize store: %v", err)
	}
	if _, _, _, err := s.CreateIssue(ctx, store.CreateIssueParams{
		IssueID:   "mem-c111111",
		Type:      "task",
		Title:     "Gate command issue",
		Actor:     "test",
		CommandID: "cmd-cli-gate-create-1",
	}); err != nil {
		t.Fatalf("create issue: %v", err)
	}
	if _, _, _, err := s.UpdateIssueStatus(ctx, store.UpdateIssueStatusParams{
		IssueID:   "mem-c111111",
		Status:    "inprogress",
		Actor:     "test",
		CommandID: "cmd-cli-gate-progress-1",
	}); err != nil {
		t.Fatalf("move issue to inprogress: %v", err)
	}

	_, err = s.DB().ExecContext(ctx, `
		INSERT INTO gate_sets(
			gate_set_id, issue_id, cycle_no, template_refs_json, frozen_definition_json,
			gate_set_hash, locked_at, created_at, created_by
		) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, "gs_cli_1", "mem-c111111", 1, `["tmpl-default@1"]`, `{"gates":[{"id":"build"},{"id":"lint"}]}`, "gs_cli_hash_1", "2026-03-06T12:00:00Z", "2026-03-06T12:00:00Z", "test")
	if err != nil {
		t.Fatalf("insert gate set: %v", err)
	}
	_, err = s.DB().ExecContext(ctx, `
		INSERT INTO gate_set_items(gate_set_id, gate_id, kind, required, criteria_json)
		VALUES
			('gs_cli_1', 'build', 'check', 1, '{"command":"go test ./..."}'),
			('gs_cli_1', 'lint', 'check', 0, '{"command":"golangci-lint run"}')
	`)
	if err != nil {
		t.Fatalf("insert gate set items: %v", err)
	}

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
	_, err = s.DB().ExecContext(ctx, `
		INSERT INTO gate_sets(
			gate_set_id, issue_id, cycle_no, template_refs_json, frozen_definition_json,
			gate_set_hash, locked_at, created_at, created_by
		) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, "gs_cli_2", "mem-c111111", 2, `["tmpl-default@2"]`, `{"gates":[{"id":"security"}]}`, "gs_cli_hash_2", "2026-03-06T13:00:00Z", "2026-03-06T13:00:00Z", "test")
	if err != nil {
		t.Fatalf("insert historical gate set: %v", err)
	}
	_, err = s.DB().ExecContext(ctx, `
		INSERT INTO gate_set_items(gate_set_id, gate_id, kind, required, criteria_json)
		VALUES('gs_cli_2', 'security', 'check', 1, '{"command":"go test ./..."}')
	`)
	if err != nil {
		t.Fatalf("insert historical gate set item: %v", err)
	}
}

func seedGateVerifyCommandTestDB(t *testing.T) string {
	t.Helper()
	return seedGateVerifyCommandTestDBWithCommand(t, "echo verified")
}

func seedGateVerifyCommandTestDBWithCommand(t *testing.T, command string) string {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), "memori-cli-gate-verify.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer s.Close()

	ctx := context.Background()
	if err := s.Initialize(ctx, store.InitializeParams{IssueKeyPrefix: "mem"}); err != nil {
		t.Fatalf("initialize store: %v", err)
	}
	if _, _, _, err := s.CreateIssue(ctx, store.CreateIssueParams{
		IssueID:   "mem-c111111",
		Type:      "task",
		Title:     "Gate verify command issue",
		Actor:     "test",
		CommandID: "cmd-cli-gate-verify-create-1",
	}); err != nil {
		t.Fatalf("create issue: %v", err)
	}
	if _, _, _, err := s.UpdateIssueStatus(ctx, store.UpdateIssueStatusParams{
		IssueID:   "mem-c111111",
		Status:    "inprogress",
		Actor:     "test",
		CommandID: "cmd-cli-gate-verify-progress-1",
	}); err != nil {
		t.Fatalf("move issue to inprogress: %v", err)
	}
	_, err = s.DB().ExecContext(ctx, `
		INSERT INTO gate_sets(
			gate_set_id, issue_id, cycle_no, template_refs_json, frozen_definition_json,
			gate_set_hash, locked_at, created_at, created_by
		) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, "gs_cli_verify_1", "mem-c111111", 1, `["tmpl-default@1"]`, `{"gates":[{"id":"build"}]}`, "gs_cli_verify_hash_1", "2026-03-06T12:00:00Z", "2026-03-06T12:00:00Z", "test")
	if err != nil {
		t.Fatalf("insert gate set: %v", err)
	}
	criteriaJSON, err := json.Marshal(map[string]string{"command": command})
	if err != nil {
		t.Fatalf("marshal gate verification criteria: %v", err)
	}
	_, err = s.DB().ExecContext(ctx, `
		INSERT INTO gate_set_items(gate_set_id, gate_id, kind, required, criteria_json)
		VALUES(?, ?, ?, ?, ?)
	`, "gs_cli_verify_1", "build", "check", 1, string(criteriaJSON))
	if err != nil {
		t.Fatalf("insert gate set item: %v", err)
	}
	return dbPath
}
