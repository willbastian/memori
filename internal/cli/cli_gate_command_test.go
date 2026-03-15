package cli

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/willbastian/memori/internal/store"
)

type gateEvaluateEnvelope struct {
	ResponseSchemaVersion int    `json:"response_schema_version"`
	DBSchemaVersion       int    `json:"db_schema_version"`
	Command               string `json:"command"`
	Data                  struct {
		Evaluation store.GateEvaluation `json:"evaluation"`
		Idempotent bool                 `json:"idempotent"`
	} `json:"data"`
}

type gateStatusEnvelope struct {
	ResponseSchemaVersion int    `json:"response_schema_version"`
	DBSchemaVersion       int    `json:"db_schema_version"`
	Command               string `json:"command"`
	Data                  struct {
		Status store.GateStatus `json:"status"`
	} `json:"data"`
}

type gateVerifyEnvelope struct {
	ResponseSchemaVersion int    `json:"response_schema_version"`
	DBSchemaVersion       int    `json:"db_schema_version"`
	Command               string `json:"command"`
	Data                  struct {
		Evaluation store.GateEvaluation `json:"evaluation"`
		Event      store.Event          `json:"event"`
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
		"--result", "fail",
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
	assertEnvelopeMetadata(t, evalResp.ResponseSchemaVersion, evalResp.DBSchemaVersion)
	if evalResp.Command != "gate evaluate" {
		t.Fatalf("expected gate evaluate command, got %q", evalResp.Command)
	}
	if evalResp.Data.Idempotent {
		t.Fatalf("expected first gate evaluation to be non-idempotent")
	}
	if evalResp.Data.Evaluation.Result != "FAIL" {
		t.Fatalf("expected normalized FAIL result, got %q", evalResp.Data.Evaluation.Result)
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
	assertEnvelopeMetadata(t, statusResp.ResponseSchemaVersion, statusResp.DBSchemaVersion)
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
	if buildResult != "FAIL" {
		t.Fatalf("expected build gate FAIL, got %q", buildResult)
	}
	if lintResult != "MISSING" {
		t.Fatalf("expected lint gate MISSING, got %q", lintResult)
	}
}

func TestGateEvaluateRejectsPASSForExecutableGate(t *testing.T) {
	t.Parallel()

	dbPath := seedGateCommandTestDB(t)

	_, _, err := runMemoriForTest(
		"gate", "evaluate",
		"--db", dbPath,
		"--issue", "mem-c111111",
		"--gate", "build",
		"--result", "PASS",
		"--evidence", "ci://run/pass-without-proof",
		"--command-id", "cmd-cli-gate-eval-pass-without-proof-1",
		"--json",
	)
	if err == nil || !strings.Contains(err.Error(), "use memori gate verify") {
		t.Fatalf("expected executable PASS rejection directing user to gate verify, got: %v", err)
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
	assertEnvelopeMetadata(t, verifyResp.ResponseSchemaVersion, verifyResp.DBSchemaVersion)
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
	assertEnvelopeMetadata(t, firstResp.ResponseSchemaVersion, firstResp.DBSchemaVersion)
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
	assertEnvelopeMetadata(t, retryResp.ResponseSchemaVersion, retryResp.DBSchemaVersion)
	if !retryResp.Data.Idempotent {
		t.Fatalf("expected idempotent retry response")
	}
	if retryResp.Data.Event.EventID != firstResp.Data.Event.EventID {
		t.Fatalf("expected replay to return original event_id %q, got %q", firstResp.Data.Event.EventID, retryResp.Data.Event.EventID)
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

func TestGateVerifyRejectsExecutableCommandFromNonHumanTemplate(t *testing.T) {
	t.Parallel()

	dbPath := seedUnsafeGateVerifyCommandTestDB(t, "echo unsafe")

	_, _, err := runMemoriForTest(
		"gate", "verify",
		"--db", dbPath,
		"--issue", "mem-c111111",
		"--gate", "build",
		"--command-id", "cmd-cli-gate-verify-unsafe-1",
		"--json",
	)
	if err == nil || !strings.Contains(err.Error(), "unapproved template") {
		t.Fatalf("expected governance rejection for unsafe executable template, got: %v", err)
	}
}

func TestGateVerifyHumanOutputReportsFailingVerifierExitCode(t *testing.T) {
	t.Parallel()

	dbPath := seedGateVerifyCommandTestDBWithCommand(t, "echo verifier-failed && exit 7")

	stdout, stderr, err := runMemoriForTest(
		"gate", "verify",
		"--db", dbPath,
		"--issue", "mem-c111111",
		"--gate", "build",
		"--command-id", "cmd-cli-gate-verify-fail-text-1",
	)
	if err != nil {
		t.Fatalf("run failing gate verify command: %v\nstderr: %s", err, stderr)
	}
	for _, want := range []string{
		"Verified gate build for issue mem-c111111 -> FAIL (exit=7)",
		"Output SHA256:",
	} {
		if !strings.Contains(stdout, want) {
			t.Fatalf("expected failing gate verify output to contain %q, got:\n%s", want, stdout)
		}
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
