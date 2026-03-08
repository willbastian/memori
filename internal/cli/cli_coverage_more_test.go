package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/willbastian/memori/internal/dbschema"
	"github.com/willbastian/memori/internal/store"
)

func TestRunInitTextOutputIncludesNextSteps(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "memori-cli-init-text.db")

	var out bytes.Buffer
	if err := runInit([]string{"--db", dbPath, "--issue-prefix", "wrk"}, &out); err != nil {
		t.Fatalf("run init text output: %v", err)
	}

	rendered := out.String()
	for _, want := range []string{
		"OK Initialized memori database",
		"DB Path: " + dbPath,
		"Schema: v",
		"Issue Prefix: wrk",
		"Next:",
		"memori auth set-password --db " + dbPath,
		`memori issue create --type task --title "First ticket"`,
		"memori backlog",
	} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("expected init output to contain %q, got:\n%s", want, rendered)
		}
	}
}

func TestRunAuthDispatchesStatusAndRejectsInvalidSubcommands(t *testing.T) {
	t.Parallel()

	s, dbPath := newCLIAuthTestStore(t)
	if err := s.Close(); err != nil {
		t.Fatalf("close auth store: %v", err)
	}

	var out bytes.Buffer
	if err := runAuth([]string{"status", "--db", dbPath}, &out); err != nil {
		t.Fatalf("dispatch auth status: %v", err)
	}
	if !strings.Contains(out.String(), "Human auth: not configured") {
		t.Fatalf("expected auth status output, got:\n%s", out.String())
	}

	if err := runAuth(nil, io.Discard); err == nil || !strings.Contains(err.Error(), "auth subcommand required") {
		t.Fatalf("expected missing auth subcommand error, got %v", err)
	}

	if err := runAuth([]string{"rotate"}, io.Discard); err == nil || !strings.Contains(err.Error(), `unknown auth subcommand "rotate"`) {
		t.Fatalf("expected unknown auth subcommand error, got %v", err)
	}
}

func TestRunIssueShowTextOutputIncludesOptionalFields(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "memori-cli-issue-show-text.db")
	s := openInitializedCLIStoreForCoverage(t, dbPath)
	defer s.Close()

	ctx := context.Background()
	if _, _, _, err := s.CreateIssue(ctx, store.CreateIssueParams{
		IssueID:   "mem-b2c3d4e",
		Type:      "story",
		Title:     "Parent story",
		Actor:     "test",
		CommandID: "cmd-cli-coverage-parent-1",
	}); err != nil {
		t.Fatalf("create parent issue: %v", err)
	}
	if _, _, _, err := s.CreateIssue(ctx, store.CreateIssueParams{
		IssueID:            "mem-a1b2c3d",
		Type:               "task",
		Title:              "Detailed child issue",
		ParentID:           "mem-b2c3d4e",
		Description:        "Document every optional field shown by issue show.",
		AcceptanceCriteria: "Text output keeps description, acceptance, and references visible.",
		References:         []string{"docs/coverage.md", "internal/cli/cli.go"},
		Actor:              "test",
		CommandID:          "cmd-cli-coverage-child-1",
	}); err != nil {
		t.Fatalf("create child issue: %v", err)
	}

	priority := "P1"
	labels := []string{"cli", "coverage"}
	if _, _, _, err := s.UpdateIssue(ctx, store.UpdateIssueParams{
		IssueID:   "mem-a1b2c3d",
		Priority:  &priority,
		Labels:    &labels,
		Actor:     "test",
		CommandID: "cmd-cli-coverage-child-update-1",
	}); err != nil {
		t.Fatalf("update child issue: %v", err)
	}

	var out bytes.Buffer
	if err := runIssueShow([]string{"--db", dbPath, "--key", "mem-a1b2c3d"}, &out); err != nil {
		t.Fatalf("run issue show text: %v", err)
	}

	rendered := out.String()
	for _, want := range []string{
		"mem-a1b2c3d [Task/Todo]",
		"Title: Detailed child issue",
		"Parent: mem-b2c3d4e",
		"Priority: P1",
		"Labels: cli, coverage",
		"Description:",
		"Document every optional field shown by issue show.",
		"Acceptance Criteria:",
		"Text output keeps description, acceptance, and references visible.",
		"References:",
		"- docs/coverage.md",
		"- internal/cli/cli.go",
		"Timeline:",
		"Created:",
		"Updated:",
	} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("expected issue show output to contain %q, got:\n%s", want, rendered)
		}
	}
}

func TestRunContextLoopsValidatesCycleAndPrintsLoopDetails(t *testing.T) {
	t.Parallel()

	dbPath := seedGateCommandTestDB(t)

	if _, stderr, err := runMemoriForTest(
		"gate", "evaluate",
		"--db", dbPath,
		"--issue", "mem-c111111",
		"--gate", "build",
		"--result", "FAIL",
		"--evidence", "ci://run/context-loops-1",
		"--command-id", "cmd-cli-context-loops-evaluate-1",
	); err != nil {
		t.Fatalf("create open loop: %v\nstderr: %s", err, stderr)
	}

	if err := runContextLoops([]string{"--db", dbPath, "--cycle", "0"}, io.Discard); err == nil || !strings.Contains(err.Error(), "--cycle must be > 0") {
		t.Fatalf("expected invalid cycle error, got %v", err)
	}

	var out bytes.Buffer
	if err := runContextLoops([]string{"--db", dbPath, "--issue", "mem-c111111", "--cycle", "1"}, &out); err != nil {
		t.Fatalf("run context loops text: %v", err)
	}

	rendered := out.String()
	for _, want := range []string{
		"issue=mem-c111111 cycle=1",
		"[gate/Open]",
		"priority=P1",
		"source=",
	} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("expected context loops output to contain %q, got:\n%s", want, rendered)
		}
	}

	out.Reset()
	if err := runContextLoops([]string{"--db", dbPath, "--issue", "mem-deadbee"}, &out); err != nil {
		t.Fatalf("run context loops with no matches: %v", err)
	}
	if got := strings.TrimSpace(out.String()); got != "No context loops matched the filters." {
		t.Fatalf("expected no-match context loops output, got %q", got)
	}
}

func TestRunEventTextOutputAndValidation(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "memori-cli-event-text.db")
	s := openInitializedCLIStoreForCoverage(t, dbPath)
	defer s.Close()

	ctx := context.Background()
	if _, _, _, err := s.CreateIssue(ctx, store.CreateIssueParams{
		IssueID:   "mem-e1e2e3f",
		Type:      "task",
		Title:     "Event output coverage",
		Actor:     "test",
		CommandID: "cmd-cli-event-create-1",
	}); err != nil {
		t.Fatalf("create event issue: %v", err)
	}
	if _, _, _, err := s.UpdateIssueStatus(ctx, store.UpdateIssueStatusParams{
		IssueID:   "mem-e1e2e3f",
		Status:    "inprogress",
		Actor:     "test",
		CommandID: "cmd-cli-event-update-1",
	}); err != nil {
		t.Fatalf("update event issue: %v", err)
	}

	if err := runEvent(nil, io.Discard); err == nil || !strings.Contains(err.Error(), "event subcommand required") {
		t.Fatalf("expected missing event subcommand error, got %v", err)
	}
	if err := runEvent([]string{"tail"}, io.Discard); err == nil || !strings.Contains(err.Error(), `unknown event subcommand "tail"`) {
		t.Fatalf("expected unknown event subcommand error, got %v", err)
	}

	var out bytes.Buffer
	if err := runEvent([]string{"log", "--db", dbPath, "--entity", "mem-e1e2e3f"}, &out); err != nil {
		t.Fatalf("run event log text: %v", err)
	}

	rendered := out.String()
	for _, want := range []string{
		"Events for issue:mem-e1e2e3f",
		"issue.created",
		"issue.updated",
		"actor=test",
		"command_id=cmd-cli-event-create-1",
		"command_id=cmd-cli-event-update-1",
		"causation_id=",
		"correlation_id=",
	} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("expected event log output to contain %q, got:\n%s", want, rendered)
		}
	}

	out.Reset()
	if err := runEvent([]string{"log", "--db", dbPath, "--entity", "packet:missing"}, &out); err != nil {
		t.Fatalf("run empty event log: %v", err)
	}
	if got := strings.TrimSpace(out.String()); got != "No events for packet:missing" {
		t.Fatalf("expected no-events output, got %q", got)
	}
}

func TestOpenInitializedStoreRejectsUninitializedAndBehindSchemas(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	uninitializedPath := filepath.Join(t.TempDir(), "memori-cli-open-uninitialized.db")
	uninitializedStore, err := store.Open(uninitializedPath)
	if err != nil {
		t.Fatalf("open uninitialized store: %v", err)
	}
	if _, err := uninitializedStore.DB().ExecContext(ctx, `
		CREATE TABLE schema_meta(
			key TEXT PRIMARY KEY,
			value TEXT NOT NULL,
			updated_at TEXT NOT NULL
		)
	`); err != nil {
		t.Fatalf("create schema_meta table: %v", err)
	}
	if err := uninitializedStore.Close(); err != nil {
		t.Fatalf("close uninitialized store: %v", err)
	}

	if _, _, err := openInitializedStore(ctx, uninitializedPath); err == nil || !(strings.Contains(err.Error(), "database is not initialized") || strings.Contains(err.Error(), "query schema version")) {
		t.Fatalf("expected uninitialized store error, got %v", err)
	}

	behindPath := filepath.Join(t.TempDir(), "memori-cli-open-behind.db")
	behindStore, err := store.Open(behindPath)
	if err != nil {
		t.Fatalf("open behind store: %v", err)
	}
	headVersion, err := dbschema.HeadVersion()
	if err != nil {
		t.Fatalf("head version: %v", err)
	}
	targetVersion := headVersion - 1
	if targetVersion <= 0 {
		t.Fatalf("expected head schema version > 1, got %d", headVersion)
	}
	if _, err := dbschema.Migrate(ctx, behindStore.DB(), &targetVersion); err != nil {
		t.Fatalf("migrate behind store: %v", err)
	}
	if err := behindStore.Close(); err != nil {
		t.Fatalf("close behind store: %v", err)
	}

	if _, _, err := openInitializedStore(ctx, behindPath); err == nil || !strings.Contains(err.Error(), "database schema is behind by 1 migration(s)") {
		t.Fatalf("expected pending migration error, got %v", err)
	}
}

func TestOpenInitializedStoreReturnsStoreAndVersionForInitializedDB(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "memori-cli-open-initialized.db")
	seed := openInitializedCLIStoreForCoverage(t, dbPath)
	if err := seed.Close(); err != nil {
		t.Fatalf("close initialized seed store: %v", err)
	}

	s, version, err := openInitializedStore(context.Background(), dbPath)
	if err != nil {
		t.Fatalf("open initialized store: %v", err)
	}
	defer s.Close()

	headVersion, err := dbschema.HeadVersion()
	if err != nil {
		t.Fatalf("head version: %v", err)
	}
	if version != headVersion {
		t.Fatalf("expected schema version %d, got %d", headVersion, version)
	}
}

func TestPrintGateVerifyResultTextAndJSON(t *testing.T) {
	t.Parallel()

	evaluation := store.GateEvaluation{
		IssueID:   "mem-gate11",
		GateSetID: "gs_test_1",
		GateID:    "build",
		Result:    "PASS",
	}
	event := store.Event{EventID: "evt_gate_1", EventType: "gate.verified", EventOrder: 7}

	var out bytes.Buffer
	if err := printGateVerifyResult(&out, 16, evaluation, event, "go test ./...", 0, "sha256sum", false, false); err != nil {
		t.Fatalf("print gate verify text: %v", err)
	}
	for _, want := range []string{
		"Verified gate build for issue mem-gate11 -> PASS (exit=0)",
		"Gate Set: gs_test_1",
		"Output SHA256: sha256sum",
	} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("expected gate verify text to contain %q, got:\n%s", want, out.String())
		}
	}

	out.Reset()
	if err := printGateVerifyResult(&out, 16, evaluation, event, "go test ./...", 0, "sha256sum", true, false); err != nil {
		t.Fatalf("print idempotent gate verify text: %v", err)
	}
	if !strings.Contains(out.String(), "Gate verification already recorded for issue mem-gate11 gate build.") {
		t.Fatalf("expected idempotent gate verify output, got:\n%s", out.String())
	}

	out.Reset()
	if err := printGateVerifyResult(&out, 16, evaluation, event, "go test ./...", 0, "sha256sum", false, true); err != nil {
		t.Fatalf("print gate verify json: %v", err)
	}

	var resp gateVerifyEnvelope
	if err := json.Unmarshal(out.Bytes(), &resp); err != nil {
		t.Fatalf("decode gate verify json: %v\nstdout: %s", err, out.String())
	}
	if resp.Command != "gate verify" {
		t.Fatalf("expected gate verify command, got %q", resp.Command)
	}
	if resp.Data.Command != "go test ./..." || resp.Data.ExitCode != 0 || resp.Data.OutputSHA != "sha256sum" {
		t.Fatalf("unexpected gate verify json payload: %+v", resp.Data)
	}
}

func TestShouldUseColorHonorsEnvOverridesAndWriterKinds(t *testing.T) {
	clearColorEnv := func(t *testing.T) {
		t.Helper()
		t.Setenv("MEMORI_COLOR", "")
		t.Setenv("NO_COLOR", "")
		t.Setenv("CLICOLOR", "")
		t.Setenv("CLICOLOR_FORCE", "")
		t.Setenv("FORCE_COLOR", "")
		t.Setenv("TERM", "")
	}

	t.Run("memori color always wins", func(t *testing.T) {
		clearColorEnv(t)
		t.Setenv("MEMORI_COLOR", "always")
		if !shouldUseColor(&bytes.Buffer{}) {
			t.Fatal("expected MEMORI_COLOR=always to force color")
		}
	})

	t.Run("memori color never disables color", func(t *testing.T) {
		clearColorEnv(t)
		t.Setenv("MEMORI_COLOR", "never")
		if shouldUseColor(&bytes.Buffer{}) {
			t.Fatal("expected MEMORI_COLOR=never to disable color")
		}
	})

	t.Run("no color disables color", func(t *testing.T) {
		clearColorEnv(t)
		t.Setenv("NO_COLOR", "1")
		if shouldUseColor(&bytes.Buffer{}) {
			t.Fatal("expected NO_COLOR to disable color")
		}
	})

	t.Run("clicolor zero disables color", func(t *testing.T) {
		clearColorEnv(t)
		t.Setenv("CLICOLOR", "0")
		if shouldUseColor(&bytes.Buffer{}) {
			t.Fatal("expected CLICOLOR=0 to disable color")
		}
	})

	t.Run("clicolor force enables color", func(t *testing.T) {
		clearColorEnv(t)
		t.Setenv("CLICOLOR_FORCE", "1")
		if !shouldUseColor(&bytes.Buffer{}) {
			t.Fatal("expected CLICOLOR_FORCE=1 to force color")
		}
	})

	t.Run("force color enables color", func(t *testing.T) {
		clearColorEnv(t)
		t.Setenv("FORCE_COLOR", "1")
		if !shouldUseColor(&bytes.Buffer{}) {
			t.Fatal("expected FORCE_COLOR=1 to force color")
		}
	})

	t.Run("dumb terminals disable color", func(t *testing.T) {
		clearColorEnv(t)
		t.Setenv("TERM", "dumb")
		if shouldUseColor(os.Stdout) {
			t.Fatal("expected TERM=dumb to disable color")
		}
	})

	t.Run("non file writers do not use color by default", func(t *testing.T) {
		clearColorEnv(t)
		if shouldUseColor(&bytes.Buffer{}) {
			t.Fatal("expected non-file writer to disable color")
		}
	})

	t.Run("regular files do not use color", func(t *testing.T) {
		clearColorEnv(t)
		file, err := os.CreateTemp(t.TempDir(), "memori-color-*")
		if err != nil {
			t.Fatalf("create temp file: %v", err)
		}
		defer file.Close()
		if shouldUseColor(file) {
			t.Fatal("expected regular files to disable color")
		}
	})

	t.Run("stat failures disable color", func(t *testing.T) {
		clearColorEnv(t)
		file, err := os.CreateTemp(t.TempDir(), "memori-color-closed-*")
		if err != nil {
			t.Fatalf("create temp file: %v", err)
		}
		if err := file.Close(); err != nil {
			t.Fatalf("close temp file: %v", err)
		}
		if shouldUseColor(file) {
			t.Fatal("expected stat failure to disable color")
		}
	})
}

func TestColorForGateResultMapsKnownStates(t *testing.T) {
	t.Parallel()

	cases := map[string]string{
		"PASS":    "32",
		"FAIL":    "31",
		"BLOCKED": "33",
		"MISSING": "90",
		"OTHER":   "37",
	}
	for result, want := range cases {
		if got := colorForGateResult(result); got != want {
			t.Fatalf("result %q: expected %q, got %q", result, want, got)
		}
	}
}

func TestTextUIHelpersRenderExpectedOutput(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	ui := newTextUI(&out)
	if ui.colors {
		t.Fatal("expected bytes.Buffer text UI to disable color")
	}

	ui.heading("Heading")
	ui.success("Created thing")
	ui.note("Careful with retries")
	ui.section("Details")
	ui.field("Blank", "   ")
	ui.field("Field", "Value")
	ui.bullet("step one")
	ui.blank()
	ui.nextSteps()
	ui.nextSteps("run tests", "ship it")

	rendered := out.String()
	for _, want := range []string{
		"Heading",
		"OK Created thing",
		"Note Careful with retries",
		"Details:",
		"Field: Value",
		"- step one",
		"Next:",
		"- run tests",
		"- ship it",
	} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("expected text UI output to contain %q, got:\n%s", want, rendered)
		}
	}
	if strings.Contains(rendered, "Blank:") {
		t.Fatalf("expected blank field to be skipped, got:\n%s", rendered)
	}
}

func openInitializedCLIStoreForCoverage(t *testing.T, dbPath string) *store.Store {
	t.Helper()

	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if err := s.Initialize(context.Background(), store.InitializeParams{IssueKeyPrefix: "mem"}); err != nil {
		t.Fatalf("initialize store: %v", err)
	}
	return s
}
