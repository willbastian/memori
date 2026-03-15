package cli

import (
	"context"
	"database/sql"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"github.com/willbastian/memori/internal/store"
)

type issueEnvelope struct {
	Command string `json:"command"`
	Data    struct {
		Issue store.Issue `json:"issue"`
	} `json:"data"`
}

func TestIssueCreateAndShowExposeRichContextFields(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "memori-cli-issue-context.db")
	if _, stderr, err := runMemoriForTest("init", "--db", dbPath, "--issue-prefix", "mem", "--json"); err != nil {
		t.Fatalf("init db: %v\nstderr: %s", err, stderr)
	}

	stdout, stderr, err := runMemoriForTest(
		"issue", "create",
		"--db", dbPath,
		"--key", "mem-2222aaa",
		"--type", "task",
		"--title", "Richer context",
		"--description", "A detailed description",
		"--acceptance-criteria", "Readable in issue show",
		"--reference", "https://example.com/spec",
		"--reference", "notes.md",
		"--actor", "test",
		"--command-id", "cmd-cli-rich-create-1",
		"--json",
	)
	if err != nil {
		t.Fatalf("issue create with rich context: %v\nstderr: %s", err, stderr)
	}
	var created issueEnvelope
	if err := json.Unmarshal([]byte(stdout), &created); err != nil {
		t.Fatalf("decode issue create json: %v\nstdout: %s", err, stdout)
	}
	if created.Data.Issue.Description != "A detailed description" {
		t.Fatalf("unexpected description: %q", created.Data.Issue.Description)
	}
	if created.Data.Issue.Acceptance != "Readable in issue show" {
		t.Fatalf("unexpected acceptance criteria: %q", created.Data.Issue.Acceptance)
	}
	if len(created.Data.Issue.References) != 2 {
		t.Fatalf("expected 2 references, got %#v", created.Data.Issue.References)
	}

	stdout, stderr, err = runMemoriForTest("issue", "show", "--db", dbPath, "--key", "mem-2222aaa", "--json")
	if err != nil {
		t.Fatalf("issue show json: %v\nstderr: %s", err, stderr)
	}
	var shown issueEnvelope
	if err := json.Unmarshal([]byte(stdout), &shown); err != nil {
		t.Fatalf("decode issue show json: %v\nstdout: %s", err, stdout)
	}
	if shown.Data.Issue.Description != "A detailed description" {
		t.Fatalf("expected show description, got %q", shown.Data.Issue.Description)
	}
	if shown.Data.Issue.Acceptance != "Readable in issue show" {
		t.Fatalf("expected show acceptance criteria, got %q", shown.Data.Issue.Acceptance)
	}
}

func TestIssueUpdateSupportsContextOnlyMutation(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "memori-cli-issue-update-context.db")
	if _, stderr, err := runMemoriForTest("init", "--db", dbPath, "--issue-prefix", "mem", "--json"); err != nil {
		t.Fatalf("init db: %v\nstderr: %s", err, stderr)
	}
	if _, stderr, err := runMemoriForTest(
		"issue", "create",
		"--db", dbPath,
		"--key", "mem-3333bbb",
		"--type", "task",
		"--title", "Context update",
		"--actor", "test",
		"--command-id", "cmd-cli-rich-update-create-1",
		"--json",
	); err != nil {
		t.Fatalf("issue create: %v\nstderr: %s", err, stderr)
	}

	stdout, stderr, err := runMemoriForTest(
		"issue", "update",
		"--db", dbPath,
		"--key", "mem-3333bbb",
		"--title", "Context update renamed",
		"--description", "Context-only update",
		"--acceptance-criteria", "No status required for context edit",
		"--reference", "https://example.com/context",
		"--actor", "test",
		"--command-id", "cmd-cli-rich-update-1",
		"--json",
	)
	if err != nil {
		t.Fatalf("issue update context-only: %v\nstderr: %s", err, stderr)
	}
	var updated issueEnvelope
	if err := json.Unmarshal([]byte(stdout), &updated); err != nil {
		t.Fatalf("decode issue update json: %v\nstdout: %s", err, stdout)
	}
	if updated.Data.Issue.Status != "Todo" {
		t.Fatalf("status should remain Todo on context-only update, got %q", updated.Data.Issue.Status)
	}
	if updated.Data.Issue.Title != "Context update renamed" {
		t.Fatalf("unexpected title after update: %q", updated.Data.Issue.Title)
	}
	if updated.Data.Issue.Description != "Context-only update" {
		t.Fatalf("unexpected description after update: %q", updated.Data.Issue.Description)
	}
}

func TestIssueUpdateRequiresAtLeastOneMutationField(t *testing.T) {
	t.Parallel()

	_, _, err := runMemoriForTest(
		"issue", "update",
		"--key", "mem-4444ccc",
		"--command-id", "cmd-cli-rich-update-empty-1",
	)
	if err == nil || !strings.Contains(err.Error(), "one of --title, --status, --priority, --label, --description, --acceptance-criteria, or --reference is required") {
		t.Fatalf("expected mutation field validation error, got: %v", err)
	}
}

func TestIssueUpdateRejectsBlankTitle(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "memori-cli-issue-update-blank-title.db")
	if _, stderr, err := runMemoriForTest("init", "--db", dbPath, "--issue-prefix", "mem", "--json"); err != nil {
		t.Fatalf("init db: %v\nstderr: %s", err, stderr)
	}
	if _, stderr, err := runMemoriForTest(
		"issue", "create",
		"--db", dbPath,
		"--key", "mem-5555ddd",
		"--type", "task",
		"--title", "Blank title target",
		"--actor", "test",
		"--command-id", "cmd-cli-blank-title-create-1",
		"--json",
	); err != nil {
		t.Fatalf("issue create: %v\nstderr: %s", err, stderr)
	}

	_, _, err := runMemoriForTest(
		"issue", "update",
		"--db", dbPath,
		"--key", "mem-5555ddd",
		"--title", "   ",
		"--actor", "test",
		"--command-id", "cmd-cli-blank-title-update-1",
	)
	if err == nil || !strings.Contains(err.Error(), "--title is required") {
		t.Fatalf("expected blank title validation error, got: %v", err)
	}
}

func TestIssueUpdateInProgressStartsContinuityAndFocusForAgent(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "memori-cli-issue-update-starts-continuity.db")
	if _, stderr, err := runMemoriForTest("init", "--db", dbPath, "--issue-prefix", "mem", "--json"); err != nil {
		t.Fatalf("init db: %v\nstderr: %s", err, stderr)
	}
	if _, stderr, err := runMemoriForTest(
		"issue", "create",
		"--db", dbPath,
		"--key", "mem-6666eee",
		"--type", "task",
		"--title", "Auto-start continuity",
		"--actor", "test",
		"--command-id", "cmd-cli-start-continuity-create-1",
		"--json",
	); err != nil {
		t.Fatalf("issue create: %v\nstderr: %s", err, stderr)
	}

	stdout, stderr, err := runMemoriForTest(
		"issue", "update",
		"--db", dbPath,
		"--key", "mem-6666eee",
		"--status", "inprogress",
		"--agent", "agent-start-1",
		"--actor", "test",
		"--command-id", "cmd-cli-start-continuity-update-1",
	)
	if err != nil {
		t.Fatalf("issue update inprogress: %v\nstderr: %s", err, stderr)
	}
	mustContain(t, stdout, "Continuity Started:")
	mustContain(t, stdout, "Captured session ")
	mustContain(t, stdout, "Refreshed issue packet ")
	mustContain(t, stdout, "Updated agent agent-start-1 focus to mem-6666eee via packet ")

	stdout, stderr, err = runMemoriForTest(
		"context", "rehydrate",
		"--db", dbPath,
		"--json",
	)
	if err != nil {
		t.Fatalf("context rehydrate latest session: %v\nstderr: %s", err, stderr)
	}
	var rehydrated contextRehydrateEnvelope
	if err := json.Unmarshal([]byte(stdout), &rehydrated); err != nil {
		t.Fatalf("decode context rehydrate json: %v\nstdout: %s", err, stdout)
	}
	if !strings.HasPrefix(rehydrated.Data.SessionID, "sess_") {
		t.Fatalf("expected auto-created session id, got %+v", rehydrated)
	}

	stdout, stderr, err = runMemoriForTest(
		"event", "log",
		"--db", dbPath,
		"--entity", "session:"+rehydrated.Data.SessionID,
		"--json",
	)
	if err != nil {
		t.Fatalf("event log session: %v\nstderr: %s", err, stderr)
	}
	var sessionEvents eventLogEnvelope
	if err := json.Unmarshal([]byte(stdout), &sessionEvents); err != nil {
		t.Fatalf("decode session event log json: %v\nstdout: %s", err, stdout)
	}
	if len(sessionEvents.Data.Events) != 1 || sessionEvents.Data.Events[0].EventType != "session.checkpointed" {
		t.Fatalf("expected session checkpoint event, got %+v", sessionEvents.Data.Events)
	}
	if sessionEvents.Data.Events[0].CommandID != "cmd-cli-start-continuity-update-1-checkpoint" {
		t.Fatalf("expected checkpoint command id, got %+v", sessionEvents.Data.Events[0])
	}

	stdout, stderr, err = runMemoriForTest(
		"event", "log",
		"--db", dbPath,
		"--entity", "focus:agent-start-1",
		"--json",
	)
	if err != nil {
		t.Fatalf("event log focus: %v\nstderr: %s", err, stderr)
	}
	var focusEvents eventLogEnvelope
	if err := json.Unmarshal([]byte(stdout), &focusEvents); err != nil {
		t.Fatalf("decode focus event log json: %v\nstdout: %s", err, stdout)
	}
	if len(focusEvents.Data.Events) != 1 || focusEvents.Data.Events[0].EventType != "focus.used" {
		t.Fatalf("expected focus.used event, got %+v", focusEvents.Data.Events)
	}
	if focusEvents.Data.Events[0].CommandID != "cmd-cli-start-continuity-update-1-focus" {
		t.Fatalf("expected focus command id, got %+v", focusEvents.Data.Events[0])
	}

	stdout, stderr, err = runMemoriForTest("issue", "show", "--db", dbPath, "--key", "mem-6666eee")
	if err != nil {
		t.Fatalf("issue show: %v\nstderr: %s", err, stderr)
	}
	mustContain(t, stdout, "Latest open session for this issue "+rehydrated.Data.SessionID+" has no saved summary and no saved session packet yet.")
	mustContain(t, stdout, "Latest issue packet ")
	mustContain(t, stdout, "is fresh for mem-6666eee cycle 1.")
}

func TestIssueUpdateBlockedSavesContinuityByDefault(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "memori-cli-issue-update-blocked-continuity.db")
	if _, stderr, err := runMemoriForTest("init", "--db", dbPath, "--issue-prefix", "mem", "--json"); err != nil {
		t.Fatalf("init db: %v\nstderr: %s", err, stderr)
	}
	if _, stderr, err := runMemoriForTest(
		"issue", "create",
		"--db", dbPath,
		"--key", "mem-7777fff",
		"--type", "task",
		"--title", "Auto-save continuity",
		"--actor", "test",
		"--command-id", "cmd-cli-blocked-continuity-create-1",
		"--json",
	); err != nil {
		t.Fatalf("issue create: %v\nstderr: %s", err, stderr)
	}
	if _, stderr, err := runMemoriForTest(
		"issue", "update",
		"--db", dbPath,
		"--key", "mem-7777fff",
		"--status", "inprogress",
		"--actor", "test",
		"--command-id", "cmd-cli-blocked-continuity-progress-1",
		"--json",
	); err != nil {
		t.Fatalf("issue update inprogress: %v\nstderr: %s", err, stderr)
	}

	stdout, stderr, err := runMemoriForTest(
		"issue", "update",
		"--db", dbPath,
		"--key", "mem-7777fff",
		"--status", "blocked",
		"--note", "waiting on review",
		"--actor", "test",
		"--command-id", "cmd-cli-blocked-continuity-blocked-1",
	)
	if err != nil {
		t.Fatalf("issue update blocked: %v\nstderr: %s", err, stderr)
	}
	mustContain(t, stdout, "Continuity Saved:")
	mustContain(t, stdout, "Used the open session already tracking this issue")
	mustContain(t, stdout, "Summarized session ")
	mustContain(t, stdout, "Saved session packet ")

	stdout, stderr, err = runMemoriForTest(
		"context", "rehydrate",
		"--db", dbPath,
		"--json",
	)
	if err != nil {
		t.Fatalf("context rehydrate after blocked: %v\nstderr: %s", err, stderr)
	}
	var rehydrated contextRehydrateEnvelope
	if err := json.Unmarshal([]byte(stdout), &rehydrated); err != nil {
		t.Fatalf("decode blocked rehydrate json: %v\nstdout: %s", err, stdout)
	}

	stdout, stderr, err = runMemoriForTest(
		"event", "log",
		"--db", dbPath,
		"--entity", "session:"+rehydrated.Data.SessionID,
		"--json",
	)
	if err != nil {
		t.Fatalf("event log blocked session: %v\nstderr: %s", err, stderr)
	}
	var sessionEvents eventLogEnvelope
	if err := json.Unmarshal([]byte(stdout), &sessionEvents); err != nil {
		t.Fatalf("decode blocked session event log: %v\nstdout: %s", err, stdout)
	}
	if len(sessionEvents.Data.Events) < 2 {
		t.Fatalf("expected checkpoint and summary events, got %+v", sessionEvents.Data.Events)
	}
	if sessionEvents.Data.Events[1].EventType != "session.summarized" {
		t.Fatalf("expected session.summarized event, got %+v", sessionEvents.Data.Events[1])
	}
	if sessionEvents.Data.Events[1].CommandID != "cmd-cli-blocked-continuity-blocked-1-summarize" {
		t.Fatalf("expected summarize command id, got %+v", sessionEvents.Data.Events[1])
	}
}

func TestIssueUpdateBlockedRejectsDifferentIssueOpenSession(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "memori-cli-issue-update-blocked-cross-issue.db")
	if _, stderr, err := runMemoriForTest("init", "--db", dbPath, "--issue-prefix", "mem", "--json"); err != nil {
		t.Fatalf("init db: %v\nstderr: %s", err, stderr)
	}
	for _, args := range [][]string{
		{
			"issue", "create",
			"--db", dbPath,
			"--key", "mem-7777aaa",
			"--type", "task",
			"--title", "Issue with continuity",
			"--actor", "test",
			"--command-id", "cmd-cli-cross-issue-create-1",
			"--json",
		},
		{
			"issue", "create",
			"--db", dbPath,
			"--key", "mem-7777aab",
			"--type", "task",
			"--title", "Issue without continuity",
			"--actor", "test",
			"--command-id", "cmd-cli-cross-issue-create-2",
			"--json",
		},
		{
			"issue", "update",
			"--db", dbPath,
			"--key", "mem-7777aaa",
			"--status", "inprogress",
			"--actor", "test",
			"--command-id", "cmd-cli-cross-issue-progress-1",
			"--json",
		},
	} {
		if _, stderr, err := runMemoriForTest(args...); err != nil {
			t.Fatalf("setup command %v: %v\nstderr: %s", args, err, stderr)
		}
	}

	_, _, err := runMemoriForTest(
		"issue", "update",
		"--db", dbPath,
		"--key", "mem-7777aab",
		"--status", "blocked",
		"--actor", "test",
		"--command-id", "cmd-cli-cross-issue-blocked-1",
	)
	if err == nil || !strings.Contains(err.Error(), "no open session found for issue mem-7777aab") {
		t.Fatalf("expected issue-scoped continuity error, got %v", err)
	}

	stdout, stderr, err := runMemoriForTest("issue", "show", "--db", dbPath, "--key", "mem-7777aab", "--json")
	if err != nil {
		t.Fatalf("issue show unaffected issue: %v\nstderr: %s", err, stderr)
	}
	var shown issueEnvelope
	if err := json.Unmarshal([]byte(stdout), &shown); err != nil {
		t.Fatalf("decode unaffected issue json: %v\nstdout: %s", err, stdout)
	}
	if shown.Data.Issue.Status != "Blocked" {
		t.Fatalf("expected target issue status update to stick before continuity failure, got %+v", shown.Data.Issue)
	}

	stdout, stderr, err = runMemoriForTest("context", "rehydrate", "--db", dbPath, "--json")
	if err != nil {
		t.Fatalf("context rehydrate surviving session: %v\nstderr: %s", err, stderr)
	}
	var rehydrated contextRehydrateEnvelope
	if err := json.Unmarshal([]byte(stdout), &rehydrated); err != nil {
		t.Fatalf("decode surviving session rehydrate json: %v\nstdout: %s", err, stdout)
	}

	stdout, stderr, err = runMemoriForTest(
		"event", "log",
		"--db", dbPath,
		"--entity", "session:"+rehydrated.Data.SessionID,
		"--json",
	)
	if err != nil {
		t.Fatalf("event log surviving session: %v\nstderr: %s", err, stderr)
	}
	var sessionEvents eventLogEnvelope
	if err := json.Unmarshal([]byte(stdout), &sessionEvents); err != nil {
		t.Fatalf("decode surviving session event log: %v\nstdout: %s", err, stdout)
	}
	if len(sessionEvents.Data.Events) != 1 || sessionEvents.Data.Events[0].EventType != "session.checkpointed" {
		t.Fatalf("expected the other issue session to stay untouched, got %+v", sessionEvents.Data.Events)
	}
}

func TestIssueUpdateBlockedUsesLegacyIssueSessionWithoutCheckpointIssueID(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "memori-cli-issue-update-blocked-legacy-issue-session.db")
	if _, stderr, err := runMemoriForTest("init", "--db", dbPath, "--issue-prefix", "mem", "--json"); err != nil {
		t.Fatalf("init db: %v\nstderr: %s", err, stderr)
	}
	if _, stderr, err := runMemoriForTest(
		"issue", "create",
		"--db", dbPath,
		"--key", "mem-7777ffc",
		"--type", "task",
		"--title", "Legacy continuity issue",
		"--actor", "test",
		"--command-id", "cmd-cli-legacy-blocked-create-1",
		"--json",
	); err != nil {
		t.Fatalf("issue create: %v\nstderr: %s", err, stderr)
	}
	if _, stderr, err := runMemoriForTest(
		"issue", "update",
		"--db", dbPath,
		"--key", "mem-7777ffc",
		"--status", "inprogress",
		"--actor", "test",
		"--command-id", "cmd-cli-legacy-blocked-progress-1",
		"--json",
	); err != nil {
		t.Fatalf("issue update inprogress: %v\nstderr: %s", err, stderr)
	}

	ctx := context.Background()
	s, _, err := openInitializedStore(ctx, dbPath)
	if err != nil {
		t.Fatalf("open initialized store: %v", err)
	}
	legacySession, found, err := s.LatestOpenSessionForIssue(ctx, "mem-7777ffc")
	s.Close()
	if err != nil {
		t.Fatalf("latest open issue session before legacy simulation: %v", err)
	}
	if !found {
		t.Fatal("expected open issue session before legacy simulation")
	}

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer db.Close()
	if _, err := db.ExecContext(ctx, `
		UPDATE sessions
		SET checkpoint_json = json_remove(checkpoint_json, '$.issue_id')
		WHERE session_id = ?
	`, legacySession.SessionID); err != nil {
		t.Fatalf("strip checkpoint issue id for legacy simulation: %v", err)
	}

	stdout, stderr, err := runMemoriForTest(
		"issue", "update",
		"--db", dbPath,
		"--key", "mem-7777ffc",
		"--status", "blocked",
		"--note", "legacy upgrade path",
		"--actor", "test",
		"--command-id", "cmd-cli-legacy-blocked-blocked-1",
	)
	if err != nil {
		t.Fatalf("issue update blocked with legacy issue session: %v\nstderr: %s", err, stderr)
	}
	mustContain(t, stdout, "Continuity Saved:")
	mustContain(t, stdout, "Used the open session already tracking this issue ("+legacySession.SessionID+")")
	mustContain(t, stdout, "Summarized session "+legacySession.SessionID+".")
}

func TestIssueUpdateBlockedRequiresOpenSessionUnlessSkipped(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "memori-cli-issue-update-blocked-requires-session.db")
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
		IssueID:   "mem-8888aaa",
		Type:      "task",
		Title:     "Missing continuity session",
		Actor:     "test",
		CommandID: "cmd-cli-blocked-requires-session-create-1",
	}); err != nil {
		t.Fatalf("create issue: %v", err)
	}
	if _, _, _, err := s.UpdateIssueStatus(ctx, store.UpdateIssueStatusParams{
		IssueID:   "mem-8888aaa",
		Status:    "inprogress",
		Actor:     "test",
		CommandID: "cmd-cli-blocked-requires-session-progress-1",
	}); err != nil {
		t.Fatalf("move issue to inprogress: %v", err)
	}

	_, _, err = runMemoriForTest(
		"issue", "update",
		"--db", dbPath,
		"--key", "mem-8888aaa",
		"--status", "blocked",
		"--actor", "test",
		"--command-id", "cmd-cli-blocked-requires-session-blocked-1",
	)
	if err == nil || !strings.Contains(err.Error(), "issue mem-8888aaa is now Blocked, but automatic continuity capture needs an open session") {
		t.Fatalf("expected blocked continuity session error, got %v", err)
	}
	stdout, stderr, err := runMemoriForTest("issue", "show", "--db", dbPath, "--key", "mem-8888aaa", "--json")
	if err != nil {
		t.Fatalf("issue show after blocked continuity failure: %v\nstderr: %s", err, stderr)
	}
	var blocked issueEnvelope
	if err := json.Unmarshal([]byte(stdout), &blocked); err != nil {
		t.Fatalf("decode blocked issue json: %v\nstdout: %s", err, stdout)
	}
	if blocked.Data.Issue.Status != "Blocked" {
		t.Fatalf("expected blocked status to persist after continuity failure, got %+v", blocked.Data.Issue)
	}
	if _, _, _, err := s.CreateIssue(ctx, store.CreateIssueParams{
		IssueID:   "mem-8888aab",
		Type:      "task",
		Title:     "Skip continuity session bypass",
		Actor:     "test",
		CommandID: "cmd-cli-blocked-requires-session-create-2",
	}); err != nil {
		t.Fatalf("create second issue: %v", err)
	}
	if _, _, _, err := s.UpdateIssueStatus(ctx, store.UpdateIssueStatusParams{
		IssueID:   "mem-8888aab",
		Status:    "inprogress",
		Actor:     "test",
		CommandID: "cmd-cli-blocked-requires-session-progress-2",
	}); err != nil {
		t.Fatalf("move second issue to inprogress: %v", err)
	}

	stdout, stderr, err = runMemoriForTest(
		"issue", "update",
		"--db", dbPath,
		"--key", "mem-8888aab",
		"--status", "blocked",
		"--skip-continuity",
		"--actor", "test",
		"--command-id", "cmd-cli-blocked-requires-session-blocked-2",
	)
	if err != nil {
		t.Fatalf("issue update blocked with skip: %v\nstderr: %s", err, stderr)
	}
	if strings.Contains(stdout, "Continuity Saved:") {
		t.Fatalf("did not expect continuity save section when skip flag is set, got:\n%s", stdout)
	}
}

func TestIssueUpdateBlockedPreservesIssueNotFoundErrorBeforeContinuityPrecheck(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "memori-cli-issue-update-blocked-missing-issue.db")
	if _, stderr, err := runMemoriForTest("init", "--db", dbPath, "--issue-prefix", "mem", "--json"); err != nil {
		t.Fatalf("init db: %v\nstderr: %s", err, stderr)
	}

	_, _, err := runMemoriForTest(
		"issue", "update",
		"--db", dbPath,
		"--key", "mem-deadbee",
		"--status", "blocked",
		"--actor", "test",
		"--command-id", "cmd-cli-blocked-missing-issue-1",
	)
	if err == nil || !strings.Contains(err.Error(), `issue "mem-deadbee" not found`) {
		t.Fatalf("expected missing issue error before continuity precheck, got %v", err)
	}
	if strings.Contains(err.Error(), "automatic continuity capture needs an open session") {
		t.Fatalf("expected missing issue error to win over continuity precheck, got %v", err)
	}
}

func TestIssueUpdateDoneSavesAndClosesContinuityByDefault(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "memori-cli-issue-update-done-continuity.db")
	if _, stderr, err := runMemoriForTest("init", "--db", dbPath, "--issue-prefix", "mem", "--json"); err != nil {
		t.Fatalf("init db: %v\nstderr: %s", err, stderr)
	}
	if _, stderr, err := runMemoriForTest(
		"issue", "create",
		"--db", dbPath,
		"--key", "mem-c111111",
		"--type", "task",
		"--title", "Auto-close continuity",
		"--actor", "test",
		"--command-id", "cmd-cli-done-continuity-create-1",
		"--json",
	); err != nil {
		t.Fatalf("issue create: %v\nstderr: %s", err, stderr)
	}
	if _, stderr, err := runMemoriForTest(
		"issue", "update",
		"--db", dbPath,
		"--key", "mem-c111111",
		"--status", "inprogress",
		"--actor", "test",
		"--command-id", "cmd-cli-done-continuity-progress-1",
		"--json",
	); err != nil {
		t.Fatalf("issue update inprogress: %v\nstderr: %s", err, stderr)
	}

	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store for gate setup: %v", err)
	}
	defer s.Close()

	ctx := context.Background()
	createGateTemplateForTest(
		t,
		s,
		ctx,
		"tmpl-close",
		1,
		[]string{"task"},
		`{"gates":[{"id":"build","criteria":{"ref":"manual-validation"}}]}`,
		"human:alice",
		"cmd-cli-done-continuity-template-1",
		"create close template",
	)
	insertGateSetForTest(
		t,
		s,
		ctx,
		"gs_cli_done_1",
		1,
		`["tmpl-close@1"]`,
		`{"gates":[{"id":"build"}]}`,
		"gs_cli_done_hash_1",
		"2026-03-15T00:00:00Z",
		"2026-03-15T00:00:00Z",
		"test",
	)
	insertGateSetItemForTest(t, s, ctx, "gs_cli_done_1", "build", "check", 1, `{"ref":"manual-validation"}`, "insert done gate item")
	if _, _, _, err := s.EvaluateGate(ctx, store.EvaluateGateParams{
		IssueID: "mem-c111111",
		GateID:  "build",
		Result:  "PASS",
		EvidenceRefs: []string{
			"test://done-continuity",
		},
		Actor:     "test",
		CommandID: "cmd-cli-done-continuity-gate-1",
	}); err != nil {
		t.Fatalf("evaluate done gate: %v", err)
	}

	stdout, stderr, err := runMemoriForTest(
		"issue", "update",
		"--db", dbPath,
		"--key", "mem-c111111",
		"--status", "done",
		"--note", "completed implementation",
		"--reason", "ready for handoff",
		"--actor", "test",
		"--command-id", "cmd-cli-done-continuity-done-1",
	)
	if err != nil {
		t.Fatalf("issue update done: %v\nstderr: %s", err, stderr)
	}
	mustContain(t, stdout, "Continuity Saved:")
	mustContain(t, stdout, "Summarized session ")
	mustContain(t, stdout, "Closed session ")
	mustContain(t, stdout, "Saved session packet ")

	stdout, stderr, err = runMemoriForTest(
		"context", "rehydrate",
		"--db", dbPath,
		"--json",
	)
	if err != nil {
		t.Fatalf("context rehydrate after done: %v\nstderr: %s", err, stderr)
	}
	var rehydrated contextRehydrateEnvelope
	if err := json.Unmarshal([]byte(stdout), &rehydrated); err != nil {
		t.Fatalf("decode done rehydrate json: %v\nstdout: %s", err, stdout)
	}
	if rehydrated.Data.Source != "packet" {
		t.Fatalf("expected packet-first rehydrate after done continuity save, got %+v", rehydrated)
	}

	stdout, stderr, err = runMemoriForTest(
		"event", "log",
		"--db", dbPath,
		"--entity", "session:"+rehydrated.Data.SessionID,
		"--json",
	)
	if err != nil {
		t.Fatalf("event log done session: %v\nstderr: %s", err, stderr)
	}
	var sessionEvents eventLogEnvelope
	if err := json.Unmarshal([]byte(stdout), &sessionEvents); err != nil {
		t.Fatalf("decode done session event log: %v\nstdout: %s", err, stdout)
	}
	if len(sessionEvents.Data.Events) < 3 {
		t.Fatalf("expected checkpoint, summary, and close events, got %+v", sessionEvents.Data.Events)
	}
	if sessionEvents.Data.Events[1].EventType != "session.summarized" || sessionEvents.Data.Events[2].EventType != "session.closed" {
		t.Fatalf("expected summarized and closed events, got %+v", sessionEvents.Data.Events)
	}
	if sessionEvents.Data.Events[2].CommandID != "cmd-cli-done-continuity-done-1-close" {
		t.Fatalf("expected close command id, got %+v", sessionEvents.Data.Events[2])
	}
}

func TestIssueUpdateContinuityModeManualSkipsAutomaticStart(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "memori-cli-issue-update-manual-mode.db")
	if _, stderr, err := runMemoriForTest("init", "--db", dbPath, "--issue-prefix", "mem", "--json"); err != nil {
		t.Fatalf("init db: %v\nstderr: %s", err, stderr)
	}
	if _, stderr, err := runMemoriForTest(
		"issue", "create",
		"--db", dbPath,
		"--key", "mem-b111111",
		"--type", "task",
		"--title", "Manual continuity mode",
		"--command-id", "cmd-cli-manual-mode-create-1",
		"--json",
	); err != nil {
		t.Fatalf("issue create: %v\nstderr: %s", err, stderr)
	}

	stdout, stderr, err := runMemoriForTest(
		"issue", "update",
		"--db", dbPath,
		"--key", "mem-b111111",
		"--status", "inprogress",
		"--continuity", "manual",
		"--command-id", "cmd-cli-manual-mode-update-1",
	)
	if err != nil {
		t.Fatalf("issue update manual mode: %v\nstderr: %s", err, stderr)
	}
	mustContain(t, stdout, "Continuity Mode:")
	mustContain(t, stdout, "Continuity mode manual disabled automatic continuity for this command.")
	if strings.Contains(stdout, "Continuity Started:") {
		t.Fatalf("did not expect automatic continuity start in manual mode, got:\n%s", stdout)
	}

	stdout, stderr, err = runMemoriForTest("issue", "show", "--db", dbPath, "--key", "mem-b111111")
	if err != nil {
		t.Fatalf("issue show manual mode: %v\nstderr: %s", err, stderr)
	}
	mustContain(t, stdout, "No open or historical session is available yet.")
}

func TestIssueUpdateContinuityModeAssistShowsExplicitBundleSteps(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "memori-cli-issue-update-assist-mode.db")
	if _, stderr, err := runMemoriForTest("init", "--db", dbPath, "--issue-prefix", "mem", "--json"); err != nil {
		t.Fatalf("init db: %v\nstderr: %s", err, stderr)
	}
	if _, stderr, err := runMemoriForTest(
		"issue", "create",
		"--db", dbPath,
		"--key", "mem-b222222",
		"--type", "task",
		"--title", "Assist continuity mode",
		"--command-id", "cmd-cli-assist-mode-create-1",
		"--json",
	); err != nil {
		t.Fatalf("issue create: %v\nstderr: %s", err, stderr)
	}

	stdout, stderr, err := runMemoriForTest(
		"issue", "update",
		"--db", dbPath,
		"--key", "mem-b222222",
		"--status", "inprogress",
		"--agent", "agent-assist-1",
		"--continuity", "assist",
		"--command-id", "cmd-cli-assist-mode-update-1",
	)
	if err != nil {
		t.Fatalf("issue update assist mode: %v\nstderr: %s", err, stderr)
	}
	mustContain(t, stdout, "Continuity Assist:")
	mustContain(t, stdout, "Continuity mode assist kept continuity explicit for this command.")
	mustContain(t, stdout, "memori context start --issue mem-b222222 --agent agent-assist-1")
	if strings.Contains(stdout, "Continuity Started:") {
		t.Fatalf("did not expect automatic continuity start in assist mode, got:\n%s", stdout)
	}
}

func TestIssueUpdateContinuityModeAssistScopesSaveStepsToIssueSession(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "memori-cli-issue-update-assist-scope.db")
	if _, stderr, err := runMemoriForTest("init", "--db", dbPath, "--issue-prefix", "mem", "--json"); err != nil {
		t.Fatalf("init db: %v\nstderr: %s", err, stderr)
	}
	for _, issueKey := range []string{"mem-b222223", "mem-b222224"} {
		if _, stderr, err := runMemoriForTest(
			"issue", "create",
			"--db", dbPath,
			"--key", issueKey,
			"--type", "task",
			"--title", "Assist continuity scope",
			"--command-id", "cmd-"+issueKey+"-create-1",
			"--json",
		); err != nil {
			t.Fatalf("issue create %s: %v\nstderr: %s", issueKey, err, stderr)
		}
	}

	if _, stderr, err := runMemoriForTest(
		"issue", "update",
		"--db", dbPath,
		"--key", "mem-b222223",
		"--status", "inprogress",
		"--agent", "agent-assist-scope-1",
		"--command-id", "cmd-assist-scope-issue-a-inprogress-1",
	); err != nil {
		t.Fatalf("issue A inprogress: %v\nstderr: %s", err, stderr)
	}
	if _, stderr, err := runMemoriForTest(
		"context", "start",
		"--db", dbPath,
		"--issue", "mem-b222224",
		"--session", "sess-assist-scope-b",
		"--agent", "agent-assist-scope-2",
		"--command-id", "cmd-assist-scope-issue-b-context-start-1",
	); err != nil {
		t.Fatalf("issue B context start: %v\nstderr: %s", err, stderr)
	}

	ctx := context.Background()
	s, _, err := openInitializedStore(ctx, dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	issueASession, found, err := s.LatestOpenSessionForIssue(ctx, "mem-b222223")
	s.Close()
	if err != nil {
		t.Fatalf("latest open session for issue A: %v", err)
	}
	if !found {
		t.Fatal("expected open session for issue A")
	}

	stdout, stderr, err := runMemoriForTest(
		"issue", "update",
		"--db", dbPath,
		"--key", "mem-b222223",
		"--status", "blocked",
		"--continuity", "assist",
		"--note", "waiting on review",
		"--command-id", "cmd-assist-scope-issue-a-blocked-1",
	)
	if err != nil {
		t.Fatalf("issue A blocked assist mode: %v\nstderr: %s", err, stderr)
	}
	mustContain(t, stdout, "Continuity Assist:")
	mustContain(t, stdout, "memori context save --session "+issueASession.SessionID+" --note 'waiting on review'")
	if strings.Contains(stdout, "memori context save --note 'waiting on review'") {
		t.Fatalf("expected assist guidance to scope save command to the issue session, got:\n%s", stdout)
	}

	doneSteps := issueUpdateContinuityAssistSteps("mem-b222227", "sess-assist-scope-c", "done", "", "wrapped up", "merged")
	if len(doneSteps) != 1 {
		t.Fatalf("expected one done assist step, got %#v", doneSteps)
	}
	if doneSteps[0] != "memori context save --session sess-assist-scope-c --close --note 'wrapped up' --reason merged" {
		t.Fatalf("expected done assist guidance to scope close command to the issue session, got %#v", doneSteps)
	}
}

func TestIssueUpdateContinuityModeCanBeSetByEnv(t *testing.T) {
	t.Setenv("MEMORI_CONTINUITY_MODE", "manual")

	dbPath := filepath.Join(t.TempDir(), "memori-cli-issue-update-env-mode.db")
	if _, stderr, err := runMemoriForTest("init", "--db", dbPath, "--issue-prefix", "mem", "--json"); err != nil {
		t.Fatalf("init db: %v\nstderr: %s", err, stderr)
	}
	if _, stderr, err := runMemoriForTest(
		"issue", "create",
		"--db", dbPath,
		"--key", "mem-b333333",
		"--type", "task",
		"--title", "Env continuity mode",
		"--command-id", "cmd-cli-env-mode-create-1",
		"--json",
	); err != nil {
		t.Fatalf("issue create: %v\nstderr: %s", err, stderr)
	}

	stdout, stderr, err := runMemoriForTest(
		"issue", "update",
		"--db", dbPath,
		"--key", "mem-b333333",
		"--status", "inprogress",
		"--command-id", "cmd-cli-env-mode-update-1",
	)
	if err != nil {
		t.Fatalf("issue update env mode: %v\nstderr: %s", err, stderr)
	}
	mustContain(t, stdout, "Continuity mode manual disabled automatic continuity for this command.")
}

func TestIssueShowResumeGuidanceUsesIssueSessionWhenAnotherSessionIsNewer(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "memori-cli-issue-show-resume-scope.db")
	if _, stderr, err := runMemoriForTest("init", "--db", dbPath, "--issue-prefix", "mem", "--json"); err != nil {
		t.Fatalf("init db: %v\nstderr: %s", err, stderr)
	}
	for _, issueKey := range []string{"mem-b222225", "mem-b222226"} {
		if _, stderr, err := runMemoriForTest(
			"issue", "create",
			"--db", dbPath,
			"--key", issueKey,
			"--type", "task",
			"--title", "Issue show resume scope",
			"--command-id", "cmd-"+issueKey+"-create-1",
			"--json",
		); err != nil {
			t.Fatalf("issue create %s: %v\nstderr: %s", issueKey, err, stderr)
		}
	}

	if _, stderr, err := runMemoriForTest(
		"issue", "update",
		"--db", dbPath,
		"--key", "mem-b222225",
		"--status", "inprogress",
		"--agent", "agent-show-scope-1",
		"--command-id", "cmd-show-scope-issue-a-inprogress-1",
	); err != nil {
		t.Fatalf("issue A inprogress: %v\nstderr: %s", err, stderr)
	}
	if _, stderr, err := runMemoriForTest(
		"context", "start",
		"--db", dbPath,
		"--issue", "mem-b222226",
		"--session", "sess-show-scope-b",
		"--agent", "agent-show-scope-2",
		"--command-id", "cmd-show-scope-issue-b-context-start-1",
	); err != nil {
		t.Fatalf("issue B context start: %v\nstderr: %s", err, stderr)
	}

	ctx := context.Background()
	s, _, err := openInitializedStore(ctx, dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	issueASession, found, err := s.LatestOpenSessionForIssue(ctx, "mem-b222225")
	s.Close()
	if err != nil {
		t.Fatalf("latest open session for issue A: %v", err)
	}
	if !found {
		t.Fatal("expected open session for issue A")
	}

	stdout, stderr, err := runMemoriForTest("issue", "show", "--db", dbPath, "--key", "mem-b222225")
	if err != nil {
		t.Fatalf("issue show scoped resume guidance: %v\nstderr: %s", err, stderr)
	}
	mustContain(t, stdout, "Resume:")
	mustContain(t, stdout, "memori context resume --session "+issueASession.SessionID)
	mustContain(t, stdout, "memori context resume --session "+issueASession.SessionID+" --agent <agent-id>")
	if strings.Contains(stdout, "sess-show-scope-b") {
		t.Fatalf("expected issue show continuity surfaces to ignore another issue's session, got:\n%s", stdout)
	}
	if strings.Contains(stdout, "memori context resume --agent <agent-id>") {
		t.Fatalf("expected resume guidance to stay scoped to the issue session, got:\n%s", stdout)
	}
}

func TestIssueNextScopesResumeAndPressureToRecommendedIssueSession(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "memori-cli-issue-next-scope.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer s.Close()

	ctx := context.Background()
	if err := s.Initialize(ctx, store.InitializeParams{IssueKeyPrefix: "mem"}); err != nil {
		t.Fatalf("initialize store: %v", err)
	}
	for _, issueID := range []string{"mem-b222230", "mem-b222231"} {
		if _, _, _, err := s.CreateIssue(ctx, store.CreateIssueParams{
			IssueID:   issueID,
			Type:      "task",
			Title:     "Issue next scope",
			Actor:     "test",
			CommandID: "cmd-create-" + issueID,
		}); err != nil {
			t.Fatalf("create issue %s: %v", issueID, err)
		}
	}
	if _, _, _, err := s.UpdateIssueStatus(ctx, store.UpdateIssueStatusParams{
		IssueID:   "mem-b222230",
		Status:    "inprogress",
		Actor:     "test",
		CommandID: "cmd-issue-next-scope-progress-1",
	}); err != nil {
		t.Fatalf("set issue A inprogress: %v", err)
	}
	packet, err := s.BuildRehydratePacket(ctx, store.BuildPacketParams{
		Scope:     "issue",
		ScopeID:   "mem-b222230",
		Actor:     "test",
		CommandID: "cmd-issue-next-scope-packet-1",
	})
	if err != nil {
		t.Fatalf("build issue packet: %v", err)
	}
	if _, _, err := s.CheckpointSession(ctx, store.CheckpointSessionParams{
		SessionID: "sess-next-scope-a",
		IssueID:   "mem-b222230",
		Trigger:   "manual",
		Actor:     "test",
		CommandID: "cmd-issue-next-scope-checkpoint-a-1",
	}); err != nil {
		t.Fatalf("checkpoint issue A: %v", err)
	}
	if _, err := s.SummarizeSession(ctx, store.SummarizeSessionParams{
		SessionID: "sess-next-scope-a",
		Note:      "issue A summary",
		Actor:     "test",
		CommandID: "cmd-issue-next-scope-summarize-a-1",
	}); err != nil {
		t.Fatalf("summarize issue A: %v", err)
	}
	if _, _, _, err := s.UseRehydratePacket(ctx, store.UsePacketParams{
		AgentID:   "agent-next-scope-1",
		PacketID:  packet.PacketID,
		Actor:     "test",
		CommandID: "cmd-issue-next-scope-use-a-1",
	}); err != nil {
		t.Fatalf("use packet for issue A: %v", err)
	}
	if _, _, err := s.CheckpointSession(ctx, store.CheckpointSessionParams{
		SessionID: "sess-next-scope-b",
		IssueID:   "mem-b222231",
		Trigger:   "manual",
		Actor:     "test",
		CommandID: "cmd-issue-next-scope-checkpoint-b-1",
	}); err != nil {
		t.Fatalf("checkpoint issue B: %v", err)
	}

	stdout, stderr, err := runMemoriForTest("issue", "next", "--db", dbPath, "--agent", "agent-next-scope-1")
	if err != nil {
		t.Fatalf("issue next scoped continuity: %v\nstderr: %s", err, stderr)
	}
	mustContain(t, stdout, "Latest open session for this issue sess-next-scope-a has summary")
	mustContain(t, stdout, "memori context resume --session sess-next-scope-a --agent agent-next-scope-1")
	if strings.Contains(stdout, "sess-next-scope-b") {
		t.Fatalf("expected issue next continuity surfaces to ignore another issue's session, got:\n%s", stdout)
	}
}

func TestIssueUpdateInProgressKeepsSessionsScopedPerIssue(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "memori-cli-issue-update-start-scope.db")
	if _, stderr, err := runMemoriForTest("init", "--db", dbPath, "--issue-prefix", "mem", "--json"); err != nil {
		t.Fatalf("init db: %v\nstderr: %s", err, stderr)
	}
	for _, issueKey := range []string{"mem-b222228", "mem-b222229"} {
		if _, stderr, err := runMemoriForTest(
			"issue", "create",
			"--db", dbPath,
			"--key", issueKey,
			"--type", "task",
			"--title", "Issue start scope",
			"--command-id", "cmd-"+issueKey+"-create-1",
			"--json",
		); err != nil {
			t.Fatalf("issue create %s: %v\nstderr: %s", issueKey, err, stderr)
		}
	}

	stdout, stderr, err := runMemoriForTest(
		"issue", "update",
		"--db", dbPath,
		"--key", "mem-b222228",
		"--status", "inprogress",
		"--agent", "agent-start-scope-1",
		"--command-id", "cmd-start-scope-issue-a-inprogress-1",
	)
	if err != nil {
		t.Fatalf("issue A inprogress: %v\nstderr: %s", err, stderr)
	}
	if strings.Contains(stdout, "Continuing open session already tracking this issue") {
		t.Fatalf("did not expect issue A to reuse an existing issue session, got:\n%s", stdout)
	}

	ctx := context.Background()
	s, _, err := openInitializedStore(ctx, dbPath)
	if err != nil {
		t.Fatalf("open store after issue A: %v", err)
	}
	issueASession, found, err := s.LatestOpenSessionForIssue(ctx, "mem-b222228")
	s.Close()
	if err != nil {
		t.Fatalf("latest open session for issue A: %v", err)
	}
	if !found {
		t.Fatal("expected open session for issue A")
	}

	stdout, stderr, err = runMemoriForTest(
		"issue", "update",
		"--db", dbPath,
		"--key", "mem-b222229",
		"--status", "inprogress",
		"--agent", "agent-start-scope-2",
		"--command-id", "cmd-start-scope-issue-b-inprogress-1",
	)
	if err != nil {
		t.Fatalf("issue B inprogress: %v\nstderr: %s", err, stderr)
	}
	if strings.Contains(stdout, "Continued open session "+issueASession.SessionID) {
		t.Fatalf("expected issue B to get its own continuity session, got:\n%s", stdout)
	}

	s, _, err = openInitializedStore(ctx, dbPath)
	if err != nil {
		t.Fatalf("reopen store after issue B: %v", err)
	}
	issueAAfter, found, err := s.LatestOpenSessionForIssue(ctx, "mem-b222228")
	if err != nil {
		s.Close()
		t.Fatalf("latest open session for issue A after issue B start: %v", err)
	}
	if !found {
		s.Close()
		t.Fatal("expected issue A to keep its open session after issue B started")
	}
	issueBSession, found, err := s.LatestOpenSessionForIssue(ctx, "mem-b222229")
	s.Close()
	if err != nil {
		t.Fatalf("latest open session for issue B: %v", err)
	}
	if !found {
		t.Fatal("expected open session for issue B")
	}
	if issueAAfter.SessionID != issueASession.SessionID {
		t.Fatalf("expected issue A session to remain %s, got %s", issueASession.SessionID, issueAAfter.SessionID)
	}
	if issueBSession.SessionID == issueASession.SessionID {
		t.Fatalf("expected issue B to get a distinct session, both used %s", issueBSession.SessionID)
	}
}
