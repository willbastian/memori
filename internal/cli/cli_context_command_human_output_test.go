package cli

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestContextHumanOutputAutoResolvesSessionWhenOmitted(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "memori-cli-context-human-auto-session.db")
	if _, stderr, err := runMemoriForTest("init", "--db", dbPath, "--issue-prefix", "mem", "--json"); err != nil {
		t.Fatalf("init db: %v\nstderr: %s", err, stderr)
	}

	stdout, stderr, err := runMemoriForTest(
		"context", "checkpoint",
		"--db", dbPath,
		"--command-id", "cmd-cli-human-auto-session-checkpoint-1",
	)
	if err != nil {
		t.Fatalf("context checkpoint auto session: %v\nstderr: %s", err, stderr)
	}
	sessionID := sessionIDFromHumanOutput(t, stdout)
	mustContain(t, stdout, "Note No --session supplied; started new session "+sessionID+".")
	mustContain(t, stdout, "OK Created session checkpoint "+sessionID)
	mustContain(t, stdout, "memori context rehydrate --session "+sessionID)

	stdout, stderr, err = runMemoriForTest(
		"context", "checkpoint",
		"--db", dbPath,
		"--command-id", "cmd-cli-human-auto-session-checkpoint-2",
	)
	if err != nil {
		t.Fatalf("context checkpoint latest open session: %v\nstderr: %s", err, stderr)
	}
	mustContain(t, stdout, "Note No --session supplied; continuing latest open session "+sessionID+". Pass --session to start a parallel session.")
	mustContain(t, stdout, "OK Updated session checkpoint "+sessionID)

	stdout, stderr, err = runMemoriForTest(
		"context", "rehydrate",
		"--db", dbPath,
	)
	if err != nil {
		t.Fatalf("context rehydrate latest open session: %v\nstderr: %s", err, stderr)
	}
	mustContain(t, stdout, "Note No --session supplied; rehydrating latest open session "+sessionID+".")
	mustContain(t, stdout, "OK Rehydrated session "+sessionID+" via relevant-chunks-fallback")
}

func TestContextSummarizeAndCloseHumanOutputShowLifecycleGuidance(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "memori-cli-context-human-lifecycle.db")
	if _, stderr, err := runMemoriForTest("init", "--db", dbPath, "--issue-prefix", "mem", "--json"); err != nil {
		t.Fatalf("init db: %v\nstderr: %s", err, stderr)
	}

	if _, stderr, err := runMemoriForTest(
		"context", "checkpoint",
		"--db", dbPath,
		"--session", "sess-human-life-1",
		"--command-id", "cmd-cli-human-life-checkpoint-1",
	); err != nil {
		t.Fatalf("context checkpoint: %v\nstderr: %s", err, stderr)
	}

	stdout, stderr, err := runMemoriForTest(
		"context", "summarize",
		"--db", dbPath,
		"--session", "sess-human-life-1",
		"--note", "paused after review",
		"--command-id", "cmd-cli-human-life-summarize-1",
	)
	if err != nil {
		t.Fatalf("context summarize text: %v\nstderr: %s", err, stderr)
	}
	for _, want := range []string{
		"OK Summarized session sess-human-life-1",
		"Summary Event:",
		"memori context rehydrate --session sess-human-life-1",
		"memori context close --session sess-human-life-1",
	} {
		mustContain(t, stdout, want)
	}

	stdout, stderr, err = runMemoriForTest(
		"context", "close",
		"--db", dbPath,
		"--session", "sess-human-life-1",
		"--reason", "handoff complete",
		"--command-id", "cmd-cli-human-life-close-1",
	)
	if err != nil {
		t.Fatalf("context close text: %v\nstderr: %s", err, stderr)
	}
	for _, want := range []string{
		"OK Closed session sess-human-life-1",
		"Ended At:",
		"memori context rehydrate --session sess-human-life-1",
		"memori context checkpoint",
	} {
		mustContain(t, stdout, want)
	}
}

func TestContextStartAndSaveHumanOutputShowHappyPathGuidance(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "memori-cli-context-human-flow.db")
	if _, stderr, err := runMemoriForTest("init", "--db", dbPath, "--issue-prefix", "mem", "--json"); err != nil {
		t.Fatalf("init db: %v\nstderr: %s", err, stderr)
	}
	if _, stderr, err := runMemoriForTest(
		"issue", "create",
		"--db", dbPath,
		"--key", "mem-f10a002",
		"--type", "task",
		"--title", "Human flow task",
		"--command-id", "cmd-cli-human-flow-create-1",
		"--json",
	); err != nil {
		t.Fatalf("issue create: %v\nstderr: %s", err, stderr)
	}

	stdout, stderr, err := runMemoriForTest(
		"context", "start",
		"--db", dbPath,
		"--issue", "mem-f10a002",
		"--agent", "agent-human-flow-1",
		"--command-id", "cmd-cli-human-flow-start-1",
	)
	if err != nil {
		t.Fatalf("context start text: %v\nstderr: %s", err, stderr)
	}
	for _, want := range []string{
		"OK Started continuity for issue mem-f10a002 and updated focus for agent-human-flow-1",
		"Session:",
		"Issue Packet:",
		"memori board --agent agent-human-flow-1",
		"memori context save --session",
	} {
		mustContain(t, stdout, want)
	}
	sessionID := ""
	for _, line := range strings.Split(stdout, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "Session: ") {
			sessionID = strings.TrimSpace(strings.TrimPrefix(line, "Session: "))
			break
		}
	}
	if sessionID == "" {
		t.Fatalf("expected session id in context start output, got:\n%s", stdout)
	}

	stdout, stderr, err = runMemoriForTest(
		"context", "save",
		"--db", dbPath,
		"--session", sessionID,
		"--note", "ready for handoff",
		"--close",
		"--reason", "handoff complete",
		"--command-id", "cmd-cli-human-flow-save-1",
	)
	if err != nil {
		t.Fatalf("context save text: %v\nstderr: %s", err, stderr)
	}
	for _, want := range []string{
		"OK Saved continuity and closed session " + sessionID,
		"Summary Event:",
		"Session Packet:",
		"memori context rehydrate --session " + sessionID,
		"memori context checkpoint",
	} {
		mustContain(t, stdout, want)
	}
}

func TestContextResumeHumanOutputCoversFallbackAndFocusedResume(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "memori-cli-context-human-resume.db")
	if _, stderr, err := runMemoriForTest("init", "--db", dbPath, "--issue-prefix", "mem", "--json"); err != nil {
		t.Fatalf("init db: %v\nstderr: %s", err, stderr)
	}
	if _, stderr, err := runMemoriForTest(
		"context", "checkpoint",
		"--db", dbPath,
		"--session", "sess-human-resume-1",
		"--command-id", "cmd-cli-human-resume-checkpoint-1",
	); err != nil {
		t.Fatalf("context checkpoint: %v\nstderr: %s", err, stderr)
	}

	stdout, stderr, err := runMemoriForTest(
		"context", "resume",
		"--db", dbPath,
		"--session", "sess-human-resume-1",
	)
	if err != nil {
		t.Fatalf("context resume fallback: %v\nstderr: %s", err, stderr)
	}
	mustContain(t, stdout, "OK Resumed session sess-human-resume-1 via relevant-chunks-fallback")
	mustContain(t, stdout, "Note No saved session packet was available; synthesized resume context from recent session chunks.")

	if _, stderr, err := runMemoriForTest(
		"issue", "create",
		"--db", dbPath,
		"--key", "mem-a222222",
		"--type", "task",
		"--title", "Focused resume target",
		"--command-id", "cmd-cli-human-resume-create-1",
		"--json",
	); err != nil {
		t.Fatalf("issue create: %v\nstderr: %s", err, stderr)
	}
	if _, stderr, err := runMemoriForTest(
		"context", "start",
		"--db", dbPath,
		"--issue", "mem-a222222",
		"--session", "sess-human-resume-2",
		"--command-id", "cmd-cli-human-resume-start-1",
		"--json",
	); err != nil {
		t.Fatalf("context start: %v\nstderr: %s", err, stderr)
	}
	if _, stderr, err := runMemoriForTest(
		"context", "save",
		"--db", dbPath,
		"--session", "sess-human-resume-2",
		"--command-id", "cmd-cli-human-resume-save-1",
		"--json",
	); err != nil {
		t.Fatalf("context save: %v\nstderr: %s", err, stderr)
	}

	stdout, stderr, err = runMemoriForTest(
		"context", "resume",
		"--db", dbPath,
		"--session", "sess-human-resume-2",
		"--agent", "agent-human-resume-1",
		"--command-id", "cmd-cli-human-resume-run-1",
	)
	if err != nil {
		t.Fatalf("context resume focused: %v\nstderr: %s", err, stderr)
	}
	mustContain(t, stdout, "OK Resumed session sess-human-resume-2 via packet and updated focus for agent-human-resume-1")
	mustContain(t, stdout, "Agent: agent-human-resume-1")
	mustContain(t, stdout, "memori issue next --agent agent-human-resume-1")
	mustContain(t, stdout, "memori board --agent agent-human-resume-1")
}
