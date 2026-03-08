package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/willbastian/memori/internal/store"
)

type boardSnapshotEnvelope struct {
	Command string `json:"command"`
	Data    struct {
		Snapshot struct {
			Agent   string `json:"agent"`
			Summary struct {
				Total      int `json:"total"`
				Todo       int `json:"todo"`
				InProgress int `json:"in_progress"`
				Blocked    int `json:"blocked"`
				Done       int `json:"done"`
			} `json:"summary"`
			Active []struct {
				Issue struct {
					ID string `json:"id"`
				} `json:"issue"`
			} `json:"active"`
			Blocked []struct {
				Issue struct {
					ID string `json:"id"`
				} `json:"issue"`
			} `json:"blocked"`
			Ready []struct {
				Issue struct {
					ID string `json:"id"`
				} `json:"issue"`
			} `json:"ready"`
			LikelyNext []struct {
				Issue struct {
					ID string `json:"id"`
				} `json:"issue"`
				Reasons []string `json:"reasons"`
			} `json:"likely_next"`
		} `json:"snapshot"`
	} `json:"data"`
}

func TestBoardCommandJSONShowsStatusBucketsAndContinuityRanking(t *testing.T) {
	t.Parallel()

	dbPath := seedBoardSnapshotTestDB(t)

	stdout, stderr, err := runMemoriForTest("board", "--db", dbPath, "--agent", "agent-board-1", "--json")
	if err != nil {
		t.Fatalf("board json: %v\nstderr: %s", err, stderr)
	}

	var resp boardSnapshotEnvelope
	if err := json.Unmarshal([]byte(stdout), &resp); err != nil {
		t.Fatalf("decode board json: %v\nstdout: %s", err, stdout)
	}
	if resp.Command != "board" {
		t.Fatalf("expected board command, got %q", resp.Command)
	}
	if resp.Data.Snapshot.Agent != "agent-board-1" {
		t.Fatalf("expected agent-board-1 in snapshot, got %q", resp.Data.Snapshot.Agent)
	}
	if resp.Data.Snapshot.Summary.Total != 4 || resp.Data.Snapshot.Summary.Todo != 2 || resp.Data.Snapshot.Summary.InProgress != 1 || resp.Data.Snapshot.Summary.Blocked != 1 || resp.Data.Snapshot.Summary.Done != 0 {
		t.Fatalf("unexpected board summary: %+v", resp.Data.Snapshot.Summary)
	}
	if len(resp.Data.Snapshot.Active) != 1 || resp.Data.Snapshot.Active[0].Issue.ID != "mem-a121212" {
		t.Fatalf("unexpected active bucket: %+v", resp.Data.Snapshot.Active)
	}
	if len(resp.Data.Snapshot.Blocked) != 1 || resp.Data.Snapshot.Blocked[0].Issue.ID != "mem-d454545" {
		t.Fatalf("unexpected blocked bucket: %+v", resp.Data.Snapshot.Blocked)
	}
	if len(resp.Data.Snapshot.Ready) != 2 || resp.Data.Snapshot.Ready[0].Issue.ID != "mem-b343434" {
		t.Fatalf("unexpected ready bucket: %+v", resp.Data.Snapshot.Ready)
	}
	if len(resp.Data.Snapshot.LikelyNext) == 0 || resp.Data.Snapshot.LikelyNext[0].Issue.ID != "mem-b343434" {
		t.Fatalf("expected continuity-rich todo issue ranked first, got %+v", resp.Data.Snapshot.LikelyNext)
	}

	reasons := strings.Join(resp.Data.Snapshot.LikelyNext[0].Reasons, "\n")
	for _, want := range []string{
		"matches the agent's active focus for resume",
		"agent already holds the latest recovery packet",
		"has 1 open loop(s) that need continuity",
		"1 required gate(s) are failing",
		"available issue packet is stale",
	} {
		if !strings.Contains(reasons, want) {
			t.Fatalf("expected likely-next reasons to contain %q, got:\n%s", want, reasons)
		}
	}
}

func TestBoardCommandHumanOutputShowsSections(t *testing.T) {
	t.Parallel()

	dbPath := seedBoardSnapshotTestDB(t)

	stdout, stderr, err := runMemoriForTest("board", "--db", dbPath, "--agent", "agent-board-1")
	if err != nil {
		t.Fatalf("board human output: %v\nstderr: %s", err, stderr)
	}

	for _, want := range []string{
		"memori board",
		"Summary:",
		"Next:",
		"Active (1):",
		"Blocked (1):",
		"Ready (2):",
		"mem-a121212 Baseline active task",
		"mem-d454545 Blocked bug",
		"mem-b343434 Continuity-heavy task [s300,focus,packet,loop,+5 more]",
		"Next:",
		"memori board --watch",
	} {
		if !strings.Contains(stdout, want) {
			t.Fatalf("expected board output to contain %q, got:\n%s", want, stdout)
		}
	}
}

func TestRunBoardLoopRendersUntilContextCancelled(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	ctx, cancel := context.WithCancel(context.Background())
	renders := 0

	err := runBoardLoop(ctx, &out, time.Millisecond, func() (string, string, error) {
		renders++
		if renders == 2 {
			cancel()
		}
		return fmt.Sprintf("frame-%d\n", minInt(renders, 1)), "same", nil
	})
	if err != nil {
		t.Fatalf("run board loop: %v", err)
	}
	if renders != 2 {
		t.Fatalf("expected exactly 2 renders before cancel, got %d", renders)
	}
	if got := out.String(); got != "frame-1\n" {
		t.Fatalf("expected change-only output, got:\n%s", got)
	}
}

func seedBoardSnapshotTestDB(t *testing.T) string {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), "memori-cli-board.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer s.Close()

	ctx := context.Background()
	if err := s.Initialize(ctx, store.InitializeParams{IssueKeyPrefix: "mem"}); err != nil {
		t.Fatalf("initialize store: %v", err)
	}

	createIssue := func(id, issueType, title, actor, commandID string) {
		t.Helper()
		if _, _, _, err := s.CreateIssue(ctx, store.CreateIssueParams{
			IssueID:   id,
			Type:      issueType,
			Title:     title,
			Actor:     actor,
			CommandID: commandID,
		}); err != nil {
			t.Fatalf("create issue %s: %v", id, err)
		}
	}

	updateStatus := func(id, status, actor, commandID string) {
		t.Helper()
		if _, _, _, err := s.UpdateIssueStatus(ctx, store.UpdateIssueStatusParams{
			IssueID:   id,
			Status:    status,
			Actor:     actor,
			CommandID: commandID,
		}); err != nil {
			t.Fatalf("update issue %s to %s: %v", id, status, err)
		}
	}

	createIssue("mem-a121212", "task", "Baseline active task", "test", "cmd-board-create-1")
	createIssue("mem-b343434", "task", "Continuity-heavy task", "test", "cmd-board-create-2")
	createIssue("mem-c565656", "task", "Fresh follow-up task", "test", "cmd-board-create-3")
	createIssue("mem-d454545", "bug", "Blocked bug", "test", "cmd-board-create-4")

	updateStatus("mem-a121212", "inprogress", "test", "cmd-board-progress-1")

	if _, _, err := s.CreateGateTemplate(ctx, store.CreateGateTemplateParams{
		TemplateID:     "board-quality",
		Version:        1,
		AppliesTo:      []string{"task"},
		DefinitionJSON: `{"gates":[{"id":"build","kind":"check","required":true,"criteria":{"command":"echo board-quality"}}]}`,
		Actor:          "human:alice",
		CommandID:      "cmd-board-template-1",
	}); err != nil {
		t.Fatalf("create gate template: %v", err)
	}
	if _, _, err := s.InstantiateGateSet(ctx, store.InstantiateGateSetParams{
		IssueID:      "mem-b343434",
		TemplateRefs: []string{"board-quality@1"},
		Actor:        "test",
		CommandID:    "cmd-board-gset-1",
	}); err != nil {
		t.Fatalf("instantiate gate set: %v", err)
	}
	if _, _, err := s.LockGateSet(ctx, store.LockGateSetParams{
		IssueID:   "mem-b343434",
		Actor:     "test",
		CommandID: "cmd-board-lock-1",
	}); err != nil {
		t.Fatalf("lock gate set: %v", err)
	}

	packet, err := s.BuildRehydratePacket(ctx, store.BuildPacketParams{
		Scope:     "issue",
		ScopeID:   "mem-b343434",
		Actor:     "test",
		CommandID: "cmd-board-packet-1",
	})
	if err != nil {
		t.Fatalf("build issue packet: %v", err)
	}
	if _, _, _, err := s.EvaluateGate(ctx, store.EvaluateGateParams{
		IssueID:      "mem-b343434",
		GateID:       "build",
		Result:       "FAIL",
		EvidenceRefs: []string{"ci://run/board-1"},
		Actor:        "test",
		CommandID:    "cmd-board-gate-fail-1",
	}); err != nil {
		t.Fatalf("evaluate gate fail: %v", err)
	}
	if _, _, _, err := s.UseRehydratePacket(ctx, store.UsePacketParams{
		AgentID:   "agent-board-1",
		PacketID:  packet.PacketID,
		Actor:     "test",
		CommandID: "cmd-board-packet-use-1",
	}); err != nil {
		t.Fatalf("use issue packet: %v", err)
	}

	updateStatus("mem-d454545", "blocked", "test", "cmd-board-blocked-1")

	return dbPath
}
