package cli

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/willbastian/memori/internal/store"
)

type stubBoardProgram struct {
	run func() (tea.Model, error)
}

func (program stubBoardProgram) Run() (tea.Model, error) {
	return program.run()
}

func boardTUITestSnapshot(title string) boardSnapshot {
	return boardSnapshot{
		Ready: []boardIssueRow{
			{Issue: boardTestIssue("mem-a111111", "Task", "Todo", title)},
		},
	}
}

func withStubbedBoardTUIRuntime(t *testing.T, buildSnapshot func(context.Context, *store.Store, string, time.Time) (boardSnapshot, error), buildAudit func(context.Context, *store.Store, string, string) (store.ContinuityAuditSnapshot, error), newProgram func(tea.Model, ...tea.ProgramOption) boardTUIProgram) {
	t.Helper()

	originalTerminalSize := boardTUITerminalSize
	originalBuildSnapshot := boardTUIBuildSnapshot
	originalBuildAudit := boardTUIBuildAudit
	originalNewProgram := boardTUINewProgram
	originalInput := boardTUIInput
	originalNow := boardTUINow
	originalSupportsInteractive := boardTUISupportsInteractive

	t.Cleanup(func() {
		boardTUITerminalSize = originalTerminalSize
		boardTUIBuildSnapshot = originalBuildSnapshot
		boardTUIBuildAudit = originalBuildAudit
		boardTUINewProgram = originalNewProgram
		boardTUIInput = originalInput
		boardTUINow = originalNow
		boardTUISupportsInteractive = originalSupportsInteractive
	})

	boardTUITerminalSize = func(io.Writer) (int, int) {
		return 120, 28
	}
	boardTUIInput = func() io.Reader {
		return bytes.NewBufferString("")
	}
	boardTUINow = func() time.Time {
		return time.Date(2026, time.March, 22, 17, 0, 0, 0, time.UTC)
	}
	boardTUISupportsInteractive = func(io.Writer) bool {
		return false
	}
	boardTUIBuildSnapshot = buildSnapshot
	boardTUIBuildAudit = buildAudit
	boardTUINewProgram = newProgram
}

func TestRunBoardTUIPropagatesInitialSnapshotErrors(t *testing.T) {
	wantErr := errors.New("snapshot unavailable")
	withStubbedBoardTUIRuntime(
		t,
		func(context.Context, *store.Store, string, time.Time) (boardSnapshot, error) {
			return boardSnapshot{}, wantErr
		},
		func(context.Context, *store.Store, string, string) (store.ContinuityAuditSnapshot, error) {
			return store.ContinuityAuditSnapshot{}, nil
		},
		func(tea.Model, ...tea.ProgramOption) boardTUIProgram {
			t.Fatal("did not expect program creation when initial snapshot fails")
			return nil
		},
	)

	err := runBoardTUI(context.Background(), nil, "agent-board", time.Second, &bytes.Buffer{})
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected snapshot error %v, got %v", wantErr, err)
	}
}

func TestRunBoardTUIConstructsBubbleTeaModel(t *testing.T) {
	var captured tea.Model
	withStubbedBoardTUIRuntime(
		t,
		func(context.Context, *store.Store, string, time.Time) (boardSnapshot, error) {
			return boardTUITestSnapshot("Ready one"), nil
		},
		func(context.Context, *store.Store, string, string) (store.ContinuityAuditSnapshot, error) {
			return store.ContinuityAuditSnapshot{}, nil
		},
		func(model tea.Model, _ ...tea.ProgramOption) boardTUIProgram {
			captured = model
			return stubBoardProgram{
				run: func() (tea.Model, error) {
					return model, nil
				},
			}
		},
	)

	if err := runBoardTUI(context.Background(), nil, "agent-board", time.Second, &bytes.Buffer{}); err != nil {
		t.Fatalf("run board TUI: %v", err)
	}

	model, ok := captured.(boardTeaModel)
	if !ok {
		t.Fatalf("expected Bubble Tea model, got %T", captured)
	}
	if model.state.selectedIssue != "mem-a111111" {
		t.Fatalf("expected initial selection to target first issue, got %+v", model.state)
	}
	if view := model.View(); !bytes.Contains([]byte(view), []byte("MEMORI BOARD")) || !bytes.Contains([]byte(view), []byte("Ready one")) {
		t.Fatalf("expected initial view to render board content, got:\n%s", view)
	}
}

func TestRunBoardTUIReturnsFinalModelError(t *testing.T) {
	wantErr := errors.New("audit failed")
	withStubbedBoardTUIRuntime(
		t,
		func(context.Context, *store.Store, string, time.Time) (boardSnapshot, error) {
			return boardTUITestSnapshot("Ready one"), nil
		},
		func(context.Context, *store.Store, string, string) (store.ContinuityAuditSnapshot, error) {
			return store.ContinuityAuditSnapshot{}, nil
		},
		func(tea.Model, ...tea.ProgramOption) boardTUIProgram {
			return stubBoardProgram{
				run: func() (tea.Model, error) {
					return boardTeaModel{err: wantErr}, nil
				},
			}
		},
	)

	err := runBoardTUI(context.Background(), nil, "agent-board", time.Second, &bytes.Buffer{})
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected final model error %v, got %v", wantErr, err)
	}
}

func TestRunBoardTUITreatsContextCancellationAsCleanExit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	withStubbedBoardTUIRuntime(
		t,
		func(context.Context, *store.Store, string, time.Time) (boardSnapshot, error) {
			return boardTUITestSnapshot("Ready one"), nil
		},
		func(context.Context, *store.Store, string, string) (store.ContinuityAuditSnapshot, error) {
			return store.ContinuityAuditSnapshot{}, nil
		},
		func(tea.Model, ...tea.ProgramOption) boardTUIProgram {
			return stubBoardProgram{
				run: func() (tea.Model, error) {
					return nil, tea.ErrProgramKilled
				},
			}
		},
	)

	if err := runBoardTUI(ctx, nil, "agent-board", time.Second, &bytes.Buffer{}); err != nil {
		t.Fatalf("expected canceled context to exit cleanly, got %v", err)
	}
}

func TestBoardTeaModelHandlesResizeRefreshAndQuit(t *testing.T) {
	model := boardTeaModel{
		ctx:      context.Background(),
		interval: time.Second,
		state:    newBoardTUIModel(boardTUITestSnapshot("Ready one"), 80, 20),
	}

	updatedModel, cmd := model.Update(tea.WindowSizeMsg{Width: 132, Height: 33})
	updated := updatedModel.(boardTeaModel)
	if updated.state.width != 132 || updated.state.height != 33 {
		t.Fatalf("expected resize to update dimensions, got %+v", updated.state)
	}

	updatedModel, cmd = updated.Update(boardRefreshTickMsg{})
	if cmd == nil {
		t.Fatal("expected refresh tick to schedule commands")
	}

	updatedModel, cmd = updatedModel.(boardTeaModel).Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'q'}})
	if cmd == nil {
		t.Fatal("expected quit key to return a quit command")
	}
}

func TestBoardTeaModelInitStartsRefreshAuditAndSpinner(t *testing.T) {
	model := boardTeaModel{
		ctx:      context.Background(),
		store:    nil,
		agent:    "agent-board",
		interval: time.Second,
		state:    newBoardTUIModel(boardTUITestSnapshot("Ready one"), 80, 20),
	}

	cmd := model.Init()
	if cmd == nil {
		t.Fatal("expected init to schedule commands")
	}
}

func TestBoardLoadCommandsReturnExpectedMessages(t *testing.T) {
	originalSnapshot := boardTUIBuildSnapshot
	originalAudit := boardTUIBuildAudit
	originalNow := boardTUINow
	t.Cleanup(func() {
		boardTUIBuildSnapshot = originalSnapshot
		boardTUIBuildAudit = originalAudit
		boardTUINow = originalNow
	})

	boardTUINow = func() time.Time {
		return time.Date(2026, time.March, 22, 19, 30, 0, 0, time.UTC)
	}
	boardTUIBuildSnapshot = func(context.Context, *store.Store, string, time.Time) (boardSnapshot, error) {
		return boardTUITestSnapshot("Loaded snapshot"), nil
	}
	boardTUIBuildAudit = func(context.Context, *store.Store, string, string) (store.ContinuityAuditSnapshot, error) {
		return store.ContinuityAuditSnapshot{
			Resolution: store.ContinuityResolution{Status: "fresh"},
		}, nil
	}

	snapshotMsg := boardLoadSnapshotCmd(context.Background(), nil, "agent-board", 7)()
	loadedSnapshot, ok := snapshotMsg.(boardSnapshotLoadedMsg)
	if !ok || loadedSnapshot.err != nil || loadedSnapshot.requestID != 7 || loadedSnapshot.snapshot.Ready[0].Issue.Title != "Loaded snapshot" {
		t.Fatalf("unexpected snapshot msg %#v", snapshotMsg)
	}

	auditMsg := boardLoadAuditCmd(context.Background(), nil, "agent-board", "mem-a111111", 9)()
	loadedAudit, ok := auditMsg.(boardAuditLoadedMsg)
	if !ok || loadedAudit.err != nil || loadedAudit.requestID != 9 || loadedAudit.issueID != "mem-a111111" || loadedAudit.audit.Resolution.Status != "fresh" {
		t.Fatalf("unexpected audit msg %#v", auditMsg)
	}

	if _, ok := boardScheduleSpinner()().(boardSpinnerTickMsg); !ok {
		t.Fatalf("expected spinner cmd to emit boardSpinnerTickMsg")
	}
	if msg := boardExpireToastCmd(17)(); msg.(boardToastExpiredMsg).id != 17 {
		t.Fatalf("expected toast expiry id 17, got %#v", msg)
	}
	if boardScheduleRefresh(0) != nil {
		t.Fatal("expected zero refresh interval to return nil cmd")
	}
	if _, ok := boardScheduleRefresh(time.Millisecond)().(boardRefreshTickMsg); !ok {
		t.Fatal("expected scheduled refresh cmd to emit boardRefreshTickMsg")
	}
}

func TestBoardTeaModelRefreshFailureBecomesToastInsteadOfFatalError(t *testing.T) {
	model := boardTeaModel{
		ctx:      context.Background(),
		interval: time.Second,
		state:    newBoardTUIModel(boardTUITestSnapshot("Ready one"), 80, 20),
	}
	model.state.snapshotLoad = boardSucceedAsyncLoad(model.state.snapshotLoad, time.Date(2026, time.March, 22, 18, 0, 0, 0, time.UTC))

	updatedModel, cmd := model.Update(boardRefreshTickMsg{})
	updated := updatedModel.(boardTeaModel)
	if cmd == nil || !updated.state.snapshotLoad.loading || !updated.state.snapshotLoad.stale {
		t.Fatalf("expected refresh tick to mark snapshot loading/stale, got %+v", updated.state.snapshotLoad)
	}
	requestID := updated.state.activeSnapshotRequestID

	updatedModel, cmd = updated.Update(boardSnapshotLoadedMsg{requestID: requestID, err: errors.New("snapshot unavailable")})
	updated = updatedModel.(boardTeaModel)
	if cmd == nil {
		t.Fatal("expected failure toast expiry command")
	}
	if updated.err != nil {
		t.Fatalf("expected refresh error to stay non-fatal, got %v", updated.err)
	}
	if updated.state.toast.message == "" || !strings.Contains(updated.state.toast.message, "Board refresh failed") {
		t.Fatalf("expected refresh failure toast, got %+v", updated.state.toast)
	}
	if updated.state.snapshotLoad.loading || updated.state.snapshotLoad.err == "" {
		t.Fatalf("expected failed snapshot state to stay visible, got %+v", updated.state.snapshotLoad)
	}
}

func TestBoardTeaModelAuditFailureBecomesToastInsteadOfFatalError(t *testing.T) {
	model := boardTeaModel{
		ctx:      context.Background(),
		interval: time.Second,
		state:    newBoardTUIModel(boardTUITestSnapshot("Ready one"), 80, 20),
	}
	model.state, _ = boardStartAuditLoad(model.state)

	updatedModel, cmd := model.Update(boardAuditLoadedMsg{
		requestID: model.state.activeAuditRequestID,
		issueID:   model.state.selectedIssue,
		err:       errors.New("audit unavailable"),
	})
	updated := updatedModel.(boardTeaModel)
	if cmd == nil {
		t.Fatal("expected audit failure toast expiry command")
	}
	if updated.err != nil {
		t.Fatalf("expected audit error to stay non-fatal, got %v", updated.err)
	}
	if updated.state.toast.message == "" || !strings.Contains(updated.state.toast.message, "Continuity refresh failed") {
		t.Fatalf("expected audit failure toast, got %+v", updated.state.toast)
	}
	if updated.state.auditLoad.loading || updated.state.auditLoad.err == "" {
		t.Fatalf("expected failed audit state to stay visible, got %+v", updated.state.auditLoad)
	}
}

func TestBoardTeaModelUpdateHandlesSuccessSpinnerAndToastLifecycle(t *testing.T) {
	model := boardTeaModel{
		ctx:      context.Background(),
		interval: time.Second,
		state:    newBoardTUIModel(boardTUITestSnapshot("Ready one"), 80, 20),
	}
	model.state, _ = boardStartSnapshotLoad(model.state)

	updatedModel, cmd := model.Update(boardSnapshotLoadedMsg{
		requestID: model.state.activeSnapshotRequestID,
		snapshot: boardSnapshot{
			Ready: []boardIssueRow{
				{Issue: boardTestIssue("mem-b222222", "Task", "Todo", "Ready two")},
			},
		},
	})
	updated := updatedModel.(boardTeaModel)
	if cmd == nil {
		t.Fatal("expected snapshot success to queue follow-up audit load")
	}
	if updated.state.selectedIssue != "mem-b222222" || updated.state.snapshotLoad.loading || updated.state.snapshotLoad.stale {
		t.Fatalf("expected snapshot success to refresh selection and clear loading, got %+v", updated.state)
	}
	if !updated.state.auditLoad.loading {
		t.Fatalf("expected snapshot success to start audit load, got %+v", updated.state.auditLoad)
	}

	updatedModel, cmd = updated.Update(boardAuditLoadedMsg{
		requestID: updated.state.activeAuditRequestID,
		issueID:   "mem-b222222",
		audit:     store.ContinuityAuditSnapshot{Resolution: store.ContinuityResolution{Status: "fresh"}},
	})
	updated = updatedModel.(boardTeaModel)
	if cmd != nil {
		t.Fatalf("expected audit success to finish without extra command, got %#v", cmd)
	}
	if updated.state.audit.Resolution.Status != "fresh" || updated.state.auditLoad.loading || updated.state.auditLoad.err != "" {
		t.Fatalf("expected audit success to populate state, got %+v", updated.state.auditLoad)
	}

	updated.state.snapshotLoad = boardBeginAsyncLoad(updated.state.snapshotLoad)
	updatedModel, cmd = updated.Update(boardSpinnerTickMsg{})
	updated = updatedModel.(boardTeaModel)
	if cmd == nil || updated.state.spinnerFrame != 1 {
		t.Fatalf("expected spinner tick to advance frame, got frame=%d cmd=%v", updated.state.spinnerFrame, cmd != nil)
	}

	updated.state = boardSetToast(updated.state, boardToastToneInfo, "toast")
	updatedModel, cmd = updated.Update(boardToastExpiredMsg{id: updated.state.toast.id})
	updated = updatedModel.(boardTeaModel)
	if cmd != nil {
		t.Fatalf("expected toast expiry to finish without command, got %#v", cmd)
	}
	if updated.state.toast.message != "" {
		t.Fatalf("expected toast expiry to clear toast, got %+v", updated.state.toast)
	}
}

func TestBoardTeaModelUpdateHandlesIgnoredAuditAndCanceledErrors(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	model := boardTeaModel{
		ctx:      ctx,
		interval: time.Second,
		state:    newBoardTUIModel(boardTUITestSnapshot("Ready one"), 80, 20),
	}

	updatedModel, cmd := model.Update(boardAuditLoadedMsg{
		requestID: 99,
		issueID:   "mem-other",
		audit:     store.ContinuityAuditSnapshot{Resolution: store.ContinuityResolution{Status: "stale"}},
	})
	updated := updatedModel.(boardTeaModel)
	if cmd != nil {
		t.Fatalf("expected mismatched audit update to do nothing, got %#v", cmd)
	}
	if updated.state.audit.Resolution.Status != "" {
		t.Fatalf("expected mismatched audit to be ignored, got %+v", updated.state.audit)
	}

	cancel()
	updated.state, _ = boardStartSnapshotLoad(updated.state)
	updatedModel, cmd = updated.Update(boardSnapshotLoadedMsg{requestID: updated.state.activeSnapshotRequestID, err: errors.New("boom")})
	updated = updatedModel.(boardTeaModel)
	if cmd == nil {
		t.Fatal("expected canceled snapshot error to quit")
	}

	updated.state.snapshotLoad = boardAsyncLoadState{}
	updatedModel, cmd = updated.Update(boardSpinnerTickMsg{})
	updated = updatedModel.(boardTeaModel)
	if cmd != nil || updated.state.spinnerFrame != 0 {
		t.Fatalf("expected idle spinner tick to do nothing, got frame=%d cmd=%v", updated.state.spinnerFrame, cmd != nil)
	}
}

func TestBoardTeaModelUpdateCoversResizeInputRefreshAndDefaultMessages(t *testing.T) {
	model := boardTeaModel{
		ctx:      context.Background(),
		interval: 2 * time.Second,
		state: newBoardTUIModel(boardSnapshot{
			Ready: []boardIssueRow{
				{Issue: boardTestIssue("mem-a111111", "Task", "Todo", "Ready one")},
				{Issue: boardTestIssue("mem-b222222", "Task", "Todo", "Ready two")},
			},
		}, 60, 12),
	}
	model.state.lane = boardLaneReady
	model.state = boardNormalizeModel(model.state)

	updatedModel, cmd := model.Update(tea.WindowSizeMsg{Width: 140, Height: 40})
	updated := updatedModel.(boardTeaModel)
	if cmd != nil || updated.state.width != 140 || updated.state.height != 40 {
		t.Fatalf("expected resize to update dimensions without command, got state=%+v cmd=%v", updated.state, cmd != nil)
	}

	updatedModel, cmd = updated.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'j'}})
	updated = updatedModel.(boardTeaModel)
	if cmd == nil || updated.state.selectedIssue != "mem-b222222" || !updated.state.auditLoad.loading {
		t.Fatalf("expected movement key to change selection and start audit load, got state=%+v cmd=%v", updated.state, cmd != nil)
	}

	updatedModel, cmd = updated.Update(boardRefreshTickMsg{})
	updated = updatedModel.(boardTeaModel)
	if cmd == nil || !updated.state.snapshotLoad.loading {
		t.Fatalf("expected refresh tick to start snapshot load, got state=%+v cmd=%v", updated.state.snapshotLoad, cmd != nil)
	}

	updatedModel, cmd = updated.Update(struct{}{})
	updated = updatedModel.(boardTeaModel)
	if cmd != nil {
		t.Fatalf("expected default message path to return nil cmd, got %#v", cmd)
	}

	updatedModel, cmd = updated.Update(tea.KeyMsg{Type: tea.KeyF1})
	updated = updatedModel.(boardTeaModel)
	if cmd != nil {
		t.Fatalf("expected unmapped key to return nil cmd, got %#v", cmd)
	}
}

func TestBoardTeaModelIgnoresStaleSnapshotReplies(t *testing.T) {
	model := boardTeaModel{
		ctx:      context.Background(),
		interval: time.Second,
		state:    newBoardTUIModel(boardTUITestSnapshot("Ready one"), 80, 20),
	}
	model.state.snapshotLoad = boardSucceedAsyncLoad(model.state.snapshotLoad, time.Date(2026, time.March, 22, 18, 0, 0, 0, time.UTC))
	model.state, _ = boardStartSnapshotLoad(model.state)
	staleRequestID := model.state.activeSnapshotRequestID
	model.state, _ = boardStartSnapshotLoad(model.state)
	currentRequestID := model.state.activeSnapshotRequestID

	updatedModel, cmd := model.Update(boardSnapshotLoadedMsg{
		requestID: staleRequestID,
		snapshot: boardSnapshot{
			Ready: []boardIssueRow{
				{Issue: boardTestIssue("mem-stale", "Task", "Todo", "Stale snapshot")},
			},
		},
	})
	updated := updatedModel.(boardTeaModel)
	if cmd != nil {
		t.Fatalf("expected stale snapshot reply to be ignored, got %#v", cmd)
	}
	if updated.state.selectedIssue != model.state.selectedIssue {
		t.Fatalf("expected stale snapshot reply to leave selection unchanged, got %+v", updated.state)
	}
	if updated.state.activeSnapshotRequestID != currentRequestID {
		t.Fatalf("expected current snapshot request %d to stay active, got %d", currentRequestID, updated.state.activeSnapshotRequestID)
	}

	updatedModel, cmd = updated.Update(boardSnapshotLoadedMsg{
		requestID: staleRequestID,
		err:       errors.New("stale failure"),
	})
	updated = updatedModel.(boardTeaModel)
	if cmd != nil {
		t.Fatalf("expected stale snapshot failure to be ignored, got %#v", cmd)
	}
	if updated.state.toast.message != "" {
		t.Fatalf("expected stale snapshot failure to avoid toast, got %+v", updated.state.toast)
	}
}

func TestBoardTeaModelIgnoresStaleAuditReplies(t *testing.T) {
	model := boardTeaModel{
		ctx:      context.Background(),
		interval: time.Second,
		state:    newBoardTUIModel(boardTUITestSnapshot("Ready one"), 80, 20),
	}
	model.state, _ = boardStartAuditLoad(model.state)
	staleRequestID := model.state.activeAuditRequestID
	model.state, _ = boardStartAuditLoad(model.state)
	currentRequestID := model.state.activeAuditRequestID

	updatedModel, cmd := model.Update(boardAuditLoadedMsg{
		requestID: staleRequestID,
		issueID:   model.state.selectedIssue,
		audit:     store.ContinuityAuditSnapshot{Resolution: store.ContinuityResolution{Status: "stale"}},
	})
	updated := updatedModel.(boardTeaModel)
	if cmd != nil {
		t.Fatalf("expected stale audit reply to be ignored, got %#v", cmd)
	}
	if updated.state.audit.Resolution.Status != "" {
		t.Fatalf("expected stale audit reply to leave audit unchanged, got %+v", updated.state.audit)
	}
	if updated.state.activeAuditRequestID != currentRequestID {
		t.Fatalf("expected current audit request %d to stay active, got %d", currentRequestID, updated.state.activeAuditRequestID)
	}

	updatedModel, cmd = updated.Update(boardAuditLoadedMsg{
		requestID: staleRequestID,
		issueID:   model.state.selectedIssue,
		err:       errors.New("stale audit failure"),
	})
	updated = updatedModel.(boardTeaModel)
	if cmd != nil {
		t.Fatalf("expected stale audit failure to be ignored, got %#v", cmd)
	}
	if updated.state.toast.message != "" {
		t.Fatalf("expected stale audit failure to avoid toast, got %+v", updated.state.toast)
	}
}

func TestBoardTUIShouldUseColorHonorsInteractiveDefaults(t *testing.T) {
	t.Setenv("MEMORI_COLOR", "")
	t.Setenv("NO_COLOR", "")
	t.Setenv("CLICOLOR", "")
	t.Setenv("CLICOLOR_FORCE", "")
	t.Setenv("FORCE_COLOR", "")
	t.Setenv("TERM", "xterm-256color")

	original := boardTUISupportsInteractive
	t.Cleanup(func() {
		boardTUISupportsInteractive = original
	})

	boardTUISupportsInteractive = func(io.Writer) bool {
		return true
	}
	if !boardTUIShouldUseColor(&bytes.Buffer{}) {
		t.Fatal("expected interactive board TUI to default to color")
	}

	t.Setenv("NO_COLOR", "1")
	if boardTUIShouldUseColor(&bytes.Buffer{}) {
		t.Fatal("expected NO_COLOR to still disable board TUI color")
	}
}
