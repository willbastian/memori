package cli

import (
	"bytes"
	"context"
	"errors"
	"io"
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

	t.Cleanup(func() {
		boardTUITerminalSize = originalTerminalSize
		boardTUIBuildSnapshot = originalBuildSnapshot
		boardTUIBuildAudit = originalBuildAudit
		boardTUINewProgram = originalNewProgram
		boardTUIInput = originalInput
		boardTUINow = originalNow
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
