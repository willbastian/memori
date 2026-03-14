package cli

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/willbastian/memori/internal/store"
)

type stubBoardTicker struct {
	c       chan time.Time
	stopped bool
}

func (ticker *stubBoardTicker) channel() <-chan time.Time {
	return ticker.c
}

func (ticker *stubBoardTicker) stop() {
	ticker.stopped = true
}

func boardTUITestSnapshot(title string) boardSnapshot {
	return boardSnapshot{
		Ready: []boardIssueRow{
			{Issue: boardTestIssue("mem-a111111", "Task", "Todo", title)},
		},
	}
}

func withStubbedBoardTUIRuntime(t *testing.T, ticker *stubBoardTicker) {
	t.Helper()

	originalEnterRawMode := boardTUIEnterRawMode
	originalTerminalSize := boardTUITerminalSize
	originalBuildSnapshot := boardTUIBuildSnapshot
	originalReadInputs := boardTUIReadInputs
	originalNow := boardTUINow
	originalNewTicker := boardTUINewTicker

	t.Cleanup(func() {
		boardTUIEnterRawMode = originalEnterRawMode
		boardTUITerminalSize = originalTerminalSize
		boardTUIBuildSnapshot = originalBuildSnapshot
		boardTUIReadInputs = originalReadInputs
		boardTUINow = originalNow
		boardTUINewTicker = originalNewTicker
	})

	boardTUITerminalSize = func(io.Writer) (int, int) {
		return 120, 28
	}
	boardTUINow = func() time.Time {
		return time.Date(2026, time.March, 14, 12, 0, 0, 0, time.UTC)
	}
	boardTUINewTicker = func(time.Duration) boardTUITicker {
		return ticker
	}
}

func TestRunBoardTUIPropagatesRawModeErrors(t *testing.T) {
	ticker := &stubBoardTicker{c: make(chan time.Time, 1)}
	withStubbedBoardTUIRuntime(t, ticker)

	wantErr := errors.New("raw mode unavailable")
	boardTUIEnterRawMode = func() (func(), error) {
		return nil, wantErr
	}

	err := runBoardTUI(context.Background(), nil, "agent-board", time.Second, &bytes.Buffer{})
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected raw mode error %v, got %v", wantErr, err)
	}
	if ticker.stopped {
		t.Fatal("did not expect ticker to start when raw mode setup fails")
	}
}

func TestRunBoardTUIRendersInitialFrameAndQuitsOnInput(t *testing.T) {
	ticker := &stubBoardTicker{c: make(chan time.Time, 1)}
	withStubbedBoardTUIRuntime(t, ticker)

	restoreCalls := 0
	buildCalls := 0
	boardTUIEnterRawMode = func() (func(), error) {
		return func() {
			restoreCalls++
		}, nil
	}
	boardTUIBuildSnapshot = func(context.Context, *store.Store, string, time.Time) (boardSnapshot, error) {
		buildCalls++
		return boardTUITestSnapshot("Ready one"), nil
	}
	boardTUIReadInputs = func(keyCh chan<- boardKeyInput, errCh chan<- error) {
		keyCh <- boardKeyInput{action: boardActionQuit}
	}

	var out bytes.Buffer
	err := runBoardTUI(context.Background(), nil, "agent-board", time.Second, &out)
	if err != nil {
		t.Fatalf("run board TUI: %v", err)
	}
	if restoreCalls != 1 {
		t.Fatalf("expected restore to run once, got %d", restoreCalls)
	}
	if buildCalls != 1 {
		t.Fatalf("expected one initial snapshot build, got %d", buildCalls)
	}
	if !ticker.stopped {
		t.Fatal("expected ticker to stop when loop exits")
	}

	rendered := out.String()
	for _, want := range []string{
		"\x1b[?1049h\x1b[?25l",
		"MEMORI BOARD",
		"Ready one",
		"\x1b[?25h\x1b[?1049l",
	} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("expected output to contain %q, got:\n%s", want, rendered)
		}
	}
}

func TestRunBoardTUIRefreshesSnapshotOnTick(t *testing.T) {
	ticker := &stubBoardTicker{c: make(chan time.Time, 1)}
	withStubbedBoardTUIRuntime(t, ticker)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	buildCalls := 0
	boardTUIEnterRawMode = func() (func(), error) {
		return func() {}, nil
	}
	boardTUIBuildSnapshot = func(context.Context, *store.Store, string, time.Time) (boardSnapshot, error) {
		buildCalls++
		if buildCalls == 1 {
			return boardTUITestSnapshot("Ready one"), nil
		}
		cancel()
		return boardTUITestSnapshot("Ready two"), nil
	}
	boardTUIReadInputs = func(chan<- boardKeyInput, chan<- error) {}

	done := make(chan error, 1)
	var out bytes.Buffer
	go func() {
		done <- runBoardTUI(ctx, nil, "agent-board", time.Second, &out)
	}()

	ticker.c <- time.Date(2026, time.March, 14, 12, 0, 1, 0, time.UTC)

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run board TUI: %v", err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("timed out waiting for board TUI refresh")
	}

	if buildCalls != 2 {
		t.Fatalf("expected initial and refresh snapshot builds, got %d", buildCalls)
	}
	if got := strings.Count(out.String(), "MEMORI BOARD"); got != 2 {
		t.Fatalf("expected two rendered frames, got %d in output:\n%s", got, out.String())
	}
	if !strings.Contains(out.String(), "Ready two") {
		t.Fatalf("expected refreshed frame to contain updated issue title, got:\n%s", out.String())
	}
}

func TestRunBoardTUIStopsCleanlyOnEOF(t *testing.T) {
	ticker := &stubBoardTicker{c: make(chan time.Time, 1)}
	withStubbedBoardTUIRuntime(t, ticker)

	boardTUIEnterRawMode = func() (func(), error) {
		return func() {}, nil
	}
	boardTUIBuildSnapshot = func(context.Context, *store.Store, string, time.Time) (boardSnapshot, error) {
		return boardTUITestSnapshot("Ready one"), nil
	}
	boardTUIReadInputs = func(keyCh chan<- boardKeyInput, errCh chan<- error) {
		errCh <- io.EOF
	}

	err := runBoardTUI(context.Background(), nil, "agent-board", time.Second, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("expected EOF to exit cleanly, got %v", err)
	}
	if !ticker.stopped {
		t.Fatal("expected ticker to stop on EOF exit")
	}
}

func TestRunBoardTUIPropagatesRefreshErrors(t *testing.T) {
	ticker := &stubBoardTicker{c: make(chan time.Time, 1)}
	withStubbedBoardTUIRuntime(t, ticker)

	wantErr := errors.New("refresh failed")
	buildCalls := 0
	boardTUIEnterRawMode = func() (func(), error) {
		return func() {}, nil
	}
	boardTUIBuildSnapshot = func(context.Context, *store.Store, string, time.Time) (boardSnapshot, error) {
		buildCalls++
		if buildCalls == 1 {
			return boardTUITestSnapshot("Ready one"), nil
		}
		return boardSnapshot{}, wantErr
	}
	boardTUIReadInputs = func(chan<- boardKeyInput, chan<- error) {}

	done := make(chan error, 1)
	go func() {
		done <- runBoardTUI(context.Background(), nil, "agent-board", time.Second, &bytes.Buffer{})
	}()

	ticker.c <- time.Date(2026, time.March, 14, 12, 0, 1, 0, time.UTC)

	select {
	case err := <-done:
		if !errors.Is(err, wantErr) {
			t.Fatalf("expected refresh error %v, got %v", wantErr, err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("timed out waiting for board TUI refresh failure")
	}
}
