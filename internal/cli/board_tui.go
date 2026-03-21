package cli

import (
	"context"
	"io"
	"strings"
	"time"

	"github.com/willbastian/memori/internal/store"
)

type boardTUITicker interface {
	channel() <-chan time.Time
	stop()
}

type boardTimeTicker struct {
	*time.Ticker
}

func (ticker boardTimeTicker) channel() <-chan time.Time {
	return ticker.C
}

func (ticker boardTimeTicker) stop() {
	ticker.Ticker.Stop()
}

var (
	boardTUIEnterRawMode  = boardEnterRawMode
	boardTUITerminalSize  = boardTerminalSize
	boardTUIBuildSnapshot = buildBoardSnapshot
	boardTUIBuildAudit    = func(ctx context.Context, s *store.Store, issueID, agent string) (store.ContinuityAuditSnapshot, error) {
		if s == nil || strings.TrimSpace(issueID) == "" {
			return store.ContinuityAuditSnapshot{}, nil
		}
		return s.ContinuityAuditSnapshot(ctx, store.ContinuityAuditSnapshotParams{
			IssueID: issueID,
			AgentID: agent,
		})
	}
	boardTUINow = func() time.Time {
		return time.Now().UTC()
	}
	boardTUINewTicker = func(interval time.Duration) boardTUITicker {
		return boardTimeTicker{Ticker: time.NewTicker(interval)}
	}
)

// TODO(mem-5ece68e): split terminal control, input wiring, and render-loop setup
// behind injectable adapters so the Darwin interactive loop can be covered
// without PTY-driven tests before board_tui.go is decomposed.
func runBoardTUI(ctx context.Context, s *store.Store, agent string, interval time.Duration, out io.Writer) error {
	restore, err := boardTUIEnterRawMode()
	if err != nil {
		return err
	}
	defer restore()

	_, _ = io.WriteString(out, "\x1b[?1049h\x1b[?25l")
	defer func() {
		_, _ = io.WriteString(out, "\x1b[?25h\x1b[?1049l")
	}()

	width, height := boardTUITerminalSize(out)
	snapshot, err := boardTUIBuildSnapshot(ctx, s, agent, boardTUINow())
	if err != nil {
		return err
	}
	model := newBoardTUIModel(snapshot, width, height)
	model, err = boardApplyContinuityAudit(ctx, s, agent, model)
	if err != nil {
		return err
	}

	renderFrame := func() error {
		frame := renderBoardTUI(model, shouldUseColor(out))
		_, _ = io.WriteString(out, frame)
		return nil
	}
	if err := renderFrame(); err != nil {
		return err
	}

	keyCh := make(chan boardKeyInput, 8)
	errCh := make(chan error, 1)
	boardTUIReadInputs(keyCh, errCh)

	ticker := boardTUINewTicker(interval)
	defer ticker.stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-errCh:
			if err == io.EOF {
				return nil
			}
			return err
		case input := <-keyCh:
			quit := false
			model, quit = boardHandleInput(model, input)
			if quit {
				return nil
			}
			model, err = boardApplyContinuityAudit(ctx, s, agent, model)
			if err != nil {
				return err
			}
			if err := renderFrame(); err != nil {
				return err
			}
		case <-ticker.channel():
			width, height = boardTUITerminalSize(out)
			snapshot, err = boardTUIBuildSnapshot(ctx, s, agent, boardTUINow())
			if err != nil {
				return err
			}
			model = boardApplySnapshot(model, snapshot, width, height)
			model, err = boardApplyContinuityAudit(ctx, s, agent, model)
			if err != nil {
				return err
			}
			if err := renderFrame(); err != nil {
				return err
			}
		}
	}
}

func boardApplyContinuityAudit(ctx context.Context, s *store.Store, agent string, model boardTUIModel) (boardTUIModel, error) {
	if s == nil || strings.TrimSpace(model.selectedIssue) == "" {
		model.audit = store.ContinuityAuditSnapshot{}
		return model, nil
	}
	audit, err := boardTUIBuildAudit(ctx, s, model.selectedIssue, agent)
	if err != nil {
		return model, err
	}
	model.audit = audit
	return model, nil
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
