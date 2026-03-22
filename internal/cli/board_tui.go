package cli

import (
	"context"
	"errors"
	"io"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/willbastian/memori/internal/store"
)

type boardTUIProgram interface {
	Run() (tea.Model, error)
}

type boardRefreshTickMsg struct{}

type boardSnapshotLoadedMsg struct {
	snapshot boardSnapshot
	err      error
}

type boardAuditLoadedMsg struct {
	issueID string
	audit   store.ContinuityAuditSnapshot
	err     error
}

type boardTeaModel struct {
	ctx      context.Context
	store    *store.Store
	agent    string
	interval time.Duration
	colors   bool
	state    boardTUIModel
	err      error
}

var (
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
	boardTUIInput = func() io.Reader {
		return boardInput()
	}
	boardTUINewProgram = func(model tea.Model, opts ...tea.ProgramOption) boardTUIProgram {
		return tea.NewProgram(model, opts...)
	}
)

func runBoardTUI(ctx context.Context, s *store.Store, agent string, interval time.Duration, out io.Writer) error {
	width, height := boardTUITerminalSize(out)
	snapshot, err := boardTUIBuildSnapshot(ctx, s, agent, boardTUINow())
	if err != nil {
		return err
	}

	state := newBoardTUIModel(snapshot, width, height)
	audit, err := boardTUIBuildAudit(ctx, s, state.selectedIssue, agent)
	if err != nil {
		return err
	}
	state.audit = audit

	model := boardTeaModel{
		ctx:      ctx,
		store:    s,
		agent:    agent,
		interval: interval,
		colors:   shouldUseColor(out),
		state:    state,
	}

	program := boardTUINewProgram(model,
		tea.WithContext(ctx),
		tea.WithInput(boardTUIInput()),
		tea.WithOutput(out),
		tea.WithAltScreen(),
		tea.WithoutSignals(),
	)

	finalModel, err := program.Run()
	if errors.Is(err, tea.ErrProgramKilled) && ctx.Err() != nil {
		return nil
	}
	if err != nil {
		return err
	}
	final, ok := finalModel.(boardTeaModel)
	if ok && final.err != nil {
		return final.err
	}
	return nil
}

func (model boardTeaModel) Init() tea.Cmd {
	return boardScheduleRefresh(model.interval)
}

func (model boardTeaModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		model.state.width = maxInt(msg.Width, 24)
		model.state.height = maxInt(msg.Height, 10)
		model.state = boardNormalizeModel(model.state)
		return model, nil
	case tea.KeyMsg:
		input, ok := boardKeyInputFromKeyMsg(msg)
		if !ok {
			return model, nil
		}
		nextState, quit := boardHandleInput(model.state, input)
		model.state = nextState
		if quit {
			return model, tea.Quit
		}
		return model, boardLoadAuditCmd(model.ctx, model.store, model.agent, model.state.selectedIssue)
	case boardRefreshTickMsg:
		return model, tea.Batch(
			boardLoadSnapshotCmd(model.ctx, model.store, model.agent),
			boardScheduleRefresh(model.interval),
		)
	case boardSnapshotLoadedMsg:
		if msg.err != nil {
			if model.ctx.Err() != nil {
				return model, tea.Quit
			}
			model.err = msg.err
			return model, tea.Quit
		}
		model.state = boardApplySnapshot(model.state, msg.snapshot, model.state.width, model.state.height)
		return model, boardLoadAuditCmd(model.ctx, model.store, model.agent, model.state.selectedIssue)
	case boardAuditLoadedMsg:
		if msg.err != nil {
			if model.ctx.Err() != nil {
				return model, tea.Quit
			}
			model.err = msg.err
			return model, tea.Quit
		}
		if strings.TrimSpace(msg.issueID) == strings.TrimSpace(model.state.selectedIssue) {
			model.state.audit = msg.audit
		}
		return model, nil
	default:
		return model, nil
	}
}

func (model boardTeaModel) View() string {
	return renderBoardTUI(model.state, model.colors)
}

func boardScheduleRefresh(interval time.Duration) tea.Cmd {
	if interval <= 0 {
		return nil
	}
	return tea.Tick(interval, func(time.Time) tea.Msg {
		return boardRefreshTickMsg{}
	})
}

func boardLoadSnapshotCmd(ctx context.Context, s *store.Store, agent string) tea.Cmd {
	return func() tea.Msg {
		snapshot, err := boardTUIBuildSnapshot(ctx, s, agent, boardTUINow())
		return boardSnapshotLoadedMsg{snapshot: snapshot, err: err}
	}
}

func boardLoadAuditCmd(ctx context.Context, s *store.Store, agent, issueID string) tea.Cmd {
	return func() tea.Msg {
		audit, err := boardTUIBuildAudit(ctx, s, issueID, agent)
		return boardAuditLoadedMsg{issueID: issueID, audit: audit, err: err}
	}
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
