package cli

import (
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/willbastian/memori/internal/store"
)

type boardTUIProgram interface {
	Run() (tea.Model, error)
}

type boardRefreshTickMsg struct{}
type boardSpinnerTickMsg struct{}
type boardToastExpiredMsg struct {
	id int
}

type boardSnapshotLoadedMsg struct {
	requestID int
	snapshot  boardSnapshot
	err       error
}

type boardAuditLoadedMsg struct {
	requestID int
	issueID   string
	audit     store.ContinuityAuditSnapshot
	err       error
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
	boardTUISupportsInteractive = boardSupportsInteractive
	boardTUIInput               = func() io.Reader {
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
	state.snapshotLoad = boardSucceedAsyncLoad(state.snapshotLoad, boardTUINow())
	if strings.TrimSpace(state.selectedIssue) != "" {
		state, _ = boardStartAuditLoad(state)
	}

	model := boardTeaModel{
		ctx:      ctx,
		store:    s,
		agent:    agent,
		interval: interval,
		colors:   boardTUIShouldUseColor(out),
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
	cmds := []tea.Cmd{boardScheduleRefresh(model.interval)}
	if strings.TrimSpace(model.state.selectedIssue) != "" {
		requestID := model.state.activeAuditRequestID
		cmds = append(cmds,
			boardLoadAuditCmd(model.ctx, model.store, model.agent, model.state.selectedIssue, requestID),
			boardScheduleSpinner(),
		)
	}
	return tea.Batch(cmds...)
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
		previous := model.state
		nextState, quit := boardHandleInput(model.state, input)
		model.state = nextState
		if quit {
			return model, tea.Quit
		}
		cmds := make([]tea.Cmd, 0, 3)
		if previous.selectedIssue != model.state.selectedIssue || input.action == boardActionToggleContinuity {
			requestID := 0
			model.state, requestID = boardStartAuditLoad(model.state)
			cmds = append(cmds, boardLoadAuditCmd(model.ctx, model.store, model.agent, model.state.selectedIssue, requestID))
		}
		if !boardAnyLoading(previous) && boardAnyLoading(model.state) {
			cmds = append(cmds, boardScheduleSpinner())
		}
		if model.state.toast.id != 0 && model.state.toast.id != previous.toast.id {
			cmds = append(cmds, boardExpireToastCmd(model.state.toast.id))
		}
		return model, tea.Batch(cmds...)
	case boardRefreshTickMsg:
		wasLoading := boardAnyLoading(model.state)
		requestID := 0
		model.state, requestID = boardStartSnapshotLoad(model.state)
		cmds := []tea.Cmd{
			boardLoadSnapshotCmd(model.ctx, model.store, model.agent, requestID),
			boardScheduleRefresh(model.interval),
		}
		if !wasLoading {
			cmds = append(cmds, boardScheduleSpinner())
		}
		return model, tea.Batch(cmds...)
	case boardSnapshotLoadedMsg:
		if msg.requestID != model.state.activeSnapshotRequestID {
			return model, nil
		}
		model.state.activeSnapshotRequestID = 0
		if msg.err != nil {
			if model.ctx.Err() != nil {
				return model, tea.Quit
			}
			previousToastID := model.state.toast.id
			model.state.snapshotLoad = boardFailAsyncLoad(model.state.snapshotLoad, msg.err)
			model.state = boardSetToast(model.state, boardToastToneWarn, "Board refresh failed: "+msg.err.Error())
			cmds := make([]tea.Cmd, 0, 1)
			if model.state.toast.id != 0 && model.state.toast.id != previousToastID {
				cmds = append(cmds, boardExpireToastCmd(model.state.toast.id))
			}
			return model, tea.Batch(cmds...)
		}
		wasLoading := boardAnyLoading(model.state)
		model.state = boardApplySnapshot(model.state, msg.snapshot, model.state.width, model.state.height)
		model.state.snapshotLoad = boardSucceedAsyncLoad(model.state.snapshotLoad, boardTUINow())
		cmds := make([]tea.Cmd, 0, 2)
		if strings.TrimSpace(model.state.selectedIssue) != "" {
			requestID := 0
			model.state, requestID = boardStartAuditLoad(model.state)
			cmds = append(cmds, boardLoadAuditCmd(model.ctx, model.store, model.agent, model.state.selectedIssue, requestID))
		}
		if !wasLoading && boardAnyLoading(model.state) {
			cmds = append(cmds, boardScheduleSpinner())
		}
		return model, tea.Batch(cmds...)
	case boardAuditLoadedMsg:
		if msg.requestID != model.state.activeAuditRequestID {
			return model, nil
		}
		model.state.activeAuditRequestID = 0
		if msg.err != nil {
			if model.ctx.Err() != nil {
				return model, tea.Quit
			}
			if strings.TrimSpace(msg.issueID) != strings.TrimSpace(model.state.selectedIssue) {
				return model, nil
			}
			previousToastID := model.state.toast.id
			model.state.auditLoad = boardFailAsyncLoad(model.state.auditLoad, msg.err)
			model.state = boardSetToast(model.state, boardToastToneWarn, "Continuity refresh failed for "+msg.issueID)
			cmds := make([]tea.Cmd, 0, 1)
			if model.state.toast.id != 0 && model.state.toast.id != previousToastID {
				cmds = append(cmds, boardExpireToastCmd(model.state.toast.id))
			}
			return model, tea.Batch(cmds...)
		}
		if strings.TrimSpace(msg.issueID) == strings.TrimSpace(model.state.selectedIssue) {
			model.state.audit = msg.audit
			model.state.auditLoad = boardSucceedAsyncLoad(model.state.auditLoad, boardTUINow())
		}
		return model, nil
	case boardSpinnerTickMsg:
		if !boardAnyLoading(model.state) {
			return model, nil
		}
		model.state.spinnerFrame++
		return model, boardScheduleSpinner()
	case boardToastExpiredMsg:
		model.state = boardClearToast(model.state, msg.id)
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

func boardLoadSnapshotCmd(ctx context.Context, s *store.Store, agent string, requestID int) tea.Cmd {
	return func() tea.Msg {
		snapshot, err := boardTUIBuildSnapshot(ctx, s, agent, boardTUINow())
		return boardSnapshotLoadedMsg{requestID: requestID, snapshot: snapshot, err: err}
	}
}

func boardLoadAuditCmd(ctx context.Context, s *store.Store, agent, issueID string, requestID int) tea.Cmd {
	return func() tea.Msg {
		audit, err := boardTUIBuildAudit(ctx, s, issueID, agent)
		return boardAuditLoadedMsg{requestID: requestID, issueID: issueID, audit: audit, err: err}
	}
}

func boardScheduleSpinner() tea.Cmd {
	return tea.Tick(120*time.Millisecond, func(time.Time) tea.Msg {
		return boardSpinnerTickMsg{}
	})
}

func boardExpireToastCmd(id int) tea.Cmd {
	return tea.Tick(2800*time.Millisecond, func(time.Time) tea.Msg {
		return boardToastExpiredMsg{id: id}
	})
}

func boardTUIShouldUseColor(out io.Writer) bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("MEMORI_COLOR"))) {
	case "always":
		return true
	case "never":
		return false
	}
	if os.Getenv("NO_COLOR") != "" || strings.TrimSpace(os.Getenv("CLICOLOR")) == "0" {
		return false
	}
	if force := strings.TrimSpace(os.Getenv("CLICOLOR_FORCE")); force != "" && force != "0" {
		return true
	}
	if force := strings.TrimSpace(os.Getenv("FORCE_COLOR")); force != "" && force != "0" {
		return true
	}
	if strings.EqualFold(strings.TrimSpace(os.Getenv("TERM")), "dumb") {
		return false
	}
	if boardTUISupportsInteractive(out) {
		return true
	}
	return shouldUseColor(out)
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
