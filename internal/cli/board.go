package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"

	"memori/internal/store"
)

const ansiClearScreen = "\x1b[H\x1b[2J"

type boardData struct {
	Snapshot   boardSnapshot   `json:"snapshot"`
	Counts     boardSummary    `json:"counts"`
	Active     []boardIssueRow `json:"active"`
	Blocked    []boardIssueRow `json:"blocked"`
	Ready      []boardIssueRow `json:"ready"`
	LikelyNext *boardIssueRow  `json:"likely_next,omitempty"`
}

type boardSnapshot struct {
	GeneratedAt string          `json:"generated_at"`
	Agent       string          `json:"agent,omitempty"`
	Summary     boardSummary    `json:"summary"`
	Active      []boardIssueRow `json:"active"`
	Blocked     []boardIssueRow `json:"blocked"`
	Ready       []boardIssueRow `json:"ready"`
	LikelyNext  []boardIssueRow `json:"likely_next"`
}

type boardSummary struct {
	Total      int `json:"total"`
	Todo       int `json:"todo"`
	InProgress int `json:"in_progress"`
	Blocked    int `json:"blocked"`
	Done       int `json:"done"`
}

type boardIssueRow struct {
	Issue   store.Issue `json:"issue"`
	Score   int         `json:"score,omitempty"`
	Reasons []string    `json:"reasons,omitempty"`
}

type boardRenderOptions struct {
	Colors   bool
	Watch    bool
	Interval time.Duration
}

func runBoard(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("board", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dbPath := fs.String("db", defaultDBPath(), "sqlite database path")
	agent := fs.String("agent", "", "optional agent id requesting continuity-aware recommendations")
	watch := fs.Bool("watch", false, "continuously refresh the board")
	interval := fs.Duration("interval", 5*time.Second, "refresh interval when --watch is enabled")
	jsonOut := fs.Bool("json", false, "machine-readable output")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *interval <= 0 {
		return errors.New("--interval must be > 0")
	}
	if *watch && *jsonOut {
		return errors.New("--watch cannot be combined with --json")
	}

	baseCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	ctx, cancel := context.WithTimeout(baseCtx, 5*time.Second)
	s, dbVersion, err := openInitializedStore(ctx, *dbPath)
	cancel()
	if err != nil {
		return err
	}
	defer s.Close()

	renderFrame := func() error {
		snapshot, err := buildBoardSnapshot(baseCtx, s, *agent, time.Now().UTC())
		if err != nil {
			return err
		}
		return renderBoardSnapshot(out, snapshot, boardRenderOptions{
			Colors:   shouldUseColor(out),
			Watch:    *watch,
			Interval: *interval,
		})
	}

	if *jsonOut {
		snapshot, err := buildBoardSnapshot(baseCtx, s, *agent, time.Now().UTC())
		if err != nil {
			return err
		}
		return printJSON(out, jsonEnvelope{
			ResponseSchemaVersion: responseSchemaVersion,
			DBSchemaVersion:       dbVersion,
			Command:               "board",
			Data:                  newBoardData(snapshot),
		})
	}

	if !*watch {
		return renderFrame()
	}

	return runBoardLoop(baseCtx, out, *interval, supportsScreenControl(out), renderFrame)
}

func buildBoardSnapshot(ctx context.Context, s *store.Store, agent string, now time.Time) (boardSnapshot, error) {
	issues, err := s.ListIssues(ctx, store.ListIssuesParams{})
	if err != nil {
		return boardSnapshot{}, err
	}

	agent = strings.TrimSpace(agent)
	nextCandidates := make([]store.IssueNextCandidate, 0)
	next, err := s.NextIssue(ctx, agent)
	switch {
	case err == nil:
		nextCandidates = next.Candidates
	case strings.Contains(err.Error(), "no actionable issues found"):
	default:
		return boardSnapshot{}, err
	}

	scoreByID := make(map[string]store.IssueNextCandidate, len(nextCandidates))
	rankByID := make(map[string]int, len(nextCandidates))
	for idx, candidate := range nextCandidates {
		scoreByID[candidate.Issue.ID] = candidate
		rankByID[candidate.Issue.ID] = idx
	}

	snapshot := boardSnapshot{
		GeneratedAt: now.Format(time.RFC3339),
		Agent:       agent,
	}
	for _, issue := range issues {
		snapshot.Summary.Total++
		switch issue.Status {
		case "Todo":
			snapshot.Summary.Todo++
		case "InProgress":
			snapshot.Summary.InProgress++
		case "Blocked":
			snapshot.Summary.Blocked++
		case "Done":
			snapshot.Summary.Done++
		}

		row := boardIssueRow{Issue: issue}
		if candidate, ok := scoreByID[issue.ID]; ok {
			row.Score = candidate.Score
			row.Reasons = append([]string(nil), candidate.Reasons...)
		}

		switch issue.Status {
		case "InProgress":
			snapshot.Active = append(snapshot.Active, row)
		case "Blocked":
			snapshot.Blocked = append(snapshot.Blocked, row)
		case "Todo":
			snapshot.Ready = append(snapshot.Ready, row)
		}
	}

	for _, candidate := range nextCandidates {
		snapshot.LikelyNext = append(snapshot.LikelyNext, boardIssueRow{
			Issue:   candidate.Issue,
			Score:   candidate.Score,
			Reasons: append([]string(nil), candidate.Reasons...),
		})
	}

	sortBoardRows(snapshot.Active, rankByID)
	sortBoardRows(snapshot.Ready, rankByID)
	sortBoardRows(snapshot.Blocked, rankByID)

	return snapshot, nil
}

func sortBoardRows(rows []boardIssueRow, rankByID map[string]int) {
	sort.SliceStable(rows, func(i, j int) bool {
		leftRank, leftFound := rankByID[rows[i].Issue.ID]
		rightRank, rightFound := rankByID[rows[j].Issue.ID]
		if leftFound != rightFound {
			return leftFound
		}
		if leftFound && rightFound && leftRank != rightRank {
			return leftRank < rightRank
		}
		if rows[i].Issue.UpdatedAt != rows[j].Issue.UpdatedAt {
			return rows[i].Issue.UpdatedAt < rows[j].Issue.UpdatedAt
		}
		return rows[i].Issue.ID < rows[j].Issue.ID
	})
}

func renderBoardSnapshot(out io.Writer, snapshot boardSnapshot, opts boardRenderOptions) error {
	ui := newTextUI(out)
	ui.heading("memori board")
	ui.field("Updated", snapshot.GeneratedAt)
	if snapshot.Agent != "" {
		ui.field("Agent", snapshot.Agent)
	}
	ui.field("Summary", formatBoardSummary(snapshot.Summary, ui.colors))
	if opts.Watch {
		ui.field("Refresh", opts.Interval.String())
		ui.note("Watching for updates. Press Ctrl+C to exit.")
	}
	ui.blank()

	renderBoardSection(ui, "Active Work", snapshot.Active, "No active work.", false)
	renderBoardSection(ui, "Blocked Work", snapshot.Blocked, "No blocked work.", false)
	renderBoardSection(ui, "Ready Work", snapshot.Ready, "No ready work.", false)
	renderBoardSection(ui, "Likely Next Work", snapshot.LikelyNext, "No continuity-ranked work is ready yet.", true)

	if !opts.Watch {
		nextCommand := "memori issue next"
		if snapshot.Agent != "" {
			nextCommand += " --agent " + snapshot.Agent
		}
		ui.nextSteps(
			"memori board --watch",
			nextCommand,
		)
	}
	return nil
}

func renderBoardSection(ui textUI, label string, rows []boardIssueRow, emptyMessage string, showReasons bool) {
	ui.section(label)
	if len(rows) == 0 {
		ui.bullet(emptyMessage)
		ui.blank()
		return
	}
	for _, row := range rows {
		ui.bullet(formatBoardIssueRow(row, ui.colors))
		if showReasons && len(row.Reasons) > 0 {
			_, _ = fmt.Fprintf(ui.out, "  why: %s\n", strings.Join(compactReasons(row.Reasons, 3), "; "))
		}
	}
	ui.blank()
}

func formatBoardIssueRow(row boardIssueRow, colors bool) string {
	line := formatIssueLine(row.Issue, colors)
	if row.Score > 0 {
		line += fmt.Sprintf(" (score=%d)", row.Score)
	}
	return line
}

func compactReasons(reasons []string, limit int) []string {
	if limit <= 0 || len(reasons) <= limit {
		return append([]string(nil), reasons...)
	}
	trimmed := append([]string(nil), reasons[:limit]...)
	trimmed = append(trimmed, fmt.Sprintf("+%d more", len(reasons)-limit))
	return trimmed
}

func newBoardData(snapshot boardSnapshot) boardData {
	data := boardData{
		Snapshot: snapshot,
		Counts:   snapshot.Summary,
		Active:   append([]boardIssueRow(nil), snapshot.Active...),
		Blocked:  append([]boardIssueRow(nil), snapshot.Blocked...),
		Ready:    append([]boardIssueRow(nil), snapshot.Ready...),
	}
	if len(snapshot.LikelyNext) > 0 {
		row := snapshot.LikelyNext[0]
		data.LikelyNext = &row
	}
	return data
}

func formatBoardSummary(summary boardSummary, colors bool) string {
	parts := []string{
		fmt.Sprintf("total=%d", summary.Total),
		fmt.Sprintf("%s=%d", colorize(colors, colorForStatus("InProgress"), "in_progress"), summary.InProgress),
		fmt.Sprintf("%s=%d", colorize(colors, colorForStatus("Blocked"), "blocked"), summary.Blocked),
		fmt.Sprintf("%s=%d", colorize(colors, colorForStatus("Todo"), "todo"), summary.Todo),
		fmt.Sprintf("%s=%d", colorize(colors, colorForStatus("Done"), "done"), summary.Done),
	}
	return strings.Join(parts, ", ")
}

func supportsScreenControl(out io.Writer) bool {
	if strings.EqualFold(strings.TrimSpace(os.Getenv("TERM")), "dumb") {
		return false
	}
	file, ok := out.(*os.File)
	if !ok {
		return false
	}
	info, err := file.Stat()
	if err != nil {
		return false
	}
	return (info.Mode() & os.ModeCharDevice) != 0
}

func runBoardLoop(ctx context.Context, out io.Writer, interval time.Duration, clear bool, render func() error) error {
	renderFrame := func() error {
		if clear {
			_, _ = io.WriteString(out, ansiClearScreen)
		}
		return render()
	}

	if err := renderFrame(); err != nil {
		return err
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := renderFrame(); err != nil {
				return err
			}
		}
	}
}
