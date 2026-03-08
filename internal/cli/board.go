package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"memori/internal/store"
)

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
	Width    int
}

func runBoard(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("board", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dbPath := fs.String("db", defaultDBPath(), "sqlite database path")
	agent := fs.String("agent", "", "optional agent id requesting continuity-aware recommendations")
	watch := fs.Bool("watch", false, "continuously refresh the board")
	interactive := fs.Bool("interactive", false, "force the interactive TUI")
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
	if *interactive && *jsonOut {
		return errors.New("--interactive cannot be combined with --json")
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

	renderFrame := func() (string, string, error) {
		snapshot, err := buildBoardSnapshot(baseCtx, s, *agent, time.Now().UTC())
		if err != nil {
			return "", "", err
		}
		rendered, err := renderBoardSnapshot(snapshot, boardRenderOptions{
			Colors:   shouldUseColor(out),
			Watch:    *watch,
			Interval: *interval,
			Width:    boardRenderWidth(),
		})
		if err != nil {
			return "", "", err
		}
		return rendered, boardSnapshotSignature(snapshot), nil
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

	if (*interactive || boardSupportsInteractive(out)) && !*watch {
		return runBoardTUI(baseCtx, s, *agent, *interval, out)
	}

	if !*watch {
		rendered, _, err := renderFrame()
		if err != nil {
			return err
		}
		_, _ = io.WriteString(out, rendered)
		return nil
	}

	return runBoardLoop(baseCtx, out, *interval, renderFrame)
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

func renderBoardSnapshot(snapshot boardSnapshot, opts boardRenderOptions) (string, error) {
	var out bytes.Buffer
	ui := newTextUI(&out)
	width := opts.Width
	if width <= 0 {
		width = 80
	}

	header := "memori board"
	if opts.Watch {
		header = fmt.Sprintf("%s [%s]", header, snapshot.GeneratedAt)
	}
	ui.heading(header)
	boardField(ui, "Summary", formatBoardSummary(snapshot.Summary, ui.colors), width)
	if snapshot.Agent != "" {
		boardField(ui, "Agent", snapshot.Agent, width)
	}
	if opts.Watch {
		boardField(ui, "Refresh", opts.Interval.String()+" (change-only)", width)
	}
	ui.blank()

	renderBoardNext(ui, snapshot.LikelyNext, width)
	renderBoardSection(ui, "Active", snapshot.Active, boardSectionLimit(width), width)
	renderBoardSection(ui, "Blocked", snapshot.Blocked, boardSectionLimit(width), width)
	renderBoardSection(ui, "Ready", snapshot.Ready, boardSectionLimit(width), width)

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
	return out.String(), nil
}

func renderBoardNext(ui textUI, rows []boardIssueRow, width int) {
	ui.section("Next")
	if len(rows) == 0 {
		ui.bullet("No continuity-ranked work is ready yet.")
		ui.blank()
		return
	}
	for _, row := range rows[:minInt(len(rows), boardLikelyNextLimit(width))] {
		ui.bullet(truncateBoardLine(formatBoardNextRow(row), width-2))
	}
	ui.blank()
}

func renderBoardSection(ui textUI, label string, rows []boardIssueRow, limit, width int) {
	ui.section(fmt.Sprintf("%s (%d)", label, len(rows)))
	if len(rows) == 0 {
		ui.bullet("none")
		ui.blank()
		return
	}
	show := minInt(len(rows), limit)
	for _, row := range rows[:show] {
		ui.bullet(truncateBoardLine(formatBoardIssueRow(row), width-2))
	}
	if len(rows) > show {
		ui.bullet(fmt.Sprintf("+%d more", len(rows)-show))
	}
	ui.blank()
}

func formatBoardIssueRow(row boardIssueRow) string {
	return fmt.Sprintf("%s %s", row.Issue.ID, row.Issue.Title)
}

func formatBoardNextRow(row boardIssueRow) string {
	line := formatBoardIssueRow(row)
	tags := boardReasonTags(row.Reasons)
	if row.Score > 0 {
		tags = append([]string{fmt.Sprintf("s%d", row.Score)}, tags...)
	}
	if len(tags) > 0 {
		line += " [" + strings.Join(tags, ",") + "]"
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

func orderBoardReasons(reasons []string) []string {
	ordered := append([]string(nil), reasons...)
	sort.SliceStable(ordered, func(i, j int) bool {
		leftWeight := boardReasonWeight(ordered[i])
		rightWeight := boardReasonWeight(ordered[j])
		if leftWeight != rightWeight {
			return leftWeight > rightWeight
		}
		return i < j
	})
	return ordered
}

func boardReasonWeight(reason string) int {
	switch {
	case strings.Contains(reason, "matches the agent's active focus"):
		return 100
	case strings.Contains(reason, "agent already holds the latest recovery packet"):
		return 95
	case strings.Contains(reason, "open loop"):
		return 90
	case strings.Contains(reason, "required gate"):
		return 85
	case strings.Contains(reason, "issue packet"):
		return 80
	case strings.Contains(reason, "packet is stale"):
		return 75
	case strings.Contains(reason, "priority P"):
		return 50
	case strings.Contains(reason, "in-progress work"):
		return 40
	case strings.Contains(reason, "todo work"):
		return 35
	case strings.Contains(reason, "implementation-ready"):
		return 30
	case strings.Contains(reason, "operational value"):
		return 25
	case strings.Contains(reason, "can start immediately"):
		return 20
	default:
		return 10
	}
}

func boardReasonTags(reasons []string) []string {
	ordered := orderBoardReasons(reasons)
	tags := make([]string, 0, len(ordered))
	seen := make(map[string]struct{}, len(ordered))
	for _, reason := range ordered {
		tag := boardReasonTag(reason)
		if tag == "" {
			continue
		}
		if _, ok := seen[tag]; ok {
			continue
		}
		seen[tag] = struct{}{}
		tags = append(tags, tag)
	}
	return compactReasons(tags, 3)
}

func boardReasonTag(reason string) string {
	switch {
	case strings.Contains(reason, "matches the agent's active focus"):
		return "focus"
	case strings.Contains(reason, "agent already holds the latest recovery packet"):
		return "packet"
	case strings.Contains(reason, "open loop"):
		return "loop"
	case strings.Contains(reason, "required gate(s) are failing"):
		return "fail"
	case strings.Contains(reason, "required gate(s) are blocked"):
		return "blocked"
	case strings.Contains(reason, "required gate(s) still need evaluation"):
		return "gates"
	case strings.Contains(reason, "issue packet is ready") || strings.Contains(reason, "fresh issue packet"):
		return "fresh"
	case strings.Contains(reason, "packet is stale"):
		return "stale"
	case strings.Contains(reason, "priority P0"):
		return "p0"
	case strings.Contains(reason, "priority P1"):
		return "p1"
	case strings.Contains(reason, "priority P2"):
		return "p2"
	case strings.Contains(reason, "in-progress work"):
		return "active"
	case strings.Contains(reason, "todo work"):
		return "todo"
	case strings.Contains(reason, "implementation-ready"):
		return "task"
	case strings.Contains(reason, "operational value"):
		return "bug"
	case strings.Contains(reason, "can start immediately"):
		return "standalone"
	default:
		return ""
	}
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
		fmt.Sprintf("%s=%d", colorize(colors, colorForStatus("InProgress"), "ip"), summary.InProgress),
		fmt.Sprintf("%s=%d", colorize(colors, colorForStatus("Blocked"), "blocked"), summary.Blocked),
		fmt.Sprintf("%s=%d", colorize(colors, colorForStatus("Todo"), "todo"), summary.Todo),
		fmt.Sprintf("%s=%d", colorize(colors, colorForStatus("Done"), "done"), summary.Done),
	}
	return strings.Join(parts, ", ")
}

func boardField(ui textUI, label, value string, width int) {
	available := width - len(label) - 2
	if available < 8 {
		available = 8
	}
	ui.field(label, truncateBoardLine(value, available))
}

func boardRenderWidth() int {
	if raw := strings.TrimSpace(os.Getenv("COLUMNS")); raw != "" {
		if value, err := strconv.Atoi(raw); err == nil && value > 20 {
			return value
		}
	}
	return 80
}

func boardSectionLimit(width int) int {
	switch {
	case width < 50:
		return 2
	case width < 80:
		return 3
	default:
		return 5
	}
}

func boardLikelyNextLimit(width int) int {
	switch {
	case width < 50:
		return 1
	case width < 80:
		return 2
	default:
		return 3
	}
}

func truncateBoardLine(value string, width int) string {
	value = strings.TrimSpace(value)
	if width <= 0 || len(value) <= width {
		return value
	}
	if width <= 3 {
		return value[:width]
	}
	return value[:width-3] + "..."
}

func boardSnapshotSignature(snapshot boardSnapshot) string {
	normalized := snapshot
	normalized.GeneratedAt = ""
	payload, err := json.Marshal(normalized)
	if err != nil {
		return ""
	}
	return string(payload)
}

func runBoardLoop(ctx context.Context, out io.Writer, interval time.Duration, render func() (string, string, error)) error {
	var lastSignature string
	renderFrame := func(first bool) error {
		rendered, signature, err := render()
		if err != nil {
			return err
		}
		if !first && signature == lastSignature {
			return nil
		}
		if !first {
			_, _ = io.WriteString(out, "\n")
		}
		lastSignature = signature
		_, _ = io.WriteString(out, rendered)
		return nil
	}

	if err := renderFrame(true); err != nil {
		return err
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := renderFrame(false); err != nil {
				return err
			}
		}
	}
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
