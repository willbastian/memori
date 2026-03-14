package cli

import (
	"context"
	"errors"
	"flag"
	"io"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/willbastian/memori/internal/store"
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
	WontDo     int `json:"wont_do"`
}

type boardIssueRow struct {
	Issue     store.Issue         `json:"issue"`
	Hierarchy boardIssueHierarchy `json:"hierarchy,omitempty"`
	Score     int                 `json:"score,omitempty"`
	Reasons   []string            `json:"reasons,omitempty"`
}

type boardIssueHierarchy struct {
	Depth           int      `json:"depth,omitempty"`
	Path            []string `json:"path,omitempty"`
	AncestorIDs     []string `json:"ancestor_ids,omitempty"`
	ParentID        string   `json:"parent_id,omitempty"`
	ParentTitle     string   `json:"parent_title,omitempty"`
	ParentType      string   `json:"parent_type,omitempty"`
	ParentStatus    string   `json:"parent_status,omitempty"`
	ChildIDs        []string `json:"child_ids,omitempty"`
	ChildCount      int      `json:"child_count,omitempty"`
	DescendantCount int      `json:"descendant_count,omitempty"`
	HasChildren     bool     `json:"has_children,omitempty"`
	SiblingIndex    int      `json:"sibling_index,omitempty"`
	SiblingCount    int      `json:"sibling_count,omitempty"`
}

type boardRenderOptions struct {
	Colors   bool
	Watch    bool
	Interval time.Duration
	Width    int
}

type boardReasonRule struct {
	contains string
	weight   int
	tag      string
}

var boardReasonRules = []boardReasonRule{
	{contains: "matches the agent's active focus", weight: 100, tag: "focus"},
	{contains: "agent already holds the latest recovery packet", weight: 95, tag: "packet"},
	{contains: "open loop", weight: 90, tag: "loop"},
	{contains: "required gate(s) are failing", weight: 85, tag: "fail"},
	{contains: "required gate(s) are blocked", weight: 85, tag: "blocked"},
	{contains: "required gate(s) still need evaluation", weight: 85, tag: "gates"},
	{contains: "issue packet", weight: 80},
	{contains: "packet is stale", weight: 75, tag: "stale"},
	{contains: "issue packet is ready", tag: "fresh"},
	{contains: "fresh issue packet", tag: "fresh"},
	{contains: "priority P0", weight: 50, tag: "p0"},
	{contains: "priority P1", weight: 50, tag: "p1"},
	{contains: "priority P2", weight: 50, tag: "p2"},
	{contains: "in-progress work", weight: 40, tag: "active"},
	{contains: "todo work", weight: 35, tag: "todo"},
	{contains: "implementation-ready", weight: 30, tag: "task"},
	{contains: "operational value", weight: 25, tag: "bug"},
	{contains: "can start immediately", weight: 20, tag: "standalone"},
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
	hierarchyByID := buildBoardHierarchy(issues)

	agent = strings.TrimSpace(agent)
	nextCandidates, err := boardNextCandidates(ctx, s, agent)
	if err != nil {
		return boardSnapshot{}, err
	}
	scoreByID, rankByID := boardCandidateMaps(nextCandidates)

	snapshot := boardSnapshot{
		GeneratedAt: now.Format(time.RFC3339),
		Agent:       agent,
	}
	boardPopulateSnapshotRows(&snapshot, issues, hierarchyByID, scoreByID)
	boardPopulateLikelyNext(&snapshot, nextCandidates, hierarchyByID)
	boardSortSnapshot(&snapshot, rankByID)

	return snapshot, nil
}

func boardNextCandidates(ctx context.Context, s *store.Store, agent string) ([]store.IssueNextCandidate, error) {
	next, err := s.NextIssue(ctx, agent)
	switch {
	case err == nil:
		return next.Candidates, nil
	case strings.Contains(err.Error(), "no actionable issues found"):
		return nil, nil
	default:
		return nil, err
	}
}

func boardCandidateMaps(nextCandidates []store.IssueNextCandidate) (map[string]store.IssueNextCandidate, map[string]int) {
	scoreByID := make(map[string]store.IssueNextCandidate, len(nextCandidates))
	rankByID := make(map[string]int, len(nextCandidates))
	for idx, candidate := range nextCandidates {
		scoreByID[candidate.Issue.ID] = candidate
		rankByID[candidate.Issue.ID] = idx
	}
	return scoreByID, rankByID
}

func boardPopulateSnapshotRows(
	snapshot *boardSnapshot,
	issues []store.Issue,
	hierarchyByID map[string]boardIssueHierarchy,
	scoreByID map[string]store.IssueNextCandidate,
) {
	for _, issue := range issues {
		boardIncrementSummary(&snapshot.Summary, issue.Status)

		row := boardIssueRow{
			Issue:     issue,
			Hierarchy: hierarchyByID[issue.ID],
		}
		if candidate, ok := scoreByID[issue.ID]; ok {
			row.Score = candidate.Score
			row.Reasons = append([]string(nil), candidate.Reasons...)
		}

		boardAppendSnapshotRow(snapshot, row)
	}
}

func boardIncrementSummary(summary *boardSummary, status string) {
	summary.Total++
	switch status {
	case "Todo":
		summary.Todo++
	case "InProgress":
		summary.InProgress++
	case "Blocked":
		summary.Blocked++
	case "Done":
		summary.Done++
	case "WontDo":
		summary.WontDo++
	}
}

func boardAppendSnapshotRow(snapshot *boardSnapshot, row boardIssueRow) {
	switch row.Issue.Status {
	case "InProgress":
		snapshot.Active = append(snapshot.Active, row)
	case "Blocked":
		snapshot.Blocked = append(snapshot.Blocked, row)
	case "Todo":
		snapshot.Ready = append(snapshot.Ready, row)
	}
}

func boardPopulateLikelyNext(
	snapshot *boardSnapshot,
	nextCandidates []store.IssueNextCandidate,
	hierarchyByID map[string]boardIssueHierarchy,
) {
	for _, candidate := range nextCandidates {
		snapshot.LikelyNext = append(snapshot.LikelyNext, boardIssueRow{
			Issue:     candidate.Issue,
			Hierarchy: hierarchyByID[candidate.Issue.ID],
			Score:     candidate.Score,
			Reasons:   append([]string(nil), candidate.Reasons...),
		})
	}
}

func boardSortSnapshot(snapshot *boardSnapshot, rankByID map[string]int) {
	sortBoardRows(snapshot.Active, rankByID)
	sortBoardRows(snapshot.Ready, rankByID)
	sortBoardRows(snapshot.Blocked, rankByID)
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
