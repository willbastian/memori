package cli

import (
	"context"
	"flag"
	"fmt"
	"io"
	"time"

	"github.com/willbastian/memori/internal/store"
)

func runBacklog(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("backlog", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dbPath := fs.String("db", defaultDBPath(), "sqlite database path")
	issueType := fs.String("type", "", "filter by issue type: epic|story|task|bug")
	status := fs.String("status", "", "filter by status: todo|inprogress|blocked|done|wontdo")
	parent := fs.String("parent", "", "filter by parent issue key")
	jsonOut := fs.Bool("json", false, "machine-readable output")
	if err := fs.Parse(args); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	s, dbVersion, err := openInitializedStore(ctx, *dbPath)
	if err != nil {
		return err
	}
	defer s.Close()

	issues, err := s.ListIssues(ctx, store.ListIssuesParams{
		Type:     *issueType,
		Status:   *status,
		ParentID: *parent,
	})
	if err != nil {
		return err
	}

	if *jsonOut {
		return printJSON(out, jsonEnvelope{
			ResponseSchemaVersion: responseSchemaVersion,
			DBSchemaVersion:       dbVersion,
			Command:               "backlog",
			Data: backlogData{
				Count:  len(issues),
				Issues: issues,
			},
		})
	}

	if len(issues) == 0 {
		_, _ = fmt.Fprintln(out, "No issues matched the backlog filters.")
		return nil
	}
	ui := newTextUI(out)
	ui.heading("Backlog")
	renderBacklogTree(out, issues, shouldUseColor(out))
	return nil
}

type backlogData struct {
	Count  int           `json:"count"`
	Issues []store.Issue `json:"issues"`
}

func renderBacklogTree(out io.Writer, issues []store.Issue, colors bool) {
	inSet := make(map[string]bool, len(issues))
	children := make(map[string][]store.Issue)
	roots := make([]store.Issue, 0, len(issues))

	for _, issue := range issues {
		inSet[issue.ID] = true
	}
	for _, issue := range issues {
		if issue.ParentID != "" && inSet[issue.ParentID] {
			children[issue.ParentID] = append(children[issue.ParentID], issue)
			continue
		}
		roots = append(roots, issue)
	}

	visited := make(map[string]bool, len(issues))
	for i, root := range roots {
		printBacklogNode(out, root, "", i == len(roots)-1, true, inSet, children, visited, colors)
	}
}

func printBacklogNode(
	out io.Writer,
	issue store.Issue,
	prefix string,
	isLast bool,
	isRoot bool,
	inSet map[string]bool,
	children map[string][]store.Issue,
	visited map[string]bool,
	colors bool,
) {
	if visited[issue.ID] {
		return
	}
	visited[issue.ID] = true

	branch := "- "
	if !isRoot {
		if isLast {
			branch = "`- "
		} else {
			branch = "|- "
		}
	}

	line := formatIssueLine(issue, colors)
	if issue.ParentID != "" && !inSet[issue.ParentID] {
		line += fmt.Sprintf(" (parent: %s)", issue.ParentID)
	}
	_, _ = fmt.Fprintf(out, "%s%s%s\n", prefix, branch, line)

	nextPrefix := prefix
	if !isRoot {
		if isLast {
			nextPrefix += "   "
		} else {
			nextPrefix += "|  "
		}
	}
	kids := children[issue.ID]
	for i, child := range kids {
		printBacklogNode(out, child, nextPrefix, i == len(kids)-1, false, inSet, children, visited, colors)
	}
}

func formatIssueLine(issue store.Issue, colors bool) string {
	issueType := colorize(colors, colorForType(issue.Type), issue.Type)
	status := colorize(colors, colorForStatus(issue.Status), issue.Status)
	return fmt.Sprintf("%s [%s/%s] %s", issue.ID, issueType, status, issue.Title)
}
