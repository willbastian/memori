package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/willbastian/memori/internal/store"
)

func runIssueShow(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("issue show", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dbPath := fs.String("db", defaultDBPath(), "sqlite database path")
	key := fs.String("key", "", "issue key")
	id := fs.String("id", "", "deprecated alias for --key")
	jsonOut := fs.Bool("json", false, "machine-readable output")
	if err := fs.Parse(args); err != nil {
		return err
	}
	issueKey, err := coalesceIssueKey(*key, *id)
	if err != nil {
		return err
	}
	if strings.TrimSpace(issueKey) == "" {
		return errors.New("--key is required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	s, dbVersion, err := openInitializedStore(ctx, *dbPath)
	if err != nil {
		return err
	}
	defer s.Close()

	issue, err := s.GetIssue(ctx, issueKey)
	if err != nil {
		return err
	}

	if *jsonOut {
		return printJSON(out, jsonEnvelope{
			ResponseSchemaVersion: responseSchemaVersion,
			DBSchemaVersion:       dbVersion,
			Command:               "issue show",
			Data:                  issueShowData{Issue: issue},
		})
	}

	ui := newTextUI(out)
	ui.heading(fmt.Sprintf("%s [%s/%s]", issue.ID, colorize(ui.colors, colorForType(issue.Type), issue.Type), colorize(ui.colors, colorForStatus(issue.Status), issue.Status)))
	ui.field("Title", issue.Title)
	if issue.ParentID != "" {
		ui.field("Parent", issue.ParentID)
	}
	if strings.TrimSpace(issue.Priority) != "" {
		ui.field("Priority", issue.Priority)
	}
	if len(issue.Labels) > 0 {
		ui.field("Labels", strings.Join(issue.Labels, ", "))
	}
	if strings.TrimSpace(issue.Description) != "" {
		ui.blank()
		ui.section("Description")
		_, _ = fmt.Fprintln(out, issue.Description)
	}
	if strings.TrimSpace(issue.Acceptance) != "" {
		ui.blank()
		ui.section("Acceptance Criteria")
		_, _ = fmt.Fprintln(out, issue.Acceptance)
	}
	if len(issue.References) > 0 {
		ui.blank()
		ui.section("References")
		for _, ref := range issue.References {
			ui.bullet(ref)
		}
	}
	ui.blank()
	ui.section("Timeline")
	ui.field("Created", issue.CreatedAt)
	ui.field("Updated", issue.UpdatedAt)
	if message, steps := issueContinuityGuidance(issue, "show"); message != "" {
		ui.blank()
		ui.section("Continuity")
		ui.bullet(message)
		for _, step := range steps {
			ui.bullet(step)
		}
	}
	return nil
}

func runIssueNext(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("issue next", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dbPath := fs.String("db", defaultDBPath(), "sqlite database path")
	agent := fs.String("agent", "", "optional agent id requesting next work")
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

	next, err := s.NextIssue(ctx, *agent)
	if err != nil {
		return err
	}

	if *jsonOut {
		return printJSON(out, jsonEnvelope{
			ResponseSchemaVersion: responseSchemaVersion,
			DBSchemaVersion:       dbVersion,
			Command:               "issue next",
			Data: issueNextData{
				Next: next,
			},
		})
	}

	ui := newTextUI(out)
	ui.heading("Recommended issue")
	ui.field("Issue", formatIssueLine(next.Candidate.Issue, ui.colors))
	ui.field("Title", next.Candidate.Issue.Title)
	ui.blank()
	ui.section("Why This Issue")
	for _, reason := range next.Candidate.Reasons {
		ui.bullet(reason)
	}
	steps := []string{
		fmt.Sprintf("memori issue show --key %s", next.Candidate.Issue.ID),
		fmt.Sprintf("memori issue update --key %s --status inprogress", next.Candidate.Issue.ID),
	}
	if strings.TrimSpace(*agent) != "" && !continuitySignalsPresent(next.Candidate.Reasons) {
		ui.blank()
		ui.section("Continuity")
		ui.bullet(continuityBootstrapMessage(*agent))
		steps = append(continuityBootstrapSteps(next.Candidate.Issue.ID), steps...)
	}
	ui.nextSteps(steps...)
	return nil
}

type issueShowData struct {
	Issue store.Issue `json:"issue"`
}

type issueNextData struct {
	Next store.IssueNextResult `json:"next"`
}
