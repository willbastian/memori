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
	closeRecord, err := issueCloseRecordForDisplay(ctx, s, issue)
	if err != nil {
		return err
	}
	workspace, err := activeWorkspaceForIssue(ctx, s, issue.ID)
	if err != nil {
		return err
	}

	if *jsonOut {
		return printJSON(out, jsonEnvelope{
			ResponseSchemaVersion: responseSchemaVersion,
			DBSchemaVersion:       dbVersion,
			Command:               "issue show",
			Data: issueShowData{
				Issue:     issue,
				Close:     closeRecord,
				Workspace: workspace,
			},
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
	if workspace != nil {
		ui.blank()
		ui.section("Workspace")
		ui.field("Worktree", workspace.WorktreeID)
		ui.field("Path", workspace.Path)
		if strings.TrimSpace(workspace.RepoRoot) != "" {
			ui.field("Repo Root", workspace.RepoRoot)
		}
		if strings.TrimSpace(workspace.Branch) != "" {
			ui.field("Branch", workspace.Branch)
		}
		if strings.TrimSpace(workspace.HeadOID) != "" {
			ui.field("Head", workspace.HeadOID)
		}
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
	renderIssueCloseRecord(ui, closeRecord)
	if !strings.EqualFold(issue.Type, "epic") {
		snapshot, err := s.ContinuitySnapshot(ctx, store.ContinuitySnapshotParams{IssueID: issue.ID})
		if err != nil {
			return err
		}
		issueSessionID := openSessionIDForContinuitySnapshot(snapshot)
		if lines := continuityStatusLines(snapshot); len(lines) > 0 {
			ui.blank()
			ui.section("Continuity State")
			for _, line := range lines {
				ui.bullet(line)
			}
		}
		if lines := continuityPressureLines(issue, snapshot, ""); len(lines) > 0 {
			ui.blank()
			ui.section("Continuity Pressure")
			for _, line := range lines {
				ui.bullet(line)
			}
		}
		if steps := issueResumeSteps(issue, issueSessionID); len(steps) > 0 {
			ui.blank()
			ui.section("Resume")
			for _, step := range steps {
				ui.bullet(step)
			}
		}
		if message, steps := issueContinuityGuidance(issue, "show", issueSessionID); message != "" {
			ui.blank()
			ui.section("Continuity")
			ui.bullet(message)
			for _, step := range steps {
				ui.bullet(step)
			}
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
	snapshot, err := s.ContinuitySnapshot(ctx, store.ContinuitySnapshotParams{
		IssueID: next.Candidate.Issue.ID,
		AgentID: *agent,
	})
	if err != nil {
		return err
	}
	if lines := continuityStatusLines(snapshot); len(lines) > 0 {
		ui.blank()
		ui.section("Continuity State")
		for _, line := range lines {
			ui.bullet(line)
		}
	}
	if lines := continuityPressureLines(next.Candidate.Issue, snapshot, *agent); len(lines) > 0 {
		ui.blank()
		ui.section("Continuity Pressure")
		for _, line := range lines {
			ui.bullet(line)
		}
	}
	steps := []string{
		fmt.Sprintf("memori issue show --key %s", next.Candidate.Issue.ID),
		fmt.Sprintf("memori issue update --key %s --status inprogress", next.Candidate.Issue.ID),
	}
	issueSessionID := openSessionIDForContinuitySnapshot(snapshot)
	if strings.TrimSpace(*agent) != "" {
		steps[1] = fmt.Sprintf("memori issue update --key %s --status inprogress --agent %s", next.Candidate.Issue.ID, *agent)
		if continuitySignalsPresent(next.Candidate.Reasons) && issueSessionID != "" {
			steps = append([]string{fmt.Sprintf("memori context resume --session %s --agent %s", issueSessionID, *agent)}, steps...)
		}
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
	Issue     store.Issue             `json:"issue"`
	Close     *store.IssueCloseRecord `json:"close,omitempty"`
	Workspace *workspaceContext       `json:"workspace,omitempty"`
}

type issueNextData struct {
	Next store.IssueNextResult `json:"next"`
}
