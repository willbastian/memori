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

func runIssue(args []string, out io.Writer) error {
	if len(args) == 0 {
		return errors.New("issue subcommand required: create|link|update|show|next")
	}

	switch args[0] {
	case "create":
		return runIssueCreate(args[1:], out)
	case "link":
		return runIssueLink(args[1:], out)
	case "update":
		return runIssueUpdate(args[1:], out)
	case "show":
		return runIssueShow(args[1:], out)
	case "next":
		return runIssueNext(args[1:], out)
	default:
		return fmt.Errorf("unknown issue subcommand %q", args[0])
	}
}

func runIssueCreate(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("issue create", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dbPath := fs.String("db", defaultDBPath(), "sqlite database path")
	issueType := fs.String("type", "", "issue type: epic|story|task|bug")
	title := fs.String("title", "", "issue title")
	description := fs.String("description", "", "issue description/context")
	acceptance := fs.String("acceptance-criteria", "", "acceptance criteria")
	parent := fs.String("parent", "", "parent issue key")
	var references stringSliceFlag
	fs.Var(&references, "reference", "reference link/evidence (repeatable)")
	key := fs.String("key", "", "explicit issue key: {prefix}-{shortSHA} (optional)")
	id := fs.String("id", "", "deprecated alias for --key")
	actor := fs.String("actor", "", "actor id")
	commandID := fs.String("command-id", "", "idempotency command id")
	jsonOut := fs.Bool("json", false, "machine-readable output")
	if err := fs.Parse(args); err != nil {
		return err
	}
	issueKey, err := coalesceIssueKey(*key, *id)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	s, dbVersion, err := openInitializedStore(ctx, *dbPath)
	if err != nil {
		return err
	}
	defer s.Close()

	identity, err := resolveMutationIdentity(ctx, s, *dbPath, "issue-create", *actor, *commandID, defaultMutationAuthDeps())
	if err != nil {
		return err
	}

	issue, event, idempotent, err := s.CreateIssue(ctx, store.CreateIssueParams{
		IssueID:            issueKey,
		Type:               *issueType,
		Title:              *title,
		ParentID:           *parent,
		Description:        *description,
		AcceptanceCriteria: *acceptance,
		References:         references,
		Actor:              identity.Actor,
		CommandID:          identity.CommandID,
	})
	if err != nil {
		return err
	}

	if *jsonOut {
		return printJSON(out, jsonEnvelope{
			ResponseSchemaVersion: responseSchemaVersion,
			DBSchemaVersion:       dbVersion,
			Command:               "issue create",
			Data: issueCreateData{
				Issue:      issue,
				Event:      event,
				Idempotent: idempotent,
			},
		})
	}

	ui := newTextUI(out)
	if idempotent {
		ui.note(fmt.Sprintf("Command replay detected; reusing issue %s.", issue.ID))
	} else {
		ui.success(fmt.Sprintf("Created issue %s", issue.ID))
	}
	ui.field("Type", issue.Type)
	ui.field("Status", colorize(ui.colors, colorForStatus(issue.Status), issue.Status))
	ui.field("Title", issue.Title)
	if strings.TrimSpace(issue.ParentID) != "" {
		ui.field("Parent", issue.ParentID)
	}
	ui.field("Event", fmt.Sprintf("%s (%s #%d)", event.EventID, event.EventType, event.EventOrder))
	if message, steps := issueContinuityGuidance(issue, "create"); message != "" {
		ui.blank()
		ui.section("Continuity")
		ui.bullet(message)
		for _, step := range steps {
			ui.bullet(step)
		}
	}
	ui.nextSteps(
		fmt.Sprintf("memori issue show --key %s", issue.ID),
		fmt.Sprintf("memori issue update --key %s --status inprogress", issue.ID),
	)
	return nil
}

func runIssueUpdate(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("issue update", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dbPath := fs.String("db", defaultDBPath(), "sqlite database path")
	key := fs.String("key", "", "issue key")
	id := fs.String("id", "", "deprecated alias for --key")
	title := fs.String("title", "", "issue title")
	status := fs.String("status", "", "issue status: todo|inprogress|blocked|done|wontdo")
	priority := fs.String("priority", "", "issue priority (e.g., P0|P1|P2)")
	var labels stringSliceFlag
	fs.Var(&labels, "label", "issue label (repeatable)")
	description := fs.String("description", "", "issue description/context")
	acceptance := fs.String("acceptance-criteria", "", "acceptance criteria")
	var references stringSliceFlag
	fs.Var(&references, "reference", "reference link/evidence (repeatable)")
	actor := fs.String("actor", "", "actor id")
	commandID := fs.String("command-id", "", "idempotency command id")
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
	titleProvided := hasFlag(args, "title")
	statusProvided := hasFlag(args, "status")
	priorityProvided := hasFlag(args, "priority")
	labelsProvided := hasFlag(args, "label")
	descriptionProvided := hasFlag(args, "description")
	acceptanceProvided := hasFlag(args, "acceptance-criteria")
	referencesProvided := hasFlag(args, "reference")
	if !titleProvided && !statusProvided && !priorityProvided && !labelsProvided && !descriptionProvided && !acceptanceProvided && !referencesProvided {
		return errors.New("one of --title, --status, --priority, --label, --description, --acceptance-criteria, or --reference is required")
	}

	var titlePtr *string
	if titleProvided {
		titlePtr = title
	}
	var statusPtr *string
	if statusProvided {
		statusPtr = status
	}
	var priorityPtr *string
	if priorityProvided {
		priorityPtr = priority
	}
	var labelsPtr *[]string
	if labelsProvided {
		lbls := []string(labels)
		labelsPtr = &lbls
	}
	var descriptionPtr *string
	if descriptionProvided {
		descriptionPtr = description
	}
	var acceptancePtr *string
	if acceptanceProvided {
		acceptancePtr = acceptance
	}
	var referencesPtr *[]string
	if referencesProvided {
		refs := []string(references)
		referencesPtr = &refs
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	s, dbVersion, err := openInitializedStore(ctx, *dbPath)
	if err != nil {
		return err
	}
	defer s.Close()

	identity, err := resolveMutationIdentity(ctx, s, *dbPath, "issue-update", *actor, *commandID, defaultMutationAuthDeps())
	if err != nil {
		return err
	}

	issue, event, idempotent, err := s.UpdateIssue(ctx, store.UpdateIssueParams{
		IssueID:            issueKey,
		Title:              titlePtr,
		Status:             statusPtr,
		Priority:           priorityPtr,
		Labels:             labelsPtr,
		Description:        descriptionPtr,
		AcceptanceCriteria: acceptancePtr,
		References:         referencesPtr,
		Actor:              identity.Actor,
		CommandID:          identity.CommandID,
	})
	if err != nil {
		return err
	}

	if *jsonOut {
		return printJSON(out, jsonEnvelope{
			ResponseSchemaVersion: responseSchemaVersion,
			DBSchemaVersion:       dbVersion,
			Command:               "issue update",
			Data: issueUpdateData{
				Issue:      issue,
				Event:      event,
				Idempotent: idempotent,
			},
		})
	}

	ui := newTextUI(out)
	if idempotent {
		ui.note(fmt.Sprintf("Command replay detected; issue %s is already up to date.", issue.ID))
	} else {
		if statusProvided {
			ui.success(fmt.Sprintf("Updated issue %s -> %s", issue.ID, issue.Status))
		} else {
			ui.success(fmt.Sprintf("Updated issue %s", issue.ID))
		}
	}
	ui.field("Event", fmt.Sprintf("%s (%s #%d)", event.EventID, event.EventType, event.EventOrder))
	if message, steps := issueContinuityGuidance(issue, "update"); message != "" {
		ui.blank()
		ui.section("Continuity")
		ui.bullet(message)
		for _, step := range steps {
			ui.bullet(step)
		}
	}
	ui.nextSteps(
		fmt.Sprintf("memori issue show --key %s", issue.ID),
		fmt.Sprintf("memori event log --entity %s", issue.ID),
	)
	return nil
}

func runIssueLink(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("issue link", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dbPath := fs.String("db", defaultDBPath(), "sqlite database path")
	child := fs.String("child", "", "child issue key")
	parent := fs.String("parent", "", "parent issue key")
	actor := fs.String("actor", "", "actor id")
	commandID := fs.String("command-id", "", "idempotency command id")
	jsonOut := fs.Bool("json", false, "machine-readable output")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(*child) == "" {
		return errors.New("--child is required")
	}
	if strings.TrimSpace(*parent) == "" {
		return errors.New("--parent is required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	s, dbVersion, err := openInitializedStore(ctx, *dbPath)
	if err != nil {
		return err
	}
	defer s.Close()

	identity, err := resolveMutationIdentity(ctx, s, *dbPath, "issue-link", *actor, *commandID, defaultMutationAuthDeps())
	if err != nil {
		return err
	}

	issue, event, idempotent, err := s.LinkIssue(ctx, store.LinkIssueParams{
		ChildIssueID:  *child,
		ParentIssueID: *parent,
		Actor:         identity.Actor,
		CommandID:     identity.CommandID,
	})
	if err != nil {
		return err
	}

	if *jsonOut {
		return printJSON(out, jsonEnvelope{
			ResponseSchemaVersion: responseSchemaVersion,
			DBSchemaVersion:       dbVersion,
			Command:               "issue link",
			Data: issueLinkData{
				Issue:      issue,
				Event:      event,
				Idempotent: idempotent,
			},
		})
	}

	if idempotent {
		_, _ = fmt.Fprintf(out, "Issue link for %s already applied from previous command replay.\n", issue.ID)
	} else {
		_, _ = fmt.Fprintf(out, "Linked issue %s -> parent %s\n", issue.ID, issue.ParentID)
	}
	_, _ = fmt.Fprintf(out, "Event: %s (%s #%d)\n", event.EventID, event.EventType, event.EventOrder)
	return nil
}

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

type issueCreateData struct {
	Issue      store.Issue `json:"issue"`
	Event      store.Event `json:"event"`
	Idempotent bool        `json:"idempotent"`
}

type issueLinkData struct {
	Issue      store.Issue `json:"issue"`
	Event      store.Event `json:"event"`
	Idempotent bool        `json:"idempotent"`
}

type issueUpdateData struct {
	Issue      store.Issue `json:"issue"`
	Event      store.Event `json:"event"`
	Idempotent bool        `json:"idempotent"`
}

type issueShowData struct {
	Issue store.Issue `json:"issue"`
}

type issueNextData struct {
	Next store.IssueNextResult `json:"next"`
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
