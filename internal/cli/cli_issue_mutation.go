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
	agentID := fs.String("agent", "", "optional agent id to focus when moving work into progress")
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

	var continuityResult startIssueContinuityResult
	autoStartedContinuity := statusProvided && strings.EqualFold(strings.TrimSpace(*status), "inprogress")
	if autoStartedContinuity {
		continuityResult, err = startIssueContinuity(
			ctx,
			s,
			issue.ID,
			*agentID,
			"",
			"issue-update-inprogress",
			identity.Actor,
			identity.CommandID,
		)
		if err != nil {
			return fmt.Errorf("issue %s is now %s, but automatic continuity start failed: %w", issue.ID, issue.Status, err)
		}
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
	if autoStartedContinuity {
		ui.blank()
		ui.section("Continuity Started")
		if msg := sessionResolutionMessage("checkpoint", continuityResult.Resolution); msg != "" {
			ui.bullet(msg)
		}
		ui.bullet(fmt.Sprintf("Captured session %s for active work.", continuityResult.Data.Session.SessionID))
		ui.bullet(fmt.Sprintf("Refreshed issue packet %s for %s.", continuityResult.Data.Packet.PacketID, issue.ID))
		if continuityResult.Data.FocusUsed {
			if continuityResult.Data.FocusIdempotent {
				ui.bullet(fmt.Sprintf("Agent %s focus already pointed at %s via packet %s.", continuityResult.Data.Focus.AgentID, continuityResult.Data.Focus.ActiveIssueID, continuityResult.Data.Packet.PacketID))
			} else {
				ui.bullet(fmt.Sprintf("Updated agent %s focus to %s via packet %s.", continuityResult.Data.Focus.AgentID, continuityResult.Data.Focus.ActiveIssueID, continuityResult.Data.Packet.PacketID))
			}
		}
	}
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
