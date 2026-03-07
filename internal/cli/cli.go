package cli

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"memori/internal/dbschema"
	"memori/internal/store"
)

const responseSchemaVersion = 1

func Run(args []string, stdout, stderr io.Writer) error {
	_ = stderr
	if len(args) == 0 {
		printHelp(stdout)
		return nil
	}

	switch args[0] {
	case "help", "--help", "-h":
		return runHelp(args[1:], stdout)
	case "init":
		return runInit(args[1:], stdout)
	case "issue":
		return runIssue(args[1:], stdout)
	case "gate":
		return runGate(args[1:], stdout)
	case "context":
		return runContext(args[1:], stdout)
	case "backlog":
		return runBacklog(args[1:], stdout)
	case "event":
		return runEvent(args[1:], stdout)
	case "db":
		return runDB(args[1:], stdout)
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func runHelp(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("help", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	jsonOut := fs.Bool("json", false, "machine-readable output")
	if err := fs.Parse(args); err != nil {
		return err
	}

	if *jsonOut {
		commands := []string{
			"memori help [--json]",
			"memori init [--db <path>] [--issue-prefix <prefix>] [--json]",
			"memori issue create --type epic|story|task|bug --title <title> [--description <text>] [--acceptance-criteria <text>] [--reference <ref>]... [--parent <key>] [--key <prefix-shortSHA>] [--actor <actor>] --command-id <id> [--json]",
			"memori issue link --child <prefix-shortSHA> --parent <prefix-shortSHA> [--actor <actor>] --command-id <id> [--json]",
			"memori issue update --key <prefix-shortSHA> [--status todo|inprogress|blocked|done] [--description <text>] [--acceptance-criteria <text>] [--reference <ref>]... [--actor <actor>] --command-id <id> [--json]",
			"memori issue show --key <prefix-shortSHA> [--json]",
			"memori issue next [--agent <id>] [--json]",
			"memori gate template create --id <template-id> --version <n> --applies-to epic|story|task|bug [--applies-to ...] --file <path> [--actor <actor>] [--json]",
			"memori gate template list [--type epic|story|task|bug] [--json]",
			"memori gate set instantiate --issue <prefix-shortSHA> --template <template-id@version> [--template ...] [--actor <actor>] [--json]",
			"memori gate set lock --issue <prefix-shortSHA> [--cycle <n>] [--actor <actor>] [--json]",
			"memori gate evaluate --issue <prefix-shortSHA> --gate <gate-id> --result PASS|FAIL|BLOCKED --evidence <ref> [--evidence <ref>]... [--actor <actor>] --command-id <id> [--json]",
			"memori gate status --issue <prefix-shortSHA> [--cycle <n>] [--json]",
			"memori context checkpoint --session <id> [--trigger <trigger>] [--actor <actor>] [--json]",
			"memori context rehydrate --session <id> [--json]",
			"memori context packet build --scope issue|session --id <id> [--actor <actor>] [--json]",
			"memori context packet show --packet <id> [--json]",
			"memori context packet use --agent <id> --packet <id> [--json]",
			"memori context loops [--issue <prefix-shortSHA>] [--cycle <n>] [--json]",
			"memori backlog [--type epic|story|task|bug] [--status todo|inprogress|blocked|done] [--parent <key>] [--json]",
			"memori event log --entity <entityType:id|id> [--json]",
			"memori db status [--json]",
			"memori db migrate [--to <version>] [--json]",
			"memori db verify [--json]",
			"memori db backup --out <path> [--json]",
			"memori db replay [--json]",
		}
		sort.Strings(commands)
		return printJSON(out, jsonEnvelope{
			ResponseSchemaVersion: responseSchemaVersion,
			DBSchemaVersion:       0,
			Command:               "help",
			Data:                  helpData{Commands: commands},
		})
	}

	printHelp(out)
	return nil
}

func runInit(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("init", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dbPath := fs.String("db", defaultDBPath(), "sqlite database path")
	issuePrefix := fs.String("issue-prefix", store.DefaultIssueKeyPrefix, "project-wide issue key prefix for new issues")
	jsonOut := fs.Bool("json", false, "machine-readable output")
	if err := fs.Parse(args); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	s, err := store.Open(*dbPath)
	if err != nil {
		return err
	}
	defer s.Close()

	if err := s.Initialize(ctx, store.InitializeParams{IssueKeyPrefix: *issuePrefix}); err != nil {
		return err
	}

	dbVersion, err := s.SchemaVersion(ctx)
	if err != nil {
		return err
	}

	if *jsonOut {
		return printJSON(out, jsonEnvelope{
			ResponseSchemaVersion: responseSchemaVersion,
			DBSchemaVersion:       dbVersion,
			Command:               "init",
			Data: initData{
				DBPath:         *dbPath,
				Status:         "initialized",
				IssueKeyPrefix: *issuePrefix,
			},
		})
	}

	_, _ = fmt.Fprintf(out, "Initialized memori database at %s (schema v%d, issue prefix %s)\n", *dbPath, dbVersion, *issuePrefix)
	return nil
}

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
	if strings.TrimSpace(*commandID) == "" {
		return errors.New("--command-id is required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	s, dbVersion, err := openInitializedStore(ctx, *dbPath)
	if err != nil {
		return err
	}
	defer s.Close()

	issue, event, idempotent, err := s.CreateIssue(ctx, store.CreateIssueParams{
		IssueID:            issueKey,
		Type:               *issueType,
		Title:              *title,
		ParentID:           *parent,
		Description:        *description,
		AcceptanceCriteria: *acceptance,
		References:         references,
		Actor:              *actor,
		CommandID:          *commandID,
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

	if idempotent {
		_, _ = fmt.Fprintf(out, "Issue %s already exists from previous command replay.\n", issue.ID)
	} else {
		_, _ = fmt.Fprintf(out, "Created issue %s (%s): %s\n", issue.ID, issue.Type, issue.Title)
	}
	_, _ = fmt.Fprintf(out, "Status: %s\n", issue.Status)
	_, _ = fmt.Fprintf(out, "Event: %s (%s #%d)\n", event.EventID, event.EventType, event.EventOrder)
	return nil
}

func runIssueUpdate(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("issue update", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dbPath := fs.String("db", defaultDBPath(), "sqlite database path")
	key := fs.String("key", "", "issue key")
	id := fs.String("id", "", "deprecated alias for --key")
	status := fs.String("status", "", "issue status: todo|inprogress|blocked|done")
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
	if strings.TrimSpace(*commandID) == "" {
		return errors.New("--command-id is required")
	}
	statusProvided := hasFlag(args, "status")
	descriptionProvided := hasFlag(args, "description")
	acceptanceProvided := hasFlag(args, "acceptance-criteria")
	referencesProvided := hasFlag(args, "reference")
	if !statusProvided && !descriptionProvided && !acceptanceProvided && !referencesProvided {
		return errors.New("one of --status, --description, --acceptance-criteria, or --reference is required")
	}

	var statusPtr *string
	if statusProvided {
		statusPtr = status
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

	issue, event, idempotent, err := s.UpdateIssue(ctx, store.UpdateIssueParams{
		IssueID:            issueKey,
		Status:             statusPtr,
		Description:        descriptionPtr,
		AcceptanceCriteria: acceptancePtr,
		References:         referencesPtr,
		Actor:              *actor,
		CommandID:          *commandID,
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

	if idempotent {
		_, _ = fmt.Fprintf(out, "Issue %s update already applied from previous command replay.\n", issue.ID)
	} else {
		_, _ = fmt.Fprintf(out, "Updated issue %s status -> %s\n", issue.ID, issue.Status)
	}
	_, _ = fmt.Fprintf(out, "Event: %s (%s #%d)\n", event.EventID, event.EventType, event.EventOrder)
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
	if strings.TrimSpace(*commandID) == "" {
		return errors.New("--command-id is required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	s, dbVersion, err := openInitializedStore(ctx, *dbPath)
	if err != nil {
		return err
	}
	defer s.Close()

	issue, event, idempotent, err := s.LinkIssue(ctx, store.LinkIssueParams{
		ChildIssueID:  *child,
		ParentIssueID: *parent,
		Actor:         *actor,
		CommandID:     *commandID,
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

	_, _ = fmt.Fprintf(out, "ID: %s\n", issue.ID)
	_, _ = fmt.Fprintf(out, "Type: %s\n", issue.Type)
	_, _ = fmt.Fprintf(out, "Title: %s\n", issue.Title)
	if issue.ParentID != "" {
		_, _ = fmt.Fprintf(out, "Parent: %s\n", issue.ParentID)
	}
	_, _ = fmt.Fprintf(out, "Status: %s\n", issue.Status)
	if strings.TrimSpace(issue.Description) != "" {
		_, _ = fmt.Fprintf(out, "Description: %s\n", issue.Description)
	}
	if strings.TrimSpace(issue.Acceptance) != "" {
		_, _ = fmt.Fprintf(out, "Acceptance Criteria: %s\n", issue.Acceptance)
	}
	if len(issue.References) > 0 {
		_, _ = fmt.Fprintf(out, "References: %s\n", strings.Join(issue.References, ", "))
	}
	_, _ = fmt.Fprintf(out, "Created: %s\n", issue.CreatedAt)
	_, _ = fmt.Fprintf(out, "Updated: %s\n", issue.UpdatedAt)
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

	_, _ = fmt.Fprintf(out, "Next issue: %s [%s/%s] %s\n", next.Candidate.Issue.ID, next.Candidate.Issue.Type, next.Candidate.Issue.Status, next.Candidate.Issue.Title)
	for _, reason := range next.Candidate.Reasons {
		_, _ = fmt.Fprintf(out, "- %s\n", reason)
	}
	return nil
}

func runBacklog(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("backlog", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dbPath := fs.String("db", defaultDBPath(), "sqlite database path")
	issueType := fs.String("type", "", "filter by issue type: epic|story|task|bug")
	status := fs.String("status", "", "filter by status: todo|inprogress|blocked|done")
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
	renderBacklogTree(out, issues, shouldUseColor(out))
	return nil
}

func runGate(args []string, out io.Writer) error {
	if len(args) == 0 {
		return errors.New("gate subcommand required: template|set|evaluate|status")
	}
	switch args[0] {
	case "template":
		return runGateTemplate(args[1:], out)
	case "set":
		return runGateSet(args[1:], out)
	case "evaluate":
		return runGateEvaluate(args[1:], out)
	case "status":
		return runGateStatus(args[1:], out)
	default:
		return fmt.Errorf("unknown gate subcommand %q", args[0])
	}
}

func runContext(args []string, out io.Writer) error {
	if len(args) == 0 {
		return errors.New("context subcommand required: checkpoint|rehydrate|packet|loops")
	}
	switch args[0] {
	case "checkpoint":
		return runContextCheckpoint(args[1:], out)
	case "rehydrate":
		return runContextRehydrate(args[1:], out)
	case "packet":
		return runContextPacket(args[1:], out)
	case "loops":
		return runContextLoops(args[1:], out)
	default:
		return fmt.Errorf("unknown context subcommand %q", args[0])
	}
}

func runContextCheckpoint(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("context checkpoint", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dbPath := fs.String("db", defaultDBPath(), "sqlite database path")
	sessionID := fs.String("session", "", "session id")
	trigger := fs.String("trigger", "manual", "checkpoint trigger reason")
	actor := fs.String("actor", "", "actor id")
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

	session, created, err := s.CheckpointSession(ctx, store.CheckpointSessionParams{
		SessionID: *sessionID,
		Trigger:   *trigger,
		Actor:     *actor,
	})
	if err != nil {
		return err
	}

	if *jsonOut {
		return printJSON(out, jsonEnvelope{
			ResponseSchemaVersion: responseSchemaVersion,
			DBSchemaVersion:       dbVersion,
			Command:               "context checkpoint",
			Data: contextCheckpointData{
				Session: session,
				Created: created,
			},
		})
	}

	if created {
		_, _ = fmt.Fprintf(out, "Created session checkpoint %s\n", session.SessionID)
	} else {
		_, _ = fmt.Fprintf(out, "Updated session checkpoint %s\n", session.SessionID)
	}
	_, _ = fmt.Fprintf(out, "Trigger: %s\n", session.Trigger)
	return nil
}

func runContextRehydrate(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("context rehydrate", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dbPath := fs.String("db", defaultDBPath(), "sqlite database path")
	sessionID := fs.String("session", "", "session id")
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

	result, err := s.RehydrateSession(ctx, store.RehydrateSessionParams{
		SessionID: *sessionID,
	})
	if err != nil {
		return err
	}

	if *jsonOut {
		return printJSON(out, jsonEnvelope{
			ResponseSchemaVersion: responseSchemaVersion,
			DBSchemaVersion:       dbVersion,
			Command:               "context rehydrate",
			Data: contextRehydrateData{
				SessionID: result.SessionID,
				Source:    result.Source,
				Packet:    result.Packet,
			},
		})
	}

	_, _ = fmt.Fprintf(out, "Rehydrated session %s via %s\n", result.SessionID, result.Source)
	_, _ = fmt.Fprintf(out, "Packet Scope: %s\n", result.Packet.Scope)
	if strings.TrimSpace(result.Packet.PacketID) != "" {
		_, _ = fmt.Fprintf(out, "Packet ID: %s\n", result.Packet.PacketID)
	}
	return nil
}

func runContextPacket(args []string, out io.Writer) error {
	if len(args) == 0 {
		return errors.New("context packet subcommand required: build|show|use")
	}
	switch args[0] {
	case "build":
		return runContextPacketBuild(args[1:], out)
	case "show":
		return runContextPacketShow(args[1:], out)
	case "use":
		return runContextPacketUse(args[1:], out)
	default:
		return fmt.Errorf("unknown context packet subcommand %q", args[0])
	}
}

func runContextPacketBuild(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("context packet build", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dbPath := fs.String("db", defaultDBPath(), "sqlite database path")
	scope := fs.String("scope", "", "packet scope: issue|session")
	scopeID := fs.String("id", "", "scope id")
	actor := fs.String("actor", "", "actor id")
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

	packet, err := s.BuildRehydratePacket(ctx, store.BuildPacketParams{
		Scope:   *scope,
		ScopeID: *scopeID,
		Actor:   *actor,
	})
	if err != nil {
		return err
	}

	if *jsonOut {
		return printJSON(out, jsonEnvelope{
			ResponseSchemaVersion: responseSchemaVersion,
			DBSchemaVersion:       dbVersion,
			Command:               "context packet build",
			Data: contextPacketData{
				Packet: packet,
			},
		})
	}

	_, _ = fmt.Fprintf(out, "Built packet %s (%s)\n", packet.PacketID, packet.Scope)
	return nil
}

func runContextPacketShow(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("context packet show", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dbPath := fs.String("db", defaultDBPath(), "sqlite database path")
	packetID := fs.String("packet", "", "packet id")
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

	packet, err := s.GetRehydratePacket(ctx, store.GetPacketParams{PacketID: *packetID})
	if err != nil {
		return err
	}

	if *jsonOut {
		return printJSON(out, jsonEnvelope{
			ResponseSchemaVersion: responseSchemaVersion,
			DBSchemaVersion:       dbVersion,
			Command:               "context packet show",
			Data: contextPacketData{
				Packet: packet,
			},
		})
	}

	_, _ = fmt.Fprintf(out, "Packet %s (%s)\n", packet.PacketID, packet.Scope)
	return nil
}

func runContextPacketUse(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("context packet use", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dbPath := fs.String("db", defaultDBPath(), "sqlite database path")
	agentID := fs.String("agent", "", "agent id")
	packetID := fs.String("packet", "", "packet id")
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

	focus, packet, err := s.UseRehydratePacket(ctx, store.UsePacketParams{
		AgentID:  *agentID,
		PacketID: *packetID,
	})
	if err != nil {
		return err
	}

	if *jsonOut {
		return printJSON(out, jsonEnvelope{
			ResponseSchemaVersion: responseSchemaVersion,
			DBSchemaVersion:       dbVersion,
			Command:               "context packet use",
			Data: contextPacketUseData{
				Focus:  focus,
				Packet: packet,
			},
		})
	}

	_, _ = fmt.Fprintf(out, "Updated agent focus for %s using packet %s\n", focus.AgentID, packet.PacketID)
	return nil
}

func runContextLoops(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("context loops", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dbPath := fs.String("db", defaultDBPath(), "sqlite database path")
	issue := fs.String("issue", "", "optional issue key filter")
	cycle := fs.Int("cycle", 0, "optional cycle filter (> 0)")
	jsonOut := fs.Bool("json", false, "machine-readable output")
	if err := fs.Parse(args); err != nil {
		return err
	}

	var cyclePtr *int
	if hasFlag(args, "cycle") {
		if *cycle <= 0 {
			return errors.New("--cycle must be > 0")
		}
		cyclePtr = cycle
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	s, dbVersion, err := openInitializedStore(ctx, *dbPath)
	if err != nil {
		return err
	}
	defer s.Close()

	loops, err := s.ListOpenLoops(ctx, store.ListOpenLoopsParams{
		IssueID: *issue,
		CycleNo: cyclePtr,
	})
	if err != nil {
		return err
	}

	if *jsonOut {
		return printJSON(out, jsonEnvelope{
			ResponseSchemaVersion: responseSchemaVersion,
			DBSchemaVersion:       dbVersion,
			Command:               "context loops",
			Data: contextLoopsData{
				Count: len(loops),
				Loops: loops,
			},
		})
	}

	if len(loops) == 0 {
		_, _ = fmt.Fprintln(out, "No context loops matched the filters.")
		return nil
	}
	for _, loop := range loops {
		_, _ = fmt.Fprintf(out, "- %s [%s/%s] issue=%s cycle=%d", loop.LoopID, loop.LoopType, loop.Status, loop.IssueID, loop.CycleNo)
		if strings.TrimSpace(loop.Priority) != "" {
			_, _ = fmt.Fprintf(out, " priority=%s", loop.Priority)
		}
		if strings.TrimSpace(loop.SourceEventID) != "" {
			_, _ = fmt.Fprintf(out, " source=%s", loop.SourceEventID)
		}
		_, _ = fmt.Fprintln(out)
	}
	return nil
}

func runGateTemplate(args []string, out io.Writer) error {
	if len(args) == 0 {
		return errors.New("gate template subcommand required: create|list")
	}
	switch args[0] {
	case "create":
		return runGateTemplateCreate(args[1:], out)
	case "list":
		return runGateTemplateList(args[1:], out)
	default:
		return fmt.Errorf("unknown gate template subcommand %q", args[0])
	}
}

func runGateTemplateCreate(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("gate template create", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dbPath := fs.String("db", defaultDBPath(), "sqlite database path")
	templateID := fs.String("id", "", "template id")
	version := fs.Int("version", 0, "template version (> 0)")
	var appliesTo stringSliceFlag
	fs.Var(&appliesTo, "applies-to", "issue type this template applies to: epic|story|task|bug (repeatable)")
	filePath := fs.String("file", "", "path to JSON definition file")
	actor := fs.String("actor", "", "actor id")
	jsonOut := fs.Bool("json", false, "machine-readable output")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(*templateID) == "" {
		return errors.New("--id is required")
	}
	if *version <= 0 {
		return errors.New("--version must be > 0")
	}
	if strings.TrimSpace(*filePath) == "" {
		return errors.New("--file is required")
	}

	definitionBytes, err := os.ReadFile(*filePath)
	if err != nil {
		return fmt.Errorf("read --file %s: %w", *filePath, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	s, dbVersion, err := openInitializedStore(ctx, *dbPath)
	if err != nil {
		return err
	}
	defer s.Close()

	template, idempotent, err := s.CreateGateTemplate(ctx, store.CreateGateTemplateParams{
		TemplateID:     *templateID,
		Version:        *version,
		AppliesTo:      appliesTo,
		DefinitionJSON: string(definitionBytes),
		Actor:          *actor,
	})
	if err != nil {
		return err
	}

	if *jsonOut {
		return printJSON(out, jsonEnvelope{
			ResponseSchemaVersion: responseSchemaVersion,
			DBSchemaVersion:       dbVersion,
			Command:               "gate template create",
			Data: gateTemplateCreateData{
				Template:   template,
				Idempotent: idempotent,
			},
		})
	}

	if idempotent {
		_, _ = fmt.Fprintf(out, "Gate template %s@%d already exists.\n", template.TemplateID, template.Version)
	} else {
		_, _ = fmt.Fprintf(out, "Created gate template %s@%d\n", template.TemplateID, template.Version)
	}
	_, _ = fmt.Fprintf(out, "Applies To: %s\n", strings.Join(template.AppliesTo, ", "))
	_, _ = fmt.Fprintf(out, "Definition Hash: %s\n", template.DefinitionHash)
	return nil
}

func runGateTemplateList(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("gate template list", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dbPath := fs.String("db", defaultDBPath(), "sqlite database path")
	issueType := fs.String("type", "", "filter by issue type: epic|story|task|bug")
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

	templates, err := s.ListGateTemplates(ctx, store.ListGateTemplatesParams{IssueType: *issueType})
	if err != nil {
		return err
	}

	if *jsonOut {
		return printJSON(out, jsonEnvelope{
			ResponseSchemaVersion: responseSchemaVersion,
			DBSchemaVersion:       dbVersion,
			Command:               "gate template list",
			Data: gateTemplateListData{
				Count:     len(templates),
				Templates: templates,
			},
		})
	}

	if len(templates) == 0 {
		_, _ = fmt.Fprintln(out, "No gate templates matched.")
		return nil
	}
	for _, template := range templates {
		_, _ = fmt.Fprintf(
			out,
			"- %s@%d applies_to=%s hash=%s\n",
			template.TemplateID,
			template.Version,
			strings.Join(template.AppliesTo, ","),
			template.DefinitionHash,
		)
	}
	return nil
}

func runGateSet(args []string, out io.Writer) error {
	if len(args) == 0 {
		return errors.New("gate set subcommand required: instantiate|lock")
	}
	switch args[0] {
	case "instantiate":
		return runGateSetInstantiate(args[1:], out)
	case "lock":
		return runGateSetLock(args[1:], out)
	default:
		return fmt.Errorf("unknown gate set subcommand %q", args[0])
	}
}

func runGateSetInstantiate(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("gate set instantiate", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dbPath := fs.String("db", defaultDBPath(), "sqlite database path")
	issue := fs.String("issue", "", "issue key")
	var templates stringSliceFlag
	fs.Var(&templates, "template", "template ref: <template_id>@<version> (repeatable)")
	actor := fs.String("actor", "", "actor id")
	jsonOut := fs.Bool("json", false, "machine-readable output")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(*issue) == "" {
		return errors.New("--issue is required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	s, dbVersion, err := openInitializedStore(ctx, *dbPath)
	if err != nil {
		return err
	}
	defer s.Close()

	gateSet, idempotent, err := s.InstantiateGateSet(ctx, store.InstantiateGateSetParams{
		IssueID:      *issue,
		TemplateRefs: templates,
		Actor:        *actor,
	})
	if err != nil {
		return err
	}

	if *jsonOut {
		return printJSON(out, jsonEnvelope{
			ResponseSchemaVersion: responseSchemaVersion,
			DBSchemaVersion:       dbVersion,
			Command:               "gate set instantiate",
			Data: gateSetInstantiateData{
				GateSet:    gateSet,
				Idempotent: idempotent,
			},
		})
	}

	if idempotent {
		_, _ = fmt.Fprintf(out, "Gate set already exists for issue %s cycle %d: %s\n", gateSet.IssueID, gateSet.CycleNo, gateSet.GateSetID)
	} else {
		_, _ = fmt.Fprintf(out, "Instantiated gate set %s for issue %s cycle %d\n", gateSet.GateSetID, gateSet.IssueID, gateSet.CycleNo)
	}
	_, _ = fmt.Fprintf(out, "Templates: %s\n", strings.Join(gateSet.TemplateRefs, ", "))
	_, _ = fmt.Fprintf(out, "Gate Set Hash: %s\n", gateSet.GateSetHash)
	return nil
}

func runGateSetLock(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("gate set lock", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dbPath := fs.String("db", defaultDBPath(), "sqlite database path")
	issue := fs.String("issue", "", "issue key")
	cycle := fs.Int("cycle", 0, "issue cycle to lock (defaults to current cycle)")
	actor := fs.String("actor", "", "actor id")
	jsonOut := fs.Bool("json", false, "machine-readable output")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(*issue) == "" {
		return errors.New("--issue is required")
	}

	var cyclePtr *int
	if hasFlag(args, "cycle") {
		cyclePtr = cycle
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	s, dbVersion, err := openInitializedStore(ctx, *dbPath)
	if err != nil {
		return err
	}
	defer s.Close()

	gateSet, lockedNow, err := s.LockGateSet(ctx, store.LockGateSetParams{
		IssueID: *issue,
		CycleNo: cyclePtr,
		Actor:   *actor,
	})
	if err != nil {
		return err
	}

	if *jsonOut {
		return printJSON(out, jsonEnvelope{
			ResponseSchemaVersion: responseSchemaVersion,
			DBSchemaVersion:       dbVersion,
			Command:               "gate set lock",
			Data: gateSetLockData{
				GateSet:   gateSet,
				LockedNow: lockedNow,
			},
		})
	}

	if lockedNow {
		_, _ = fmt.Fprintf(out, "Locked gate set %s for issue %s cycle %d at %s\n", gateSet.GateSetID, gateSet.IssueID, gateSet.CycleNo, gateSet.LockedAt)
	} else {
		_, _ = fmt.Fprintf(out, "Gate set %s is already locked at %s\n", gateSet.GateSetID, gateSet.LockedAt)
	}
	return nil
}

func runGateEvaluate(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("gate evaluate", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dbPath := fs.String("db", defaultDBPath(), "sqlite database path")
	issue := fs.String("issue", "", "issue key")
	gate := fs.String("gate", "", "gate id")
	result := fs.String("result", "", "evaluation result: PASS|FAIL|BLOCKED")
	var evidence stringSliceFlag
	fs.Var(&evidence, "evidence", "evidence reference (repeatable)")
	actor := fs.String("actor", "", "actor id")
	commandID := fs.String("command-id", "", "idempotency command id")
	jsonOut := fs.Bool("json", false, "machine-readable output")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(*issue) == "" {
		return errors.New("--issue is required")
	}
	if strings.TrimSpace(*gate) == "" {
		return errors.New("--gate is required")
	}
	if strings.TrimSpace(*result) == "" {
		return errors.New("--result is required")
	}
	if strings.TrimSpace(*commandID) == "" {
		return errors.New("--command-id is required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	s, dbVersion, err := openInitializedStore(ctx, *dbPath)
	if err != nil {
		return err
	}
	defer s.Close()

	evaluation, event, idempotent, err := s.EvaluateGate(ctx, store.EvaluateGateParams{
		IssueID:      *issue,
		GateID:       *gate,
		Result:       *result,
		EvidenceRefs: evidence,
		Actor:        *actor,
		CommandID:    *commandID,
	})
	if err != nil {
		return err
	}

	if *jsonOut {
		return printJSON(out, jsonEnvelope{
			ResponseSchemaVersion: responseSchemaVersion,
			DBSchemaVersion:       dbVersion,
			Command:               "gate evaluate",
			Data: gateEvaluateData{
				Evaluation: evaluation,
				Event:      event,
				Idempotent: idempotent,
			},
		})
	}

	if idempotent {
		_, _ = fmt.Fprintf(out, "Gate evaluation already recorded for issue %s gate %s.\n", evaluation.IssueID, evaluation.GateID)
	} else {
		_, _ = fmt.Fprintf(out, "Recorded gate evaluation for issue %s gate %s: %s\n", evaluation.IssueID, evaluation.GateID, evaluation.Result)
	}
	_, _ = fmt.Fprintf(out, "Gate Set: %s\n", evaluation.GateSetID)
	_, _ = fmt.Fprintf(out, "Evaluated At: %s\n", evaluation.EvaluatedAt)
	_, _ = fmt.Fprintf(out, "Event: %s (%s #%d)\n", event.EventID, event.EventType, event.EventOrder)
	return nil
}

func runGateStatus(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("gate status", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dbPath := fs.String("db", defaultDBPath(), "sqlite database path")
	issue := fs.String("issue", "", "issue key")
	cycle := fs.Int("cycle", 0, "issue cycle to inspect (defaults to current cycle)")
	jsonOut := fs.Bool("json", false, "machine-readable output")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(*issue) == "" {
		return errors.New("--issue is required")
	}
	var cyclePtr *int
	if hasFlag(args, "cycle") {
		if *cycle <= 0 {
			return errors.New("--cycle must be > 0")
		}
		cyclePtr = cycle
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	s, dbVersion, err := openInitializedStore(ctx, *dbPath)
	if err != nil {
		return err
	}
	defer s.Close()

	status, err := s.GetGateStatusForCycle(ctx, store.GetGateStatusParams{
		IssueID: *issue,
		CycleNo: cyclePtr,
	})
	if err != nil {
		return err
	}

	if *jsonOut {
		return printJSON(out, jsonEnvelope{
			ResponseSchemaVersion: responseSchemaVersion,
			DBSchemaVersion:       dbVersion,
			Command:               "gate status",
			Data: gateStatusData{
				Status: status,
			},
		})
	}

	_, _ = fmt.Fprintf(out, "Gate status for %s\n", status.IssueID)
	_, _ = fmt.Fprintf(out, "Gate Set: %s (cycle %d)\n", status.GateSetID, status.CycleNo)
	if strings.TrimSpace(status.LockedAt) != "" {
		_, _ = fmt.Fprintf(out, "Locked At: %s\n", status.LockedAt)
	}
	for _, gate := range status.Gates {
		required := "optional"
		if gate.Required {
			required = "required"
		}
		resultValue := colorize(shouldUseColor(out), colorForGateResult(gate.Result), gate.Result)
		_, _ = fmt.Fprintf(out, "- %s [%s/%s] %s", gate.GateID, required, gate.Kind, resultValue)
		if gate.EvaluatedAt != "" {
			_, _ = fmt.Fprintf(out, " at %s", gate.EvaluatedAt)
		}
		if len(gate.EvidenceRefs) > 0 {
			_, _ = fmt.Fprintf(out, " evidence=%s", strings.Join(gate.EvidenceRefs, ","))
		}
		_, _ = fmt.Fprintln(out)
	}
	return nil
}

func runEvent(args []string, out io.Writer) error {
	if len(args) == 0 {
		return errors.New("event subcommand required: log")
	}
	if args[0] != "log" {
		return fmt.Errorf("unknown event subcommand %q", args[0])
	}

	fs := flag.NewFlagSet("event log", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dbPath := fs.String("db", defaultDBPath(), "sqlite database path")
	entity := fs.String("entity", "", "entity reference: entityType:id or id (defaults to issue)")
	jsonOut := fs.Bool("json", false, "machine-readable output")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if strings.TrimSpace(*entity) == "" {
		return errors.New("--entity is required")
	}

	entityType, entityID, err := parseEntityRef(*entity)
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

	events, err := s.ListEventsForEntity(ctx, entityType, entityID)
	if err != nil {
		return err
	}

	if *jsonOut {
		return printJSON(out, jsonEnvelope{
			ResponseSchemaVersion: responseSchemaVersion,
			DBSchemaVersion:       dbVersion,
			Command:               "event log",
			Data: eventLogData{
				EntityType: entityType,
				EntityID:   entityID,
				Events:     events,
			},
		})
	}

	if len(events) == 0 {
		_, _ = fmt.Fprintf(out, "No events for %s:%s\n", entityType, entityID)
		return nil
	}
	_, _ = fmt.Fprintf(out, "Events for %s:%s\n", entityType, entityID)
	for _, event := range events {
		_, _ = fmt.Fprintf(out, "- #%d %s %s actor=%s command_id=%s\n", event.EventOrder, event.EventType, event.CreatedAt, event.Actor, event.CommandID)
	}
	return nil
}

func runDB(args []string, out io.Writer) error {
	if len(args) == 0 {
		return errors.New("db subcommand required: status|migrate|verify|backup|replay")
	}

	switch args[0] {
	case "status":
		return runDBStatus(args[1:], out)
	case "migrate":
		return runDBMigrate(args[1:], out)
	case "verify":
		return runDBVerify(args[1:], out)
	case "backup":
		return runDBBackup(args[1:], out)
	case "replay":
		return runDBReplay(args[1:], out)
	default:
		return fmt.Errorf("unknown db subcommand %q", args[0])
	}
}

func runDBStatus(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("db status", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dbPath := fs.String("db", defaultDBPath(), "sqlite database path")
	jsonOut := fs.Bool("json", false, "machine-readable output")
	if err := fs.Parse(args); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	s, err := store.Open(*dbPath)
	if err != nil {
		return err
	}
	defer s.Close()

	status, err := dbschema.StatusOf(ctx, s.DB())
	if err != nil {
		return err
	}
	dbVersion, err := s.SchemaVersion(ctx)
	if err != nil {
		dbVersion = 0
	}

	if *jsonOut {
		return printJSON(out, jsonEnvelope{
			ResponseSchemaVersion: responseSchemaVersion,
			DBSchemaVersion:       dbVersion,
			Command:               "db status",
			Data: dbStatusData{
				CurrentVersion:    status.CurrentVersion,
				HeadVersion:       status.HeadVersion,
				PendingMigrations: status.PendingMigrations,
			},
		})
	}

	_, _ = fmt.Fprintf(out, "Current schema version: %d\n", status.CurrentVersion)
	_, _ = fmt.Fprintf(out, "Head schema version: %d\n", status.HeadVersion)
	_, _ = fmt.Fprintf(out, "Pending migrations: %d\n", status.PendingMigrations)
	return nil
}

func runDBMigrate(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("db migrate", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dbPath := fs.String("db", defaultDBPath(), "sqlite database path")
	to := fs.Int("to", 0, "target migration version (optional)")
	jsonOut := fs.Bool("json", false, "machine-readable output")
	if err := fs.Parse(args); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	s, err := store.Open(*dbPath)
	if err != nil {
		return err
	}
	defer s.Close()

	before, err := dbschema.StatusOf(ctx, s.DB())
	if err != nil {
		return err
	}

	toSet := hasFlag(args, "to")
	var toPtr *int
	if toSet {
		toPtr = to
	}
	after, err := dbschema.Migrate(ctx, s.DB(), toPtr)
	if err != nil {
		return err
	}

	dbVersion, err := s.SchemaVersion(ctx)
	if err != nil {
		dbVersion = 0
	}

	if *jsonOut {
		return printJSON(out, jsonEnvelope{
			ResponseSchemaVersion: responseSchemaVersion,
			DBSchemaVersion:       dbVersion,
			Command:               "db migrate",
			Data: dbMigrateData{
				FromVersion:       before.CurrentVersion,
				CurrentVersion:    after.CurrentVersion,
				HeadVersion:       after.HeadVersion,
				PendingMigrations: after.PendingMigrations,
			},
		})
	}

	_, _ = fmt.Fprintf(out, "Migrated database from version %d to %d\n", before.CurrentVersion, after.CurrentVersion)
	_, _ = fmt.Fprintf(out, "Head schema version: %d\n", after.HeadVersion)
	_, _ = fmt.Fprintf(out, "Pending migrations: %d\n", after.PendingMigrations)
	return nil
}

func runDBVerify(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("db verify", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dbPath := fs.String("db", defaultDBPath(), "sqlite database path")
	jsonOut := fs.Bool("json", false, "machine-readable output")
	if err := fs.Parse(args); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	s, err := store.Open(*dbPath)
	if err != nil {
		return err
	}
	defer s.Close()

	result, err := dbschema.Verify(ctx, s.DB())
	if err != nil {
		return err
	}

	dbVersion, err := s.SchemaVersion(ctx)
	if err != nil {
		dbVersion = 0
	}

	if *jsonOut {
		if err := printJSON(out, jsonEnvelope{
			ResponseSchemaVersion: responseSchemaVersion,
			DBSchemaVersion:       dbVersion,
			Command:               "db verify",
			Data:                  result,
		}); err != nil {
			return err
		}
	} else if result.OK {
		_, _ = fmt.Fprintln(out, "Database verify: OK")
	} else {
		_, _ = fmt.Fprintln(out, "Database verify: FAILED")
	}

	if !result.OK {
		return errors.New(strings.Join(result.Checks, "; "))
	}
	if !*jsonOut {
		_, _ = fmt.Fprintln(out, strings.Join(result.Checks, "; "))
	}
	return nil
}

func runDBReplay(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("db replay", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dbPath := fs.String("db", defaultDBPath(), "sqlite database path")
	jsonOut := fs.Bool("json", false, "machine-readable output")
	if err := fs.Parse(args); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	s, dbVersion, err := openInitializedStore(ctx, *dbPath)
	if err != nil {
		return err
	}
	defer s.Close()

	result, err := s.ReplayProjections(ctx)
	if err != nil {
		return err
	}

	if *jsonOut {
		return printJSON(out, jsonEnvelope{
			ResponseSchemaVersion: responseSchemaVersion,
			DBSchemaVersion:       dbVersion,
			Command:               "db replay",
			Data:                  result,
		})
	}

	_, _ = fmt.Fprintf(out, "Rebuilt projections from %d events\n", result.EventsApplied)
	return nil
}

func runDBBackup(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("db backup", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dbPath := fs.String("db", defaultDBPath(), "sqlite database path")
	outPath := fs.String("out", "", "backup destination path")
	jsonOut := fs.Bool("json", false, "machine-readable output")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(*outPath) == "" {
		return errors.New("--out is required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	s, dbVersion, err := openInitializedStore(ctx, *dbPath)
	if err != nil {
		return err
	}
	defer s.Close()

	absSource, err := filepath.Abs(*dbPath)
	if err != nil {
		return fmt.Errorf("resolve --db path: %w", err)
	}
	absTarget, err := filepath.Abs(*outPath)
	if err != nil {
		return fmt.Errorf("resolve --out path: %w", err)
	}
	if absSource == absTarget {
		return errors.New("--out must be different from --db path")
	}
	if _, err := os.Stat(absTarget); err == nil {
		return fmt.Errorf("backup target already exists: %s", absTarget)
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("stat backup target %s: %w", absTarget, err)
	}
	if err := os.MkdirAll(filepath.Dir(absTarget), 0o755); err != nil {
		return fmt.Errorf("create backup directory: %w", err)
	}
	if err := sqliteVacuumInto(ctx, s.DB(), absTarget); err != nil {
		return fmt.Errorf("backup database to %s: %w", absTarget, err)
	}

	data := dbBackupData{
		SourcePath: absSource,
		TargetPath: absTarget,
		Status:     "ok",
	}
	if *jsonOut {
		return printJSON(out, jsonEnvelope{
			ResponseSchemaVersion: responseSchemaVersion,
			DBSchemaVersion:       dbVersion,
			Command:               "db backup",
			Data:                  data,
		})
	}

	_, _ = fmt.Fprintf(out, "Backed up %s -> %s\n", absSource, absTarget)
	return nil
}

func sqliteVacuumInto(ctx context.Context, db *sql.DB, outPath string) error {
	escapedOutPath := strings.ReplaceAll(outPath, "'", "''")
	_, err := db.ExecContext(ctx, "VACUUM INTO '"+escapedOutPath+"'")
	return err
}

func defaultDBPath() string {
	if fromEnv := strings.TrimSpace(os.Getenv("MEMORI_DB_PATH")); fromEnv != "" {
		return fromEnv
	}
	return ".memori/memori.db"
}

func openInitializedStore(ctx context.Context, dbPath string) (*store.Store, int, error) {
	s, err := store.Open(dbPath)
	if err != nil {
		return nil, 0, err
	}
	v, err := s.SchemaVersion(ctx)
	if err != nil {
		_ = s.Close()
		return nil, 0, err
	}
	if v == 0 {
		_ = s.Close()
		return nil, 0, fmt.Errorf("database is not initialized at %s (run: memori init --db %s)", dbPath, dbPath)
	}
	migrationStatus, err := dbschema.StatusOf(ctx, s.DB())
	if err != nil {
		_ = s.Close()
		return nil, 0, err
	}
	if migrationStatus.PendingMigrations > 0 {
		_ = s.Close()
		return nil, 0, fmt.Errorf(
			"database schema is behind by %d migration(s) (run: memori db migrate --db %s)",
			migrationStatus.PendingMigrations,
			dbPath,
		)
	}
	return s, v, nil
}

func parseEntityRef(raw string) (entityType, entityID string, err error) {
	parts := strings.SplitN(strings.TrimSpace(raw), ":", 2)
	if len(parts) == 1 {
		if parts[0] == "" {
			return "", "", errors.New("entity id cannot be empty")
		}
		return "issue", parts[0], nil
	}
	if strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[1]) == "" {
		return "", "", fmt.Errorf("invalid entity reference %q", raw)
	}
	entityType = strings.ToLower(strings.TrimSpace(parts[0]))
	entityID = strings.TrimSpace(parts[1])
	if entityType != "issue" {
		return "", "", fmt.Errorf("invalid entity type %q (expected issue)", parts[0])
	}
	return entityType, entityID, nil
}

func hasFlag(args []string, name string) bool {
	long := "--" + name
	for _, arg := range args {
		if arg == long || strings.HasPrefix(arg, long+"=") {
			return true
		}
	}
	return false
}

type jsonEnvelope struct {
	ResponseSchemaVersion int    `json:"response_schema_version"`
	DBSchemaVersion       int    `json:"db_schema_version"`
	Command               string `json:"command"`
	Data                  any    `json:"data"`
}

type helpData struct {
	Commands []string `json:"commands"`
}

type initData struct {
	DBPath         string `json:"db_path"`
	Status         string `json:"status"`
	IssueKeyPrefix string `json:"issue_key_prefix"`
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

type gateEvaluateData struct {
	Evaluation store.GateEvaluation `json:"evaluation"`
	Event      store.Event          `json:"event"`
	Idempotent bool                 `json:"idempotent"`
}

type gateTemplateCreateData struct {
	Template   store.GateTemplate `json:"template"`
	Idempotent bool               `json:"idempotent"`
}

type gateTemplateListData struct {
	Count     int                  `json:"count"`
	Templates []store.GateTemplate `json:"templates"`
}

type gateSetInstantiateData struct {
	GateSet    store.GateSet `json:"gate_set"`
	Idempotent bool          `json:"idempotent"`
}

type gateSetLockData struct {
	GateSet   store.GateSet `json:"gate_set"`
	LockedNow bool          `json:"locked_now"`
}

type gateStatusData struct {
	Status store.GateStatus `json:"status"`
}

type contextCheckpointData struct {
	Session store.Session `json:"session"`
	Created bool          `json:"created"`
}

type contextRehydrateData struct {
	SessionID string                `json:"session_id"`
	Source    string                `json:"source"`
	Packet    store.RehydratePacket `json:"packet"`
}

type contextPacketData struct {
	Packet store.RehydratePacket `json:"packet"`
}

type contextPacketUseData struct {
	Focus  store.AgentFocus      `json:"focus"`
	Packet store.RehydratePacket `json:"packet"`
}

type contextLoopsData struct {
	Count int              `json:"count"`
	Loops []store.OpenLoop `json:"loops"`
}

type eventLogData struct {
	EntityType string        `json:"entity_type"`
	EntityID   string        `json:"entity_id"`
	Events     []store.Event `json:"events"`
}

type backlogData struct {
	Count  int           `json:"count"`
	Issues []store.Issue `json:"issues"`
}

type dbStatusData struct {
	CurrentVersion    int `json:"current_version"`
	HeadVersion       int `json:"head_version"`
	PendingMigrations int `json:"pending_migrations"`
}

type dbMigrateData struct {
	FromVersion       int `json:"from_version"`
	CurrentVersion    int `json:"current_version"`
	HeadVersion       int `json:"head_version"`
	PendingMigrations int `json:"pending_migrations"`
}

type dbBackupData struct {
	SourcePath string `json:"source_path"`
	TargetPath string `json:"target_path"`
	Status     string `json:"status"`
}

type stringSliceFlag []string

func (s *stringSliceFlag) String() string {
	return strings.Join(*s, ",")
}

func (s *stringSliceFlag) Set(value string) error {
	*s = append(*s, value)
	return nil
}

func coalesceIssueKey(key, id string) (string, error) {
	key = strings.TrimSpace(key)
	id = strings.TrimSpace(id)
	if key != "" && id != "" && key != id {
		return "", errors.New("--key and --id were both provided with different values")
	}
	if key != "" {
		return key, nil
	}
	return id, nil
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

func shouldUseColor(out io.Writer) bool {
	if os.Getenv("NO_COLOR") != "" {
		return false
	}
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

func colorForType(issueType string) string {
	switch issueType {
	case "Epic":
		return "34" // blue
	case "Story":
		return "36" // cyan
	case "Task":
		return "33" // yellow
	case "Bug":
		return "31" // red
	default:
		return "37" // white
	}
}

func colorForStatus(status string) string {
	switch status {
	case "Todo":
		return "90" // gray
	case "InProgress":
		return "33" // yellow
	case "Blocked":
		return "31" // red
	case "Done":
		return "32" // green
	default:
		return "37" // white
	}
}

func colorForGateResult(result string) string {
	switch result {
	case "PASS":
		return "32" // green
	case "FAIL":
		return "31" // red
	case "BLOCKED":
		return "33" // yellow
	case "MISSING":
		return "90" // gray
	default:
		return "37" // white
	}
}

func colorize(enabled bool, colorCode, value string) string {
	if !enabled {
		return value
	}
	return "\033[" + colorCode + "m" + value + "\033[0m"
}

func printJSON(out io.Writer, v any) error {
	enc := json.NewEncoder(out)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

func printHelp(out io.Writer) {
	_, _ = fmt.Fprintln(out, "memori - local context bridge + agile issue ledger")
	_, _ = fmt.Fprintln(out, "")
	_, _ = fmt.Fprintln(out, "Commands:")
	_, _ = fmt.Fprintln(out, "  memori help [--json]")
	_, _ = fmt.Fprintln(out, "  memori init [--db <path>] [--issue-prefix <prefix>] [--json]")
	_, _ = fmt.Fprintln(out, "  memori issue create --type epic|story|task|bug --title <title> [--description <text>] [--acceptance-criteria <text>] [--reference <ref>]... [--parent <key>] [--key <prefix-shortSHA>] [--actor <actor>] --command-id <id> [--json]")
	_, _ = fmt.Fprintln(out, "  memori issue link --child <prefix-shortSHA> --parent <prefix-shortSHA> [--actor <actor>] --command-id <id> [--json]")
	_, _ = fmt.Fprintln(out, "  memori issue update --key <prefix-shortSHA> [--status todo|inprogress|blocked|done] [--description <text>] [--acceptance-criteria <text>] [--reference <ref>]... [--actor <actor>] --command-id <id> [--json]")
	_, _ = fmt.Fprintln(out, "  memori issue show --key <prefix-shortSHA> [--json]")
	_, _ = fmt.Fprintln(out, "  memori issue next [--agent <id>] [--json]")
	_, _ = fmt.Fprintln(out, "  memori gate template create --id <template-id> --version <n> --applies-to epic|story|task|bug [--applies-to ...] --file <path> [--actor <actor>] [--json]")
	_, _ = fmt.Fprintln(out, "  memori gate template list [--type epic|story|task|bug] [--json]")
	_, _ = fmt.Fprintln(out, "  memori gate set instantiate --issue <prefix-shortSHA> --template <template-id@version> [--template ...] [--actor <actor>] [--json]")
	_, _ = fmt.Fprintln(out, "  memori gate set lock --issue <prefix-shortSHA> [--cycle <n>] [--actor <actor>] [--json]")
	_, _ = fmt.Fprintln(out, "  memori gate evaluate --issue <prefix-shortSHA> --gate <gate-id> --result PASS|FAIL|BLOCKED --evidence <ref> [--evidence <ref>]... [--actor <actor>] --command-id <id> [--json]")
	_, _ = fmt.Fprintln(out, "  memori gate status --issue <prefix-shortSHA> [--cycle <n>] [--json]")
	_, _ = fmt.Fprintln(out, "  memori context checkpoint --session <id> [--trigger <trigger>] [--actor <actor>] [--json]")
	_, _ = fmt.Fprintln(out, "  memori context rehydrate --session <id> [--json]")
	_, _ = fmt.Fprintln(out, "  memori context packet build --scope issue|session --id <id> [--actor <actor>] [--json]")
	_, _ = fmt.Fprintln(out, "  memori context packet show --packet <id> [--json]")
	_, _ = fmt.Fprintln(out, "  memori context packet use --agent <id> --packet <id> [--json]")
	_, _ = fmt.Fprintln(out, "  memori context loops [--issue <prefix-shortSHA>] [--cycle <n>] [--json]")
	_, _ = fmt.Fprintln(out, "  memori backlog [--type epic|story|task|bug] [--status todo|inprogress|blocked|done] [--parent <key>] [--json]")
	_, _ = fmt.Fprintln(out, "  memori event log --entity <entityType:id|id> [--json]")
	_, _ = fmt.Fprintln(out, "  memori db status [--json]")
	_, _ = fmt.Fprintln(out, "  memori db migrate [--to <version>] [--json]")
	_, _ = fmt.Fprintln(out, "  memori db verify [--json]")
	_, _ = fmt.Fprintln(out, "  memori db backup --out <path> [--json]")
	_, _ = fmt.Fprintln(out, "  memori db replay [--json]")
}
