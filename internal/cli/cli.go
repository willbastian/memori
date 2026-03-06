package cli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
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
			"memori gate evaluate --issue <prefix-shortSHA> --gate <gate-id> --result PASS|FAIL|BLOCKED --evidence <ref> [--evidence <ref>]... [--actor <actor>] --command-id <id> [--json]",
			"memori gate status --issue <prefix-shortSHA> [--json]",
			"memori backlog [--type epic|story|task|bug] [--status todo|inprogress|blocked|done] [--parent <key>] [--json]",
			"memori event log --entity <entityType:id|id> [--json]",
			"memori db status [--json]",
			"memori db migrate [--to <version>] [--json]",
			"memori db verify [--json]",
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
		return errors.New("issue subcommand required: create|link|update|show")
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
		return errors.New("gate subcommand required: evaluate|status")
	}
	switch args[0] {
	case "evaluate":
		return runGateEvaluate(args[1:], out)
	case "status":
		return runGateStatus(args[1:], out)
	default:
		return fmt.Errorf("unknown gate subcommand %q", args[0])
	}
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

	status, err := s.GetGateStatus(ctx, *issue)
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
		return errors.New("db subcommand required: status|migrate|verify|replay")
	}

	switch args[0] {
	case "status":
		return runDBStatus(args[1:], out)
	case "migrate":
		return runDBMigrate(args[1:], out)
	case "verify":
		return runDBVerify(args[1:], out)
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

type gateEvaluateData struct {
	Evaluation store.GateEvaluation `json:"evaluation"`
	Event      store.Event          `json:"event"`
	Idempotent bool                 `json:"idempotent"`
}

type gateStatusData struct {
	Status store.GateStatus `json:"status"`
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
	_, _ = fmt.Fprintln(out, "  memori gate evaluate --issue <prefix-shortSHA> --gate <gate-id> --result PASS|FAIL|BLOCKED --evidence <ref> [--evidence <ref>]... [--actor <actor>] --command-id <id> [--json]")
	_, _ = fmt.Fprintln(out, "  memori gate status --issue <prefix-shortSHA> [--json]")
	_, _ = fmt.Fprintln(out, "  memori backlog [--type epic|story|task|bug] [--status todo|inprogress|blocked|done] [--parent <key>] [--json]")
	_, _ = fmt.Fprintln(out, "  memori event log --entity <entityType:id|id> [--json]")
	_, _ = fmt.Fprintln(out, "  memori db status [--json]")
	_, _ = fmt.Fprintln(out, "  memori db migrate [--to <version>] [--json]")
	_, _ = fmt.Fprintln(out, "  memori db verify [--json]")
	_, _ = fmt.Fprintln(out, "  memori db replay [--json]")
}
