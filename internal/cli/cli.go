package cli

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"memori/internal/dbschema"
	"memori/internal/provenance"
	"memori/internal/store"
)

const responseSchemaVersion = 1

var passwordPrompter = readPasswordNoEcho
var authCommandTimeout = 5 * time.Second

func Run(args []string, stdout, stderr io.Writer) error {
	_ = stderr
	if len(args) == 0 {
		printHelp(stdout)
		return nil
	}

	switch args[0] {
	case "help", "--help", "-h":
		return runHelp(args[1:], stdout)
	case "auth":
		return runAuth(args[1:], stdout)
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
	case "board":
		return runBoard(args[1:], stdout)
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
			"memori auth set-password [--db <path>] [--json]",
			"memori auth status [--db <path>] [--json]",
			"memori help [--json]",
			"memori init [--db <path>] [--issue-prefix <prefix>] [--json]",
			"memori issue create --type epic|story|task|bug --title <title> [--description <text>] [--acceptance-criteria <text>] [--reference <ref>]... [--parent <key>] [--key <prefix-shortSHA>] [--actor <actor>] [--command-id <id>] [--json]",
			"memori issue link --child <prefix-shortSHA> --parent <prefix-shortSHA> [--actor <actor>] [--command-id <id>] [--json]",
			"memori issue update --key <prefix-shortSHA> [--title <title>] [--status todo|inprogress|blocked|done|wontdo] [--priority <value>] [--label <label>]... [--description <text>] [--acceptance-criteria <text>] [--reference <ref>]... [--actor <actor>] [--command-id <id>] [--json]",
			"memori issue show --key <prefix-shortSHA> [--json]",
			"memori issue next [--agent <id>] [--json]",
			"memori board [--db <path>] [--agent <id>] [--watch] [--interval <duration>] [--json]",
			"memori gate template create --id <template-id> --version <n> --applies-to epic|story|task|bug [--applies-to ...] --file <path> [--actor <actor>] [--command-id <id>] [--json]",
			"memori gate template approve --id <template-id> --version <n> [--actor <actor>] [--command-id <id>] [--json]",
			"memori gate template list [--type epic|story|task|bug] [--json]",
			"memori gate template pending [--db <path>] [--json]",
			"memori gate set instantiate --issue <prefix-shortSHA> --template <template-id@version> [--template ...] [--actor <actor>] [--command-id <id>] [--json]",
			"memori gate set lock --issue <prefix-shortSHA> [--cycle <n>] [--actor <actor>] [--command-id <id>] [--json]",
			"memori gate evaluate --issue <prefix-shortSHA> --gate <gate-id> --result PASS|FAIL|BLOCKED --evidence <ref> [--evidence <ref>]... [--actor <actor>] [--command-id <id>] [--json]",
			"memori gate verify --issue <prefix-shortSHA> --gate <gate-id> [--actor <actor>] [--command-id <id>] [--json]",
			"memori gate status --issue <prefix-shortSHA> [--cycle <n>] [--json]",
			"memori context checkpoint --session <id> [--trigger <trigger>] [--actor <actor>] [--command-id <id>] [--json]",
			"memori context summarize --session <id> [--note <text>] [--actor <actor>] [--command-id <id>] [--json]",
			"memori context close --session <id> [--reason <text>] [--actor <actor>] [--command-id <id>] [--json]",
			"memori context rehydrate --session <id> [--json]",
			"memori context packet build --scope issue|session --id <id> [--actor <actor>] [--command-id <id>] [--json]",
			"memori context packet show --packet <id> [--json]",
			"memori context packet use --agent <id> --packet <id> [--actor <actor>] [--command-id <id>] [--json]",
			"memori context loops [--issue <prefix-shortSHA>] [--cycle <n>] [--json]",
			"memori backlog [--type epic|story|task|bug] [--status todo|inprogress|blocked|done|wontdo] [--parent <key>] [--json]",
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

	ui := newTextUI(out)
	ui.success("Initialized memori database")
	ui.field("DB Path", *dbPath)
	ui.field("Schema", fmt.Sprintf("v%d", dbVersion))
	ui.field("Issue Prefix", *issuePrefix)
	ui.nextSteps(
		fmt.Sprintf("memori auth set-password --db %s", *dbPath),
		`memori issue create --type task --title "First ticket"`,
		"memori backlog",
	)
	return nil
}

func runAuth(args []string, out io.Writer) error {
	if len(args) == 0 {
		return errors.New("auth subcommand required: status|set-password")
	}

	switch args[0] {
	case "status":
		return runAuthStatus(args[1:], out)
	case "set-password":
		return runAuthSetPassword(args[1:], out)
	default:
		return fmt.Errorf("unknown auth subcommand %q", args[0])
	}
}

func runAuthStatus(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("auth status", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dbPath := fs.String("db", defaultDBPath(), "sqlite database path")
	jsonOut := fs.Bool("json", false, "machine-readable output")
	if err := fs.Parse(args); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), authCommandTimeout)
	defer cancel()

	s, dbVersion, err := openInitializedStore(ctx, *dbPath)
	if err != nil {
		return err
	}
	defer s.Close()

	credential, configured, err := s.GetHumanAuthCredential(ctx)
	if err != nil {
		return err
	}

	if *jsonOut {
		return printJSON(out, jsonEnvelope{
			ResponseSchemaVersion: responseSchemaVersion,
			DBSchemaVersion:       dbVersion,
			Command:               "auth status",
			Data: authStatusData{
				Configured: configured,
				Algorithm:  credential.Algorithm,
				Iterations: credential.Iterations,
				UpdatedAt:  credential.UpdatedAt,
				RotatedBy:  credential.RotatedBy,
			},
		})
	}

	if !configured {
		_, _ = fmt.Fprintln(out, "Human auth: not configured")
		_, _ = fmt.Fprintf(out, "Run: memori auth set-password --db %s\n", *dbPath)
		return nil
	}

	_, _ = fmt.Fprintln(out, "Human auth: configured")
	_, _ = fmt.Fprintf(out, "Algorithm: %s\n", credential.Algorithm)
	_, _ = fmt.Fprintf(out, "Iterations: %d\n", credential.Iterations)
	_, _ = fmt.Fprintf(out, "Updated: %s\n", credential.UpdatedAt)
	_, _ = fmt.Fprintf(out, "Rotated By: %s\n", credential.RotatedBy)
	return nil
}

func runAuthSetPassword(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("auth set-password", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dbPath := fs.String("db", defaultDBPath(), "sqlite database path")
	jsonOut := fs.Bool("json", false, "machine-readable output")
	if err := fs.Parse(args); err != nil {
		return err
	}

	principal, err := provenance.ResolvePrincipal()
	if err != nil {
		return err
	}
	if principal.Kind != provenance.PrincipalHuman {
		return errors.New("memori auth set-password requires a human principal")
	}

	ctx, cancel := context.WithTimeout(context.Background(), authCommandTimeout)
	s, dbVersion, err := openInitializedStore(ctx, *dbPath)
	cancel()
	if err != nil {
		return err
	}
	defer s.Close()

	ctx, cancel = context.WithTimeout(context.Background(), authCommandTimeout)
	currentCredential, configured, err := s.GetHumanAuthCredential(ctx)
	cancel()
	if err != nil {
		return err
	}
	if configured {
		currentPassword, err := passwordPrompter("Current password: ")
		if err != nil {
			return err
		}
		ok, err := provenance.VerifyPassword(currentPassword, provenance.PasswordCredential{
			Algorithm:  currentCredential.Algorithm,
			Iterations: currentCredential.Iterations,
			SaltHex:    currentCredential.SaltHex,
			HashHex:    currentCredential.HashHex,
		})
		if err != nil {
			return err
		}
		if !ok {
			return errors.New("current password verification failed")
		}
	}

	password, err := passwordPrompter("New password: ")
	if err != nil {
		return err
	}
	confirm, err := passwordPrompter("Confirm password: ")
	if err != nil {
		return err
	}
	if password != confirm {
		return errors.New("password confirmation does not match")
	}

	credential, err := provenance.DerivePasswordCredential(password)
	if err != nil {
		return err
	}
	ctx, cancel = context.WithTimeout(context.Background(), authCommandTimeout)
	storedCredential, rotated, err := s.UpsertHumanAuthCredential(ctx, store.UpsertHumanAuthCredentialParams{
		Algorithm:  credential.Algorithm,
		Iterations: credential.Iterations,
		SaltHex:    credential.SaltHex,
		HashHex:    credential.HashHex,
		Actor:      principal.Actor,
	})
	cancel()
	if err != nil {
		return err
	}

	if *jsonOut {
		return printJSON(out, jsonEnvelope{
			ResponseSchemaVersion: responseSchemaVersion,
			DBSchemaVersion:       dbVersion,
			Command:               "auth set-password",
			Data: authSetPasswordData{
				Configured: true,
				Rotated:    rotated,
				Algorithm:  storedCredential.Algorithm,
				Iterations: storedCredential.Iterations,
				UpdatedAt:  storedCredential.UpdatedAt,
				Actor:      storedCredential.RotatedBy,
			},
		})
	}

	if rotated {
		_, _ = fmt.Fprintln(out, "Rotated human auth password")
	} else {
		_, _ = fmt.Fprintln(out, "Configured human auth password")
	}
	_, _ = fmt.Fprintf(out, "Actor: %s\n", storedCredential.RotatedBy)
	_, _ = fmt.Fprintf(out, "Updated: %s\n", storedCredential.UpdatedAt)
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
	ui.nextSteps(
		fmt.Sprintf("memori issue show --key %s", next.Candidate.Issue.ID),
		fmt.Sprintf("memori issue update --key %s --status inprogress", next.Candidate.Issue.ID),
	)
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

func runGate(args []string, out io.Writer) error {
	if len(args) == 0 {
		return errors.New("gate subcommand required: template|set|evaluate|verify|status")
	}
	switch args[0] {
	case "template":
		return runGateTemplate(args[1:], out)
	case "set":
		return runGateSet(args[1:], out)
	case "evaluate":
		return runGateEvaluate(args[1:], out)
	case "verify":
		return runGateVerify(args[1:], out)
	case "status":
		return runGateStatus(args[1:], out)
	default:
		return fmt.Errorf("unknown gate subcommand %q", args[0])
	}
}

func runContext(args []string, out io.Writer) error {
	if len(args) == 0 {
		return errors.New("context subcommand required: checkpoint|summarize|close|rehydrate|packet|loops")
	}
	switch args[0] {
	case "checkpoint":
		return runContextCheckpoint(args[1:], out)
	case "summarize":
		return runContextSummarize(args[1:], out)
	case "close":
		return runContextClose(args[1:], out)
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
	commandID := fs.String("command-id", "", "idempotency command id")
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

	identity, err := resolveMutationIdentity(ctx, s, *dbPath, "context-checkpoint", *actor, *commandID, defaultMutationAuthDeps())
	if err != nil {
		return err
	}

	session, created, err := s.CheckpointSession(ctx, store.CheckpointSessionParams{
		SessionID: *sessionID,
		Trigger:   *trigger,
		Actor:     identity.Actor,
		CommandID: identity.CommandID,
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

func runContextSummarize(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("context summarize", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dbPath := fs.String("db", defaultDBPath(), "sqlite database path")
	sessionID := fs.String("session", "", "session id")
	note := fs.String("note", "", "summary note")
	actor := fs.String("actor", "", "actor id")
	commandID := fs.String("command-id", "", "idempotency command id")
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

	identity, err := resolveMutationIdentity(ctx, s, *dbPath, "context-summarize", *actor, *commandID, defaultMutationAuthDeps())
	if err != nil {
		return err
	}

	session, err := s.SummarizeSession(ctx, store.SummarizeSessionParams{
		SessionID: *sessionID,
		Note:      *note,
		Actor:     identity.Actor,
		CommandID: identity.CommandID,
	})
	if err != nil {
		return err
	}

	if *jsonOut {
		return printJSON(out, jsonEnvelope{
			ResponseSchemaVersion: responseSchemaVersion,
			DBSchemaVersion:       dbVersion,
			Command:               "context summarize",
			Data: contextSessionData{
				Session: session,
			},
		})
	}

	_, _ = fmt.Fprintf(out, "Summarized session %s\n", session.SessionID)
	if strings.TrimSpace(session.SummaryEventID) != "" {
		_, _ = fmt.Fprintf(out, "Summary Event: %s\n", session.SummaryEventID)
	}
	return nil
}

func runContextClose(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("context close", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dbPath := fs.String("db", defaultDBPath(), "sqlite database path")
	sessionID := fs.String("session", "", "session id")
	reason := fs.String("reason", "", "close reason")
	actor := fs.String("actor", "", "actor id")
	commandID := fs.String("command-id", "", "idempotency command id")
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

	identity, err := resolveMutationIdentity(ctx, s, *dbPath, "context-close", *actor, *commandID, defaultMutationAuthDeps())
	if err != nil {
		return err
	}

	session, err := s.CloseSession(ctx, store.CloseSessionParams{
		SessionID: *sessionID,
		Reason:    *reason,
		Actor:     identity.Actor,
		CommandID: identity.CommandID,
	})
	if err != nil {
		return err
	}

	if *jsonOut {
		return printJSON(out, jsonEnvelope{
			ResponseSchemaVersion: responseSchemaVersion,
			DBSchemaVersion:       dbVersion,
			Command:               "context close",
			Data: contextSessionData{
				Session: session,
			},
		})
	}

	_, _ = fmt.Fprintf(out, "Closed session %s\n", session.SessionID)
	if strings.TrimSpace(session.EndedAt) != "" {
		_, _ = fmt.Fprintf(out, "Ended At: %s\n", session.EndedAt)
	}
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
	commandID := fs.String("command-id", "", "idempotency command id")
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

	identity, err := resolveMutationIdentity(ctx, s, *dbPath, "context-packet-build", *actor, *commandID, defaultMutationAuthDeps())
	if err != nil {
		return err
	}

	packet, err := s.BuildRehydratePacket(ctx, store.BuildPacketParams{
		Scope:     *scope,
		ScopeID:   *scopeID,
		Actor:     identity.Actor,
		CommandID: identity.CommandID,
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
	actor := fs.String("actor", "", "actor id (optional)")
	commandID := fs.String("command-id", "", "command id (optional; requires MEMORI_ALLOW_MANUAL_COMMAND_ID=1)")
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

	identity, err := resolveMutationIdentity(ctx, s, *dbPath, "context-packet-use", *actor, *commandID, defaultMutationAuthDeps())
	if err != nil {
		return err
	}

	focus, packet, idempotent, err := s.UseRehydratePacket(ctx, store.UsePacketParams{
		AgentID:   *agentID,
		PacketID:  *packetID,
		Actor:     identity.Actor,
		CommandID: identity.CommandID,
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
				Focus:      focus,
				Packet:     packet,
				Idempotent: idempotent,
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
		return errors.New("gate template subcommand required: create|approve|list|pending")
	}
	switch args[0] {
	case "create":
		return runGateTemplateCreate(args[1:], out)
	case "approve":
		return runGateTemplateApprove(args[1:], out)
	case "list":
		return runGateTemplateList(args[1:], out)
	case "pending":
		return runGateTemplatePending(args[1:], out)
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
	commandID := fs.String("command-id", "", "stable idempotency key")
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

	identity, err := resolveMutationIdentity(ctx, s, *dbPath, "gate-template-create", *actor, *commandID, defaultMutationAuthDeps())
	if err != nil {
		return err
	}

	template, idempotent, err := s.CreateGateTemplate(ctx, store.CreateGateTemplateParams{
		TemplateID:     *templateID,
		Version:        *version,
		AppliesTo:      appliesTo,
		DefinitionJSON: string(definitionBytes),
		Actor:          identity.Actor,
		CommandID:      identity.CommandID,
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
	if template.Executable {
		if strings.TrimSpace(template.ApprovedBy) == "" {
			_, _ = fmt.Fprintln(out, "Approval: pending human approval before instantiate/verify")
		} else {
			_, _ = fmt.Fprintf(out, "Approval: approved by %s at %s\n", template.ApprovedBy, template.ApprovedAt)
		}
	}
	return nil
}

func runGateTemplateApprove(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("gate template approve", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dbPath := fs.String("db", defaultDBPath(), "sqlite database path")
	templateID := fs.String("id", "", "template id")
	version := fs.Int("version", 0, "template version (> 0)")
	actor := fs.String("actor", "", "actor id")
	commandID := fs.String("command-id", "", "stable idempotency key")
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

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	s, dbVersion, err := openInitializedStore(ctx, *dbPath)
	if err != nil {
		return err
	}
	defer s.Close()

	identity, err := resolveMutationIdentity(ctx, s, *dbPath, "gate-template-approve", *actor, *commandID, defaultMutationAuthDeps())
	if err != nil {
		return err
	}

	template, idempotent, err := s.ApproveGateTemplate(ctx, store.ApproveGateTemplateParams{
		TemplateID: *templateID,
		Version:    *version,
		Actor:      identity.Actor,
		CommandID:  identity.CommandID,
	})
	if err != nil {
		return err
	}

	if *jsonOut {
		return printJSON(out, jsonEnvelope{
			ResponseSchemaVersion: responseSchemaVersion,
			DBSchemaVersion:       dbVersion,
			Command:               "gate template approve",
			Data: gateTemplateApproveData{
				Template:   template,
				Idempotent: idempotent,
			},
		})
	}

	if idempotent {
		_, _ = fmt.Fprintf(out, "Gate template %s@%d is already approved.\n", template.TemplateID, template.Version)
	} else {
		_, _ = fmt.Fprintf(out, "Approved gate template %s@%d\n", template.TemplateID, template.Version)
	}
	_, _ = fmt.Fprintf(out, "Approved By: %s\n", template.ApprovedBy)
	_, _ = fmt.Fprintf(out, "Approved At: %s\n", template.ApprovedAt)
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

func runGateTemplatePending(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("gate template pending", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dbPath := fs.String("db", defaultDBPath(), "sqlite database path")
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

	templates, err := s.ListPendingExecutableGateTemplates(ctx)
	if err != nil {
		return err
	}

	if *jsonOut {
		return printJSON(out, jsonEnvelope{
			ResponseSchemaVersion: responseSchemaVersion,
			DBSchemaVersion:       dbVersion,
			Command:               "gate template pending",
			Data: gateTemplateListData{
				Count:     len(templates),
				Templates: templates,
			},
		})
	}

	if len(templates) == 0 {
		_, _ = fmt.Fprintln(out, "No pending executable gate templates.")
		return nil
	}
	for _, template := range templates {
		_, _ = fmt.Fprintf(
			out,
			"- %s@%d applies_to=%s hash=%s created_by=%s created_at=%s approval=pending-human-review\n",
			template.TemplateID,
			template.Version,
			strings.Join(template.AppliesTo, ","),
			template.DefinitionHash,
			template.CreatedBy,
			template.CreatedAt,
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
	commandID := fs.String("command-id", "", "stable idempotency key")
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

	identity, err := resolveMutationIdentity(ctx, s, *dbPath, "gate-set-instantiate", *actor, *commandID, defaultMutationAuthDeps())
	if err != nil {
		return err
	}

	gateSet, idempotent, err := s.InstantiateGateSet(ctx, store.InstantiateGateSetParams{
		IssueID:      *issue,
		TemplateRefs: templates,
		Actor:        identity.Actor,
		CommandID:    identity.CommandID,
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
	commandID := fs.String("command-id", "", "stable idempotency key")
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

	identity, err := resolveMutationIdentity(ctx, s, *dbPath, "gate-set-lock", *actor, *commandID, defaultMutationAuthDeps())
	if err != nil {
		return err
	}

	gateSet, lockedNow, err := s.LockGateSet(ctx, store.LockGateSetParams{
		IssueID:   *issue,
		CycleNo:   cyclePtr,
		Actor:     identity.Actor,
		CommandID: identity.CommandID,
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

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	s, dbVersion, err := openInitializedStore(ctx, *dbPath)
	if err != nil {
		return err
	}
	defer s.Close()

	identity, err := resolveMutationIdentity(ctx, s, *dbPath, "gate-evaluate", *actor, *commandID, defaultMutationAuthDeps())
	if err != nil {
		return err
	}

	evaluation, event, idempotent, err := s.EvaluateGate(ctx, store.EvaluateGateParams{
		IssueID:      *issue,
		GateID:       *gate,
		Result:       *result,
		EvidenceRefs: evidence,
		Actor:        identity.Actor,
		CommandID:    identity.CommandID,
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

func runGateVerify(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("gate verify", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dbPath := fs.String("db", defaultDBPath(), "sqlite database path")
	issue := fs.String("issue", "", "issue key")
	gate := fs.String("gate", "", "gate id")
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

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	s, dbVersion, err := openInitializedStore(ctx, *dbPath)
	if err != nil {
		return err
	}
	defer s.Close()

	identity, err := resolveMutationIdentity(ctx, s, *dbPath, "gate-verify", *actor, *commandID, defaultMutationAuthDeps())
	if err != nil {
		return err
	}

	if evaluation, event, found, err := s.LookupGateEvaluationByCommand(ctx, identity.Actor, identity.CommandID); err != nil {
		return err
	} else if found {
		replay, err := gateVerifyReplayFromEvaluation(evaluation)
		if err != nil {
			return err
		}
		return printGateVerifyResult(out, dbVersion, evaluation, event, replay.Command, replay.ExitCode, replay.OutputSHA, true, *jsonOut)
	}

	spec, err := s.LookupGateVerificationSpec(ctx, *issue, *gate)
	if err != nil {
		return err
	}

	startedAt := time.Now().UTC().Format(time.RFC3339Nano)
	cmd := exec.CommandContext(ctx, "sh", "-lc", spec.Command)
	output, runErr := cmd.CombinedOutput()
	finishedAt := time.Now().UTC().Format(time.RFC3339Nano)
	exitCode := 0
	if runErr != nil {
		var exitErr *exec.ExitError
		if errors.As(runErr, &exitErr) {
			exitCode = exitErr.ExitCode()
		} else {
			return fmt.Errorf("run verifier command for gate %s: %w", spec.GateID, runErr)
		}
	}
	result := "PASS"
	if exitCode != 0 {
		result = "FAIL"
	}
	outputDigest := sha256.Sum256(output)
	outputHash := hex.EncodeToString(outputDigest[:])
	evidence := []string{
		"verifier:memori-cli-gate-verify",
		"command:" + spec.Command,
		fmt.Sprintf("exit:%d", exitCode),
		"output_sha256:" + outputHash,
	}

	evaluation, event, idempotent, err := s.EvaluateGate(ctx, store.EvaluateGateParams{
		IssueID:      spec.IssueID,
		GateID:       spec.GateID,
		Result:       result,
		EvidenceRefs: evidence,
		Proof: &store.GateEvaluationProof{
			Verifier:      "memori-cli-gate-verify",
			Runner:        "sh",
			RunnerVersion: "1",
			ExitCode:      exitCode,
			StartedAt:     startedAt,
			FinishedAt:    finishedAt,
			GateSetHash:   spec.GateSetHash,
		},
		Actor:     identity.Actor,
		CommandID: identity.CommandID,
	})
	if err != nil {
		return err
	}

	if idempotent {
		replay, err := gateVerifyReplayFromEvaluation(evaluation)
		if err != nil {
			return err
		}
		return printGateVerifyResult(out, dbVersion, evaluation, event, replay.Command, replay.ExitCode, replay.OutputSHA, true, *jsonOut)
	}

	return printGateVerifyResult(out, dbVersion, evaluation, event, spec.Command, exitCode, outputHash, false, *jsonOut)
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

	ui := newTextUI(out)
	ui.heading("Gate Status")
	ui.field("Issue", status.IssueID)
	ui.field("Gate Set", fmt.Sprintf("%s (cycle %d)", status.GateSetID, status.CycleNo))
	if strings.TrimSpace(status.LockedAt) != "" {
		ui.field("Locked At", status.LockedAt)
	}
	ui.field("Summary", summarizeGateResults(status.Gates, ui.colors))
	ui.blank()
	ui.section("Checks")
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
	if hasIncompleteRequiredGate(status.Gates) {
		ui.nextSteps(
			fmt.Sprintf("memori gate evaluate --issue %s --gate <gate-id> --result PASS|FAIL|BLOCKED --evidence <ref>", status.IssueID),
		)
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
		line := fmt.Sprintf("- #%d %s %s actor=%s command_id=%s", event.EventOrder, event.EventType, event.CreatedAt, event.Actor, event.CommandID)
		if strings.TrimSpace(event.CausationID) != "" {
			line += " causation_id=" + event.CausationID
		}
		if strings.TrimSpace(event.CorrelationID) != "" {
			line += " correlation_id=" + event.CorrelationID
		}
		_, _ = fmt.Fprintln(out, line)
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

type mutationAuthDeps struct {
	resolvePrincipal func() (provenance.Principal, error)
	promptPassword   func(string) (string, error)
}

type mutationIdentity struct {
	Principal provenance.Principal
	Actor     string
	CommandID string
}

func defaultMutationAuthDeps() mutationAuthDeps {
	return mutationAuthDeps{
		resolvePrincipal: provenance.ResolvePrincipal,
		promptPassword:   passwordPrompter,
	}
}

func resolveWriteActor(actorHint string, deps mutationAuthDeps) (provenance.Principal, string, error) {
	principal, err := deps.resolvePrincipal()
	if err != nil {
		return provenance.Principal{}, "", err
	}
	_ = strings.TrimSpace(actorHint)
	return principal, principal.Actor, nil
}

func resolveMutationIdentity(
	ctx context.Context,
	s *store.Store,
	dbPath string,
	operation string,
	actorHint string,
	commandIDHint string,
	deps mutationAuthDeps,
) (mutationIdentity, error) {
	principal, actor, err := resolveWriteActor(actorHint, deps)
	if err != nil {
		return mutationIdentity{}, err
	}
	if principal.Kind == provenance.PrincipalHuman {
		credential, configured, err := s.GetHumanAuthCredential(ctx)
		if err != nil {
			return mutationIdentity{}, err
		}
		if !configured {
			return mutationIdentity{}, fmt.Errorf("human auth is not configured (run: memori auth set-password --db %s)", dbPath)
		}
		password, err := deps.promptPassword("Password: ")
		if err != nil {
			return mutationIdentity{}, err
		}
		ok, err := provenance.VerifyPassword(password, provenance.PasswordCredential{
			Algorithm:  credential.Algorithm,
			Iterations: credential.Iterations,
			SaltHex:    credential.SaltHex,
			HashHex:    credential.HashHex,
		})
		if err != nil {
			return mutationIdentity{}, err
		}
		if !ok {
			return mutationIdentity{}, errors.New("human auth verification failed")
		}
	}

	commandID, err := provenance.ResolveCommandID(operation, commandIDHint)
	if err != nil {
		return mutationIdentity{}, err
	}
	return mutationIdentity{
		Principal: principal,
		Actor:     actor,
		CommandID: commandID,
	}, nil
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
	switch entityType {
	case "issue", "session", "packet", "focus":
		return entityType, entityID, nil
	case "gate-template", "gate_template":
		return "gate_template", entityID, nil
	case "gate-set", "gate_set":
		return "gate_set", entityID, nil
	default:
		return "", "", fmt.Errorf("invalid entity type %q (expected issue|session|packet|focus|gate-template|gate-set)", parts[0])
	}
}

func gateDefinitionHasExecutableCommand(definitionJSON string) (bool, error) {
	raw := strings.TrimSpace(definitionJSON)
	if raw == "" {
		return false, errors.New("--file must contain JSON")
	}

	var parsed struct {
		Gates []struct {
			Criteria map[string]any `json:"criteria"`
		} `json:"gates"`
	}
	if err := json.Unmarshal([]byte(raw), &parsed); err != nil {
		return false, fmt.Errorf("invalid gate definition JSON: %w", err)
	}
	for _, gate := range parsed.Gates {
		command, _ := gate.Criteria["command"].(string)
		if strings.TrimSpace(command) != "" {
			return true, nil
		}
	}
	return false, nil
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

type authStatusData struct {
	Configured bool   `json:"configured"`
	Algorithm  string `json:"algorithm,omitempty"`
	Iterations int    `json:"iterations,omitempty"`
	UpdatedAt  string `json:"updated_at,omitempty"`
	RotatedBy  string `json:"rotated_by,omitempty"`
}

type authSetPasswordData struct {
	Configured bool   `json:"configured"`
	Rotated    bool   `json:"rotated"`
	Algorithm  string `json:"algorithm"`
	Iterations int    `json:"iterations"`
	UpdatedAt  string `json:"updated_at"`
	Actor      string `json:"actor"`
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

type gateVerifyData struct {
	Evaluation store.GateEvaluation `json:"evaluation"`
	Event      store.Event          `json:"event"`
	Command    string               `json:"command"`
	ExitCode   int                  `json:"exit_code"`
	OutputSHA  string               `json:"output_sha256"`
	Idempotent bool                 `json:"idempotent"`
}

type gateVerifyReplay struct {
	Command   string
	ExitCode  int
	OutputSHA string
}

func gateVerifyReplayFromEvaluation(evaluation store.GateEvaluation) (gateVerifyReplay, error) {
	replay := gateVerifyReplay{}
	if evaluation.Proof != nil {
		replay.ExitCode = evaluation.Proof.ExitCode
	}

	for _, ref := range evaluation.EvidenceRefs {
		if value, ok := strings.CutPrefix(ref, "command:"); ok {
			replay.Command = strings.TrimSpace(value)
			continue
		}
		if value, ok := strings.CutPrefix(ref, "output_sha256:"); ok {
			replay.OutputSHA = strings.TrimSpace(value)
			continue
		}
		if value, ok := strings.CutPrefix(ref, "exit:"); ok && evaluation.Proof == nil {
			exitCode, err := strconv.Atoi(strings.TrimSpace(value))
			if err != nil {
				return gateVerifyReplay{}, fmt.Errorf("decode persisted verifier exit code for gate %s: %w", evaluation.GateID, err)
			}
			replay.ExitCode = exitCode
		}
	}

	if replay.Command == "" {
		return gateVerifyReplay{}, fmt.Errorf("persisted verifier command is missing for gate %s", evaluation.GateID)
	}
	if replay.OutputSHA == "" {
		return gateVerifyReplay{}, fmt.Errorf("persisted verifier output_sha256 is missing for gate %s", evaluation.GateID)
	}
	return replay, nil
}

func printGateVerifyResult(
	out io.Writer,
	dbVersion int,
	evaluation store.GateEvaluation,
	event store.Event,
	command string,
	exitCode int,
	outputSHA string,
	idempotent bool,
	jsonOut bool,
) error {
	if jsonOut {
		return printJSON(out, jsonEnvelope{
			ResponseSchemaVersion: responseSchemaVersion,
			DBSchemaVersion:       dbVersion,
			Command:               "gate verify",
			Data: gateVerifyData{
				Evaluation: evaluation,
				Event:      event,
				Command:    command,
				ExitCode:   exitCode,
				OutputSHA:  outputSHA,
				Idempotent: idempotent,
			},
		})
	}

	if idempotent {
		_, _ = fmt.Fprintf(out, "Gate verification already recorded for issue %s gate %s.\n", evaluation.IssueID, evaluation.GateID)
	} else {
		_, _ = fmt.Fprintf(out, "Verified gate %s for issue %s -> %s (exit=%d)\n", evaluation.GateID, evaluation.IssueID, evaluation.Result, exitCode)
	}
	_, _ = fmt.Fprintf(out, "Gate Set: %s\n", evaluation.GateSetID)
	_, _ = fmt.Fprintf(out, "Output SHA256: %s\n", outputSHA)
	return nil
}

type gateTemplateCreateData struct {
	Template   store.GateTemplate `json:"template"`
	Idempotent bool               `json:"idempotent"`
}

type gateTemplateApproveData struct {
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

type contextSessionData struct {
	Session store.Session `json:"session"`
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
	Focus      store.AgentFocus      `json:"focus"`
	Packet     store.RehydratePacket `json:"packet"`
	Idempotent bool                  `json:"idempotent"`
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
	switch strings.ToLower(strings.TrimSpace(os.Getenv("MEMORI_COLOR"))) {
	case "always":
		return true
	case "never":
		return false
	}
	if os.Getenv("NO_COLOR") != "" || strings.TrimSpace(os.Getenv("CLICOLOR")) == "0" {
		return false
	}
	if force := strings.TrimSpace(os.Getenv("CLICOLOR_FORCE")); force != "" && force != "0" {
		return true
	}
	if force := strings.TrimSpace(os.Getenv("FORCE_COLOR")); force != "" && force != "0" {
		return true
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
	case "WontDo":
		return "35" // magenta
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

func summarizeGateResults(gates []store.GateStatusItem, colors bool) string {
	if len(gates) == 0 {
		return "no gates"
	}

	counts := make(map[string]int, 4)
	order := []string{"PASS", "FAIL", "BLOCKED", "MISSING"}
	for _, gate := range gates {
		counts[gate.Result]++
	}

	parts := make([]string, 0, len(order))
	for _, result := range order {
		if counts[result] == 0 {
			continue
		}
		label := colorize(colors, colorForGateResult(result), result)
		parts = append(parts, fmt.Sprintf("%s=%d", label, counts[result]))
	}
	if len(parts) == 0 {
		return "no results"
	}
	return strings.Join(parts, ", ")
}

func hasIncompleteRequiredGate(gates []store.GateStatusItem) bool {
	for _, gate := range gates {
		if gate.Required && gate.Result != "PASS" {
			return true
		}
	}
	return false
}

func printJSON(out io.Writer, v any) error {
	enc := json.NewEncoder(out)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

func printHelp(out io.Writer) {
	ui := newTextUI(out)
	ui.heading("memori")
	_, _ = fmt.Fprintln(out, "Local context bridge + agile issue ledger")
	ui.blank()

	ui.section("Human Workflows")
	ui.bullet("memori auth status [--db <path>] [--json]")
	ui.bullet("memori backlog [--type epic|story|task|bug] [--status todo|inprogress|blocked|done|wontdo] [--parent <key>] [--json]")
	ui.bullet("memori board [--db <path>] [--agent <id>] [--watch] [--interval <duration>] [--json]")
	ui.bullet("memori issue show --key <prefix-shortSHA> [--json]")
	ui.bullet("memori gate status --issue <prefix-shortSHA> [--cycle <n>] [--json]")
	ui.bullet("memori event log --entity <entityType:id|id> [--json]")
	ui.bullet("memori db status [--json]")

	ui.blank()
	ui.section("Agent Workflows")
	ui.bullet("memori issue next [--agent <id>] [--json]")
	ui.bullet("memori board [--db <path>] [--agent <id>] [--watch] [--interval <duration>] [--json]")
	ui.bullet("memori context checkpoint --session <id> [--trigger <trigger>] [--actor <actor>] [--command-id <id>] [--json]")
	ui.bullet("memori context summarize --session <id> [--note <text>] [--actor <actor>] [--command-id <id>] [--json]")
	ui.bullet("memori context close --session <id> [--reason <text>] [--actor <actor>] [--command-id <id>] [--json]")
	ui.bullet("memori context packet build --scope issue|session --id <id> [--actor <actor>] [--command-id <id>] [--json]")
	ui.bullet("memori context packet show --packet <id> [--json]")
	ui.bullet("memori context packet use --agent <id> --packet <id> [--actor <actor>] [--command-id <id>] [--json]")
	ui.bullet("memori context rehydrate --session <id> [--json]")
	ui.bullet("memori context loops [--issue <prefix-shortSHA>] [--cycle <n>] [--json]")

	ui.blank()
	ui.section("Create And Update Work")
	ui.bullet("memori help [--json]")
	ui.bullet("memori auth set-password [--db <path>] [--json]")
	ui.bullet("memori init [--db <path>] [--issue-prefix <prefix>] [--json]")
	ui.bullet("memori issue create --type epic|story|task|bug --title <title> [--description <text>] [--acceptance-criteria <text>] [--reference <ref>]... [--parent <key>] [--key <prefix-shortSHA>] [--actor <actor>] [--command-id <id>] [--json]")
	ui.bullet("memori issue link --child <prefix-shortSHA> --parent <prefix-shortSHA> [--actor <actor>] [--command-id <id>] [--json]")
	ui.bullet("memori issue update --key <prefix-shortSHA> [--title <title>] [--status todo|inprogress|blocked|done|wontdo] [--priority <value>] [--label <label>]... [--description <text>] [--acceptance-criteria <text>] [--reference <ref>]... [--actor <actor>] [--command-id <id>] [--json]")
	ui.bullet("memori gate template create --id <template-id> --version <n> --applies-to epic|story|task|bug [--applies-to ...] --file <path> [--actor <actor>] [--command-id <id>] [--json]")
	ui.bullet("memori gate template approve --id <template-id> --version <n> [--actor <actor>] [--command-id <id>] [--json]")
	ui.bullet("memori gate template list [--type epic|story|task|bug] [--json]")
	ui.bullet("memori gate template pending [--db <path>] [--json]")
	ui.bullet("memori gate set instantiate --issue <prefix-shortSHA> --template <template-id@version> [--template ...] [--actor <actor>] [--command-id <id>] [--json]")
	ui.bullet("memori gate set lock --issue <prefix-shortSHA> [--cycle <n>] [--actor <actor>] [--command-id <id>] [--json]")
	ui.bullet("memori gate evaluate --issue <prefix-shortSHA> --gate <gate-id> --result PASS|FAIL|BLOCKED --evidence <ref> [--evidence <ref>]... [--actor <actor>] [--command-id <id>] [--json]")
	ui.bullet("memori gate verify --issue <prefix-shortSHA> --gate <gate-id> [--actor <actor>] [--command-id <id>] [--json]")
	ui.bullet("memori db migrate [--to <version>] [--json]")
	ui.bullet("memori db verify [--json]")
	ui.bullet("memori db backup --out <path> [--json]")
	ui.bullet("memori db replay [--json]")

	ui.blank()
	ui.section("Tips")
	ui.bullet("Use --json for automation and contract-stable output.")
	ui.bullet("Human writes require a configured password and interactive verification via `memori auth set-password`.")
	ui.bullet("Mutation actors are derived from the runtime principal; `--actor` is informational only.")
	ui.bullet("Command IDs are generated automatically; manual `--command-id` is gated behind MEMORI_ALLOW_MANUAL_COMMAND_ID=1.")
	ui.bullet("Control ANSI color with MEMORI_COLOR=auto|always|never. NO_COLOR, CLICOLOR, CLICOLOR_FORCE, and FORCE_COLOR are also honored.")
}
