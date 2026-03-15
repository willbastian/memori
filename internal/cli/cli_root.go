package cli

import (
	"context"
	"flag"
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/willbastian/memori/internal/dbschema"
	"github.com/willbastian/memori/internal/store"
)

var buildVersion = "dev"
var buildCommit = "unknown"
var buildDate = "unknown"

const buildModulePath = "github.com/willbastian/memori"

type helpData struct {
	Commands []string `json:"commands"`
}

type initData struct {
	DBPath         string `json:"db_path"`
	Status         string `json:"status"`
	IssueKeyPrefix string `json:"issue_key_prefix"`
}

type versionData struct {
	Version           string `json:"version"`
	Commit            string `json:"commit"`
	BuildDate         string `json:"build_date"`
	ModulePath        string `json:"module_path"`
	SchemaHeadVersion int    `json:"schema_head_version"`
}

func Run(args []string, stdout, stderr io.Writer) error {
	_ = stderr
	if len(args) == 0 {
		printHelp(stdout)
		return nil
	}

	switch args[0] {
	case "help", "--help", "-h":
		return runHelp(args[1:], stdout)
	case "version", "--version":
		return runVersion(args[1:], stdout)
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
			"memori version [--json]",
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
			"memori gate set instantiate --issue <prefix-shortSHA> [--template <template-id@version> ...] [--actor <actor>] [--command-id <id>] [--json]",
			"memori gate set lock --issue <prefix-shortSHA> [--cycle <n>] [--actor <actor>] [--command-id <id>] [--json]",
			"memori gate evaluate --issue <prefix-shortSHA> --gate <gate-id> --result PASS|FAIL|BLOCKED --evidence <ref> [--evidence <ref>]... [--actor <actor>] [--command-id <id>] [--json]",
			"memori gate verify --issue <prefix-shortSHA> --gate <gate-id> [--actor <actor>] [--command-id <id>] [--json]",
			"memori gate status --issue <prefix-shortSHA> [--cycle <n>] [--json]",
			"memori context start --issue <prefix-shortSHA> [--agent <id>] [--session <id>] [--trigger <trigger>] [--actor <actor>] [--command-id <id>] [--json]",
			"memori context save [--session <id>] [--note <text>] [--close] [--reason <text>] [--actor <actor>] [--command-id <id>] [--json]",
			"memori context checkpoint [--session <id>] [--trigger <trigger>] [--actor <actor>] [--command-id <id>] [--json]",
			"memori context summarize [--session <id>] [--note <text>] [--actor <actor>] [--command-id <id>] [--json]",
			"memori context close [--session <id>] [--reason <text>] [--actor <actor>] [--command-id <id>] [--json]",
			"memori context rehydrate [--session <id>] [--json]",
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

func runVersion(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("version", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	jsonOut := fs.Bool("json", false, "machine-readable output")
	if err := fs.Parse(args); err != nil {
		return err
	}

	headVersion, err := dbschema.HeadVersion()
	if err != nil {
		return err
	}
	data := versionData{
		Version:           buildVersion,
		Commit:            buildCommit,
		BuildDate:         buildDate,
		ModulePath:        buildModulePath,
		SchemaHeadVersion: headVersion,
	}

	if *jsonOut {
		return printJSON(out, jsonEnvelope{
			ResponseSchemaVersion: responseSchemaVersion,
			DBSchemaVersion:       headVersion,
			Command:               "version",
			Data:                  data,
		})
	}

	_, _ = fmt.Fprintf(out, "memori %s\n", data.Version)
	_, _ = fmt.Fprintf(out, "commit: %s\n", data.Commit)
	_, _ = fmt.Fprintf(out, "built: %s\n", data.BuildDate)
	_, _ = fmt.Fprintf(out, "module: %s\n", data.ModulePath)
	_, _ = fmt.Fprintf(out, "schema_head_version: %d\n", data.SchemaHeadVersion)
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
	ui.bullet("memori context start --issue <prefix-shortSHA> [--agent <id>] [--session <id>] [--trigger <trigger>] [--actor <actor>] [--command-id <id>] [--json]")
	ui.bullet("memori context save [--session <id>] [--note <text>] [--close] [--reason <text>] [--actor <actor>] [--command-id <id>] [--json]")
	ui.bullet("memori context checkpoint [--session <id>] [--trigger <trigger>] [--actor <actor>] [--command-id <id>] [--json]")
	ui.bullet("memori context summarize [--session <id>] [--note <text>] [--actor <actor>] [--command-id <id>] [--json]")
	ui.bullet("memori context close [--session <id>] [--reason <text>] [--actor <actor>] [--command-id <id>] [--json]")
	ui.bullet("memori context packet build --scope issue|session --id <id> [--actor <actor>] [--command-id <id>] [--json]")
	ui.bullet("memori context packet show --packet <id> [--json]")
	ui.bullet("memori context packet use --agent <id> --packet <id> [--actor <actor>] [--command-id <id>] [--json]")
	ui.bullet("memori context rehydrate [--session <id>] [--json]")
	ui.bullet("memori context loops [--issue <prefix-shortSHA>] [--cycle <n>] [--json]")

	ui.blank()
	ui.section("Create And Update Work")
	ui.bullet("memori help [--json]")
	ui.bullet("memori version [--json]")
	ui.bullet("memori auth set-password [--db <path>] [--json]")
	ui.bullet("memori init [--db <path>] [--issue-prefix <prefix>] [--json]")
	ui.bullet("memori issue create --type epic|story|task|bug --title <title> [--description <text>] [--acceptance-criteria <text>] [--reference <ref>]... [--parent <key>] [--key <prefix-shortSHA>] [--actor <actor>] [--command-id <id>] [--json]")
	ui.bullet("memori issue link --child <prefix-shortSHA> --parent <prefix-shortSHA> [--actor <actor>] [--command-id <id>] [--json]")
	ui.bullet("memori issue update --key <prefix-shortSHA> [--title <title>] [--status todo|inprogress|blocked|done|wontdo] [--priority <value>] [--label <label>]... [--description <text>] [--acceptance-criteria <text>] [--reference <ref>]... [--actor <actor>] [--command-id <id>] [--json]")
	ui.bullet("memori gate template create --id <template-id> --version <n> --applies-to epic|story|task|bug [--applies-to ...] --file <path> [--actor <actor>] [--command-id <id>] [--json]")
	ui.bullet("memori gate template approve --id <template-id> --version <n> [--actor <actor>] [--command-id <id>] [--json]")
	ui.bullet("memori gate template list [--type epic|story|task|bug] [--json]")
	ui.bullet("memori gate template pending [--db <path>] [--json]")
	ui.bullet("memori gate set instantiate --issue <prefix-shortSHA> [--template <template-id@version> ...] [--actor <actor>] [--command-id <id>] [--json]")
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
