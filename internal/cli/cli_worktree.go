package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/willbastian/memori/internal/store"
)

type worktreeData struct {
	Worktree store.Worktree `json:"worktree"`
}

type worktreeListData struct {
	Count     int              `json:"count"`
	Worktrees []store.Worktree `json:"worktrees"`
}

func runWorktree(args []string, out io.Writer) error {
	if len(args) == 0 {
		return errors.New("worktree subcommand required: register|adopt-cwd|attach|detach|archive|show|list")
	}

	switch args[0] {
	case "register":
		return runWorktreeRegister(args[1:], out)
	case "adopt-cwd":
		return runWorktreeAdoptCWD(args[1:], out)
	case "attach":
		return runWorktreeAttach(args[1:], out)
	case "detach":
		return runWorktreeDetach(args[1:], out)
	case "archive":
		return runWorktreeArchive(args[1:], out)
	case "show":
		return runWorktreeShow(args[1:], out)
	case "list":
		return runWorktreeList(args[1:], out)
	default:
		return fmt.Errorf("unknown worktree subcommand %q", args[0])
	}
}

func runWorktreeRegister(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("worktree register", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dbPath := fs.String("db", defaultDBPath(), "sqlite database path")
	worktreeID := fs.String("id", "", "optional worktree id")
	path := fs.String("path", "", "worktree path")
	repoRoot := fs.String("repo-root", "", "repository root path")
	branch := fs.String("branch", "", "worktree branch name")
	head := fs.String("head", "", "worktree head oid")
	actor := fs.String("actor", "", "actor hint")
	commandID := fs.String("command-id", "", "stable command id")
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

	identity, err := resolveMutationIdentity(ctx, s, *dbPath, "worktree register", *actor, *commandID, defaultMutationAuthDeps())
	if err != nil {
		return err
	}

	worktreePath, err := normalizeCLIPath(*path)
	if err != nil {
		return err
	}
	repoRootPath, err := normalizeCLIPath(*repoRoot)
	if err != nil {
		return err
	}
	if repoRootPath == "" {
		repoRootPath = worktreePath
	}

	worktree, _, _, err := s.RegisterWorktree(ctx, store.RegisterWorktreeParams{
		WorktreeID: *worktreeID,
		Path:       worktreePath,
		RepoRoot:   repoRootPath,
		Branch:     *branch,
		HeadOID:    *head,
		Actor:      identity.Actor,
		CommandID:  identity.CommandID,
	})
	if err != nil {
		return err
	}
	return renderWorktreeResult(out, *jsonOut, dbVersion, "worktree register", worktree, "Registered worktree")
}

func runWorktreeAdoptCWD(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("worktree adopt-cwd", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dbPath := fs.String("db", defaultDBPath(), "sqlite database path")
	worktreeID := fs.String("id", "", "optional worktree id")
	repoRoot := fs.String("repo-root", "", "repository root path")
	branch := fs.String("branch", "", "worktree branch name")
	head := fs.String("head", "", "worktree head oid")
	actor := fs.String("actor", "", "actor hint")
	commandID := fs.String("command-id", "", "stable command id")
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

	identity, err := resolveMutationIdentity(ctx, s, *dbPath, "worktree adopt-cwd", *actor, *commandID, defaultMutationAuthDeps())
	if err != nil {
		return err
	}

	cwd, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("resolve cwd: %w", err)
	}
	worktreePath, err := normalizeCLIPath(cwd)
	if err != nil {
		return err
	}
	repoRootPath, err := normalizeCLIPath(*repoRoot)
	if err != nil {
		return err
	}
	if repoRootPath == "" {
		repoRootPath, err = inferRepoRootFromPath(worktreePath)
		if err != nil {
			return err
		}
	}

	worktree, _, _, err := s.RegisterWorktree(ctx, store.RegisterWorktreeParams{
		WorktreeID: *worktreeID,
		Path:       worktreePath,
		RepoRoot:   repoRootPath,
		Branch:     *branch,
		HeadOID:    *head,
		Actor:      identity.Actor,
		CommandID:  identity.CommandID,
	})
	if err != nil {
		return err
	}
	return renderWorktreeResult(out, *jsonOut, dbVersion, "worktree adopt-cwd", worktree, "Adopted cwd as worktree")
}

func runWorktreeAttach(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("worktree attach", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dbPath := fs.String("db", defaultDBPath(), "sqlite database path")
	worktreeID := fs.String("worktree", "", "worktree id")
	issueID := fs.String("issue", "", "issue key")
	actor := fs.String("actor", "", "actor hint")
	commandID := fs.String("command-id", "", "stable command id")
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

	identity, err := resolveMutationIdentity(ctx, s, *dbPath, "worktree attach", *actor, *commandID, defaultMutationAuthDeps())
	if err != nil {
		return err
	}
	worktree, _, _, err := s.AttachWorktree(ctx, store.AttachWorktreeParams{
		WorktreeID: *worktreeID,
		IssueID:    *issueID,
		Actor:      identity.Actor,
		CommandID:  identity.CommandID,
	})
	if err != nil {
		return err
	}
	return renderWorktreeResult(out, *jsonOut, dbVersion, "worktree attach", worktree, "Attached worktree")
}

func runWorktreeDetach(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("worktree detach", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dbPath := fs.String("db", defaultDBPath(), "sqlite database path")
	worktreeID := fs.String("worktree", "", "worktree id")
	actor := fs.String("actor", "", "actor hint")
	commandID := fs.String("command-id", "", "stable command id")
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

	identity, err := resolveMutationIdentity(ctx, s, *dbPath, "worktree detach", *actor, *commandID, defaultMutationAuthDeps())
	if err != nil {
		return err
	}
	worktree, _, _, err := s.DetachWorktree(ctx, store.DetachWorktreeParams{
		WorktreeID: *worktreeID,
		Actor:      identity.Actor,
		CommandID:  identity.CommandID,
	})
	if err != nil {
		return err
	}
	return renderWorktreeResult(out, *jsonOut, dbVersion, "worktree detach", worktree, "Detached worktree")
}

func runWorktreeArchive(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("worktree archive", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dbPath := fs.String("db", defaultDBPath(), "sqlite database path")
	worktreeID := fs.String("worktree", "", "worktree id")
	actor := fs.String("actor", "", "actor hint")
	commandID := fs.String("command-id", "", "stable command id")
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

	identity, err := resolveMutationIdentity(ctx, s, *dbPath, "worktree archive", *actor, *commandID, defaultMutationAuthDeps())
	if err != nil {
		return err
	}
	worktree, _, _, err := s.ArchiveWorktree(ctx, store.ArchiveWorktreeParams{
		WorktreeID: *worktreeID,
		Actor:      identity.Actor,
		CommandID:  identity.CommandID,
	})
	if err != nil {
		return err
	}
	return renderWorktreeResult(out, *jsonOut, dbVersion, "worktree archive", worktree, "Archived worktree")
}

func runWorktreeShow(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("worktree show", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dbPath := fs.String("db", defaultDBPath(), "sqlite database path")
	worktreeID := fs.String("worktree", "", "worktree id")
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

	worktree, err := s.GetWorktree(ctx, *worktreeID)
	if err != nil {
		return err
	}
	return renderWorktreeResult(out, *jsonOut, dbVersion, "worktree show", worktree, "Worktree")
}

func runWorktreeList(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("worktree list", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dbPath := fs.String("db", defaultDBPath(), "sqlite database path")
	issueID := fs.String("issue", "", "filter by issue key")
	status := fs.String("status", "", "filter by status: active|archived")
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

	worktrees, err := s.ListWorktrees(ctx, store.ListWorktreesParams{
		IssueID: *issueID,
		Status:  *status,
	})
	if err != nil {
		return err
	}

	if *jsonOut {
		return printJSON(out, jsonEnvelope{
			ResponseSchemaVersion: responseSchemaVersion,
			DBSchemaVersion:       dbVersion,
			Command:               "worktree list",
			Data: worktreeListData{
				Count:     len(worktrees),
				Worktrees: worktrees,
			},
		})
	}

	if len(worktrees) == 0 {
		_, _ = fmt.Fprintln(out, "No worktrees matched the filters.")
		return nil
	}
	ui := newTextUI(out)
	ui.heading("Worktrees")
	for _, worktree := range worktrees {
		line := fmt.Sprintf("%s [%s] %s", worktree.WorktreeID, worktree.Status, worktree.Path)
		if strings.TrimSpace(worktree.IssueID) != "" {
			line += " issue=" + worktree.IssueID
		}
		if strings.TrimSpace(worktree.Branch) != "" {
			line += " branch=" + worktree.Branch
		}
		ui.bullet(line)
	}
	return nil
}

func renderWorktreeResult(out io.Writer, jsonOut bool, dbVersion int, command string, worktree store.Worktree, heading string) error {
	if jsonOut {
		return printJSON(out, jsonEnvelope{
			ResponseSchemaVersion: responseSchemaVersion,
			DBSchemaVersion:       dbVersion,
			Command:               command,
			Data:                  worktreeData{Worktree: worktree},
		})
	}

	ui := newTextUI(out)
	ui.success(heading)
	ui.field("Worktree", worktree.WorktreeID)
	ui.field("Status", worktree.Status)
	ui.field("Path", worktree.Path)
	ui.field("Repo Root", worktree.RepoRoot)
	ui.field("Issue", worktree.IssueID)
	ui.field("Branch", worktree.Branch)
	ui.field("Head", worktree.HeadOID)
	return nil
}

func normalizeCLIPath(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", nil
	}
	abs, err := filepath.Abs(raw)
	if err != nil {
		return "", fmt.Errorf("resolve absolute path for %q: %w", raw, err)
	}
	return filepath.Clean(abs), nil
}

func inferRepoRootFromPath(path string) (string, error) {
	current := filepath.Clean(path)
	for {
		gitPath := filepath.Join(current, ".git")
		if _, err := os.Stat(gitPath); err == nil {
			return current, nil
		}
		parent := filepath.Dir(current)
		if parent == current {
			return "", fmt.Errorf("could not infer repo root from %s", path)
		}
		current = parent
	}
}
