package cli

import (
	"errors"
	"fmt"
	"os"
	"strings"
)

const (
	defaultAgentsMDPath        = "AGENTS.md"
	agentsLandThePlaneStartTag = "<!-- memori:land-the-plane:start -->"
	agentsLandThePlaneEndTag   = "<!-- memori:land-the-plane:end -->"
)

var managedAgentsLandThePlaneSection = strings.Join([]string{
	"<!-- memori:land-the-plane:start -->",
	"## Non-Interactive Agent Setup",
	"- Before mutating Memori state from an agent or automation flow, declare the writer as an LLM explicitly:",
	"```bash",
	"export MEMORI_PRINCIPAL=llm",
	"export MEMORI_LLM_PROVIDER=openai",
	"export MEMORI_LLM_MODEL=gpt-5",
	"export MEMORI_ALLOW_MANUAL_COMMAND_ID=1",
	"```",
	"- If you skip `MEMORI_PRINCIPAL=llm`, Memori will treat the caller as a human writer and mutating commands will prompt for the configured password.",
	"",
	"## Worktree Continuity",
	"- When the current work already lives in a Git worktree, register that workspace in Memori so issue, board, and resume flows can carry the same execution context forward.",
	"- If you are standing in the worktree already, prefer `memori worktree adopt-cwd --branch <branch>`; otherwise use `memori worktree register --path <path> --repo-root <repo-root> --branch <branch>`.",
	"- Attach the recorded workspace to the active issue with `memori worktree attach --worktree <worktree_id> --issue <issue_key> --command-id \"<unique-id>\" --json`.",
	"- After attachment, `issue show`, `issue next`, `board`, and `context resume` will surface the workspace and its local health.",
	"- Memori tracks and ranks workspace context, but it does not create, switch, or delete Git worktrees for you.",
	"- Keep one active attached worktree per issue. Detach or archive the old one before attaching a replacement.",
	"",
	"## Land The Plane",
	"Before closing a task, run this checklist in order:",
	"",
	"1. Confirm scope is complete for the active issue key.",
	"2. Run validation for touched code (tests/build/lint as applicable) and ensure results are green.",
	"3. If the work changed Memori behavior, workflows, or recommended usage, update `README.md` before closing so docs match current practice and state.",
	"4. Recheck issue context and history:",
	"   - `memori issue show --key <issue_key> --json`",
	"   - `memori event log --entity <issue_key> --json`",
	"5. Ensure task status reflects reality:",
	"   - Set `inprogress` at start of work.",
	"   - Set `blocked` immediately if blocked.",
	"6. Stage and commit with a clear message:",
	"   - `git add <files>`",
	"   - Use conventional commit syntax.",
	"   - Make the subject expressive enough to describe the behavior or workflow change, not just the file touched.",
	"   - Include the active ticket id in the commit message whenever the work is tracked by a memori issue.",
	"   - `git commit -m \"<type>(<scope>): <summary> (<issue_key>)\"`",
	"7. Push commit(s) to remote:",
	"   - `git push origin <branch>`",
	"8. Verify remote push succeeded and local branch is clean:",
	"   - `git status --short`",
	"   - `git log -1 --oneline`",
	"9. Decide whether the current cycle should close ungated or under an immutable close contract:",
	"   - Ungated close is the default path. If the work does not need an immutable contract for this cycle, close it directly after validation and push.",
	"   - If the cycle should close under an immutable contract, inspect available template versions when needed.",
	"   - `memori gate template list --json`",
	"   - Instantiate the close gate set for the issue type.",
	"   - `memori gate set instantiate --issue <issue_key> --command-id \"<unique-id>\" --json`",
	"   - If more than one eligible template applies, rerun with an explicit override:",
	"   - `memori gate set instantiate --issue <issue_key> --template <template@version> --command-id \"<unique-id>\" --json`",
	"   - Lock the gate set.",
	"   - `memori gate set lock --issue <issue_key> --command-id \"<unique-id>\" --json`",
	"   - Verify required gates.",
	"   - `memori gate verify --issue <issue_key> --gate <gate_id> --command-id \"<unique-id>\" --json`",
	"   - If you later decide a previously closed issue now needs an immutable close contract, reopen it first so the contract applies to a new cycle instead of retroactively changing the earlier close.",
	"10. Mark task `done` in memori only after push is successful and, when gated, after the close gates pass:",
	"   - `memori issue update --key <issue_key> --status done --command-id \"<unique-id>\" --json`",
	"11. Share closeout summary with:",
	"   - Issue key, commit SHA, push target branch, validation run, and any follow-up tasks.",
	"<!-- memori:land-the-plane:end -->",
	"",
}, "\n")

func appendInitAgentsLandThePlane(path string) (string, error) {
	existing, err := os.ReadFile(path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return "", fmt.Errorf("read %s: %w", path, err)
	}

	current := string(existing)
	if strings.Contains(current, agentsLandThePlaneStartTag) || strings.Contains(current, agentsLandThePlaneEndTag) {
		next, changed, err := replaceManagedAgentsSection(current)
		if err != nil {
			return "", err
		}
		if !changed {
			return "up_to_date", nil
		}
		if err := os.WriteFile(path, []byte(next), 0o644); err != nil {
			return "", fmt.Errorf("write %s: %w", path, err)
		}
		return "updated", nil
	}

	if agentsLandThePlaneAlreadyPresent(current) {
		return "up_to_date", nil
	}

	next := appendMarkdownSection(current, managedAgentsLandThePlaneSection)
	if err := os.WriteFile(path, []byte(next), 0o644); err != nil {
		return "", fmt.Errorf("write %s: %w", path, err)
	}
	if strings.TrimSpace(current) == "" {
		return "created", nil
	}
	return "updated", nil
}

func replaceManagedAgentsSection(current string) (string, bool, error) {
	start := strings.Index(current, agentsLandThePlaneStartTag)
	end := strings.Index(current, agentsLandThePlaneEndTag)
	if start == -1 || end == -1 {
		return "", false, fmt.Errorf("%s has an incomplete managed memori Land The Plane block", defaultAgentsMDPath)
	}
	end += len(agentsLandThePlaneEndTag)
	switch {
	case strings.HasPrefix(current[end:], "\r\n"):
		end += 2
	case strings.HasPrefix(current[end:], "\n"):
		end++
	}
	next := current[:start] + managedAgentsLandThePlaneSection + current[end:]
	if next == current {
		return current, false, nil
	}
	return next, true, nil
}

func agentsLandThePlaneAlreadyPresent(current string) bool {
	required := [][]string{
		{"## Non-Interactive Agent Setup"},
		{"`MEMORI_PRINCIPAL=llm`", "export MEMORI_PRINCIPAL=llm"},
		{"configured password", "human writer and mutating commands will prompt for the configured password"},
		{"## Worktree Continuity"},
		{"`memori worktree adopt-cwd --branch <branch>`", "`go run ./cmd/memori worktree adopt-cwd --branch <branch>`"},
		{"`memori worktree attach --worktree <worktree_id> --issue <issue_key> --command-id \"<unique-id>\" --json`", "`go run ./cmd/memori worktree attach --worktree <worktree_id> --issue <issue_key> --command-id \"<unique-id>\" --json`"},
		{"## Land The Plane"},
		{"`git push origin <branch>`"},
		{"`memori gate set instantiate --issue <issue_key> --command-id \"<unique-id>\" --json`"},
		{"`memori issue update --key <issue_key> --status done --command-id \"<unique-id>\" --json`"},
	}
	for _, options := range required {
		found := false
		for _, want := range options {
			if strings.Contains(current, want) {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func appendMarkdownSection(current, section string) string {
	trimmed := strings.TrimRight(current, "\n")
	if strings.TrimSpace(trimmed) == "" {
		return section
	}
	return trimmed + "\n\n" + section
}
