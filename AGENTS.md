# Agent Operating Instructions

## Issue Tracking System of Record
Use `memori` for issue tracking in this repository.

- Do not track active work in ad hoc markdown checklists, scratch files, or commit messages alone.
- Every meaningful unit of work should exist as a `memori` issue (`epic`, `story`, `task`, or `bug`).
- Treat the local `memori` database as the canonical state for in-flight work.

## Default Setup
- Default DB path: `.memori/memori.db`
- You may override with `MEMORI_DB_PATH` when needed.
- Run `memori init` before first use in a fresh clone/worktree.

## Required Agent Workflow
1. Confirm DB is initialized.
   - `memori init --issue-prefix <project-prefix> --json`
2. Create a ticket before doing implementation work.
   - `memori issue create --type task --title "<clear outcome>" --command-id "<unique-id>" --json`
3. Inspect ticket context before making changes.
   - `memori issue show --key <issue_key> --json`
   - `memori event log --entity <issue_key> --json`
4. Move ticket status as work progresses.
   - `memori issue update --key <issue_key> --status inprogress --command-id "<unique-id>" --json`
   - `memori issue update --key <issue_key> --status blocked --command-id "<unique-id>" --json`
   - `memori issue update --key <issue_key> --status done --command-id "<unique-id>" --json`
5. Keep work decomposed.
   - If scope grows, create child tasks/bugs and link with `--parent` on creation.
6. Rebuild projections from event ledger when validating consistency.
   - `memori db replay --json`

## Command ID Convention
For mutating commands, always pass a stable `--command-id`.

Recommended pattern:
- `<agent>-<yyyymmdd>-<short-purpose>-<nn>`
- Example: `codex-20260306-slice1-tests-01`

## Issue Key Convention
Use issue keys in `{prefix}-{shortSHA}` format.

- Example: `mem-a1b2c3d`
- Do not encode issue type into the key prefix (`epic|story|task|bug` are reserved for the `--type` field).
- Prefix is project-wide (set once via `memori init --issue-prefix ...`) and must be consistent for all new issues.

## During Bootstrap
- `issue link`, gates, and packet flows are still in progress.
- Use `issue update` status transitions for day-to-day progress tracking.
- Prefer more small issues over untracked status notes.

## Land The Plane
Before closing a task, run this checklist in order:

1. Confirm scope is complete for the active issue key.
2. Run validation for touched code (tests/build/lint as applicable) and ensure results are green.
3. Recheck issue context and history:
   - `memori issue show --key <issue_key> --json`
   - `memori event log --entity <issue_key> --json`
4. Ensure task status reflects reality:
   - Set `inprogress` at start of work.
   - Set `blocked` immediately if blocked.
5. Stage and commit with a clear message:
   - `git add <files>`
   - `git commit -m "<message>"`
6. Push commit(s) to remote:
   - `git push origin <branch>`
7. Verify remote push succeeded and local branch is clean:
   - `git status --short`
   - `git log -1 --oneline`
8. Mark task `done` in memori only after push is successful:
   - `memori issue update --key <issue_key> --status done --command-id "<unique-id>" --json`
9. Share closeout summary with:
   - Issue key, commit SHA, push target branch, validation run, and any follow-up tasks.

## Priority Rule
If these instructions conflict with informal habits, follow this file: `memori` issue tracking is the default operating mode.
