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
   - `memori init --json`
2. Create a ticket before doing implementation work.
   - `memori issue create --type task --title "<clear outcome>" --command-id "<unique-id>" --json`
3. Inspect ticket context before making changes.
   - `memori issue show --id <issue_id> --json`
   - `memori event log --entity <issue_id> --json`
4. Keep work decomposed.
   - If scope grows, create child tasks/bugs and link with `--parent` on creation.
5. Rebuild projections from event ledger when validating consistency.
   - `memori db replay --json`

## Command ID Convention
For mutating commands, always pass a stable `--command-id`.

Recommended pattern:
- `<agent>-<yyyymmdd>-<short-purpose>-<nn>`
- Example: `codex-20260306-slice1-tests-01`

## During Bootstrap
The CLI currently has early Slice 1 commands only. Until richer issue update flows exist:

- Represent progress as additional linked tasks/bugs created in `memori`.
- Prefer more small issues over untracked status notes.

## Priority Rule
If these instructions conflict with informal habits, follow this file: `memori` issue tracking is the default operating mode.
