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
- If `memori` is not on `PATH`, use `go run ./cmd/memori` from the repo root for the same commands.

## Non-Interactive Agent Setup
When mutating state from an agent or automation context, export the LLM principal explicitly:

```bash
export MEMORI_PRINCIPAL=llm
export MEMORI_LLM_PROVIDER=openai
export MEMORI_LLM_MODEL=gpt-5
export MEMORI_ALLOW_MANUAL_COMMAND_ID=1
```

Use the same environment for `go run ./cmd/memori ...` if the binary is not installed locally.

## Required Agent Workflow
1. Confirm DB is initialized.
   - `memori init --issue-prefix <project-prefix> --json`
2. Check for existing tracked work before creating something new.
   - `memori issue next --agent <agent_id> --json`
   - `memori board --agent <agent_id> --json`
   - When resuming interrupted work, prefer `issue next` plus `context rehydrate` over relying on prior chat memory alone.
3. Create a ticket before doing implementation work only when no existing issue already covers the work.
   - `memori issue create --type task --title "<clear outcome>" --command-id "<unique-id>" --json`
4. Inspect ticket context before making changes.
   - `memori issue show --key <issue_key> --json`
   - `memori event log --entity <issue_key> --json`
5. Move ticket status as work progresses.
   - `memori issue update --key <issue_key> --status inprogress --command-id "<unique-id>" --json`
   - `memori issue update --key <issue_key> --status blocked --command-id "<unique-id>" --json`
   - `memori issue update --key <issue_key> --status done --command-id "<unique-id>" --json`
6. Keep work decomposed.
   - If scope grows, create child tasks/bugs and link with `--parent` on creation or `issue link`.
   - `memori issue link --child <child_key> --parent <parent_key> --command-id "<unique-id>" --json`
7. Keep user-facing guidance in sync with product behavior.
   - When Memori behavior, workflows, or recommended practices change, update `README.md` in the same unit of work so it reflects the latest supported state.
8. Rebuild projections from event ledger when validating consistency.
   - Run `memori db verify --json` after `memori db migrate --json` for schema or migration work.
   - `memori db replay --json`
   - Use replay when you need to recompute derived state such as gate projections, packets, focus, summaries, or open loops from the append-only ledger.
   - Use replay when validating continuity, packet, session, or other derived-projection changes against the append-only ledger.

## Command ID Convention
For mutating commands, always pass a stable `--command-id`.

memori can generate command IDs automatically, but in this repository agents should keep passing explicit stable IDs so retries and event-log review stay easy to correlate.

Recommended pattern:
- `<agent>-<yyyymmdd>-<short-purpose>-<nn>`
- Example: `codex-20260306-slice1-tests-01`

## Issue Key Convention
Use issue keys in `{prefix}-{shortSHA}` format.

- Example: `mem-a1b2c3d`
- Do not encode issue type into the key prefix (`epic|story|task|bug` are reserved for the `--type` field).
- Prefix is project-wide (set once via `memori init --issue-prefix ...`) and must be consistent for all new issues.

## During Bootstrap
- Gates, replay, and packet flows are real and should be used as the system of record, but the product remains CLI-first and local-only.
- Use `issue update` status transitions for day-to-day progress tracking.
- Prefer more small issues over untracked status notes.

## Land The Plane
Before closing a task, run this checklist in order:

1. Confirm scope is complete for the active issue key.
2. Run validation for touched code (tests/build/lint as applicable) and ensure results are green.
3. If the work changed Memori behavior, workflows, or recommended usage, update `README.md` before closing so docs match current practice and state.
4. Recheck issue context and history:
   - `memori issue show --key <issue_key> --json`
   - `memori event log --entity <issue_key> --json`
5. Ensure task status reflects reality:
   - Set `inprogress` at start of work.
   - Set `blocked` immediately if blocked.
6. Stage and commit with a clear message:
   - `git add <files>`
   - Use conventional commit syntax.
   - Make the subject expressive enough to describe the behavior or workflow change, not just the file touched.
   - Include the active ticket id in the commit message whenever the work is tracked by a memori issue.
   - `git commit -m "<type>(<scope>): <summary> (<issue_key>)"`
7. Push commit(s) to remote:
   - `git push origin <branch>`
8. Verify remote push succeeded and local branch is clean:
   - `git status --short`
   - `git log -1 --oneline`
9. Satisfy close gates for the current cycle before marking `done`:
   - Inspect available template versions when needed.
   - `memori gate template list --json`
   - Instantiate an approved close template for the issue type.
   - `memori gate set instantiate --issue <issue_key> --template <template@version> --command-id "<unique-id>" --json`
   - Lock the gate set.
   - `memori gate set lock --issue <issue_key> --command-id "<unique-id>" --json`
   - Verify required gates.
   - `memori gate verify --issue <issue_key> --gate <gate_id> --command-id "<unique-id>" --json`
10. Mark task `done` in memori only after push is successful and the close gates pass:
   - `memori issue update --key <issue_key> --status done --command-id "<unique-id>" --json`
11. Share closeout summary with:
   - Issue key, commit SHA, push target branch, validation run, and any follow-up tasks.

## Governance Notes
- Treat the event ledger as the authoritative write path; prefer commands that append events over manual database changes.
- Use `issue next --agent <agent_id>`, `context rehydrate`, and context packet commands when resuming interrupted work rather than relying on memory alone.
- Use `context summarize` to persist a structured handoff without ending a working window, and `context close` when the session should be treated as concluded before the next worker resumes it.
- After schema changes, favor `db migrate` followed by `db verify`, and use `db replay` when you need to prove derived projections still rebuild deterministically from the ledger.
- Agents may draft executable gate templates, but a human must approve the exact template version with `memori gate template approve` before it can be instantiated or used by `gate verify`.

## Priority Rule
If these instructions conflict with informal habits, follow this file: `memori` issue tracking is the default operating mode.
