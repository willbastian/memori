# memori

**memori** is a local-first issue tracker and context bridge for human-and-agent workflows.

It combines three ideas in one CLI:

- an append-only event ledger for every mutation
- issue lifecycle management with explicit completion gates
- packet-based context handoff for resume and recovery

All state lives in a local SQLite database. The CLI is the product surface today: no server, no web UI, no remote control plane.

## Why memori exists

Traditional issue trackers are good at coordination, but they are not designed for agent continuity, deterministic replay, or proof-backed completion. memori is aimed at workflows where you want:

- local ownership of project state
- immutable history for issue and gate changes
- auditable human and LLM mutations
- enforced close criteria instead of informal “done” status changes
- explicit handoff and resume paths when an agent session loses context

## Core model

| Capability | What it does |
| --- | --- |
| Event ledger | Records issue, gate, session, packet, focus, and approval changes as append-only events. |
| Issue system | Supports `Epic`, `Story`, `Task`, and `Bug` with parent-child links, status updates, and backlog views. |
| Completion gates | Freezes gate definitions per issue cycle and blocks `done` until required gates are verified. |
| Context bridge | Captures checkpoints, builds reusable packets, tracks agent focus, and supports rehydration. |
| Replay | Rebuilds projections from the event ledger with `memori db replay`. |
| Provenance | Distinguishes human and LLM actors and applies stricter policy to executable gate criteria. |

## What the repository includes today

- SQLite-backed local database, defaulting to `.memori/memori.db`
- `issue create`, `issue update`, `issue show`, `issue link`, `backlog`, `board`, and `issue next`
- append-only event log with deterministic ordering and idempotent command handling
- gate template creation, approval, instantiation, locking, evaluation, verification, and status inspection
- close validation that requires a locked gate set, passing required gates, and closed child issues
- session checkpoints, packet build/show/use flows, open-loop tracking, and rehydration
- hierarchy-aware board snapshots plus an interactive TUI with parent/child navigation and `/` issue search
- replay, migration, verification, and backup database operations
- human password-based mutation auth and explicit LLM provenance for automation

## Current boundaries

memori is already useful for local workflow design and disciplined execution, but it is still intentionally narrow:

- single-user, local-only operation
- no hosted sync, multi-user coordination layer, or web UI
- no external integrations beyond the CLI and local filesystem
- source checkout and Go toolchain expected for evaluation

## Install and run

Prerequisites:

- Go 1.25+
- a local checkout of this repository

Run directly from source:

```bash
go run ./cmd/memori help
```

Or build a binary:

```bash
go build -o memori ./cmd/memori
./memori help
```

Examples below use `go run ./cmd/memori`. Replace that prefix with `memori` if you build the binary.

## Quick start

Initialize the database:

```bash
go run ./cmd/memori init --issue-prefix mem
```

New issues default to generated keys in `{prefix}-{shortSHA}` format when you omit `--key`. Mutation commands also generate command IDs automatically unless you explicitly opt into supplying your own with `MEMORI_ALLOW_MANUAL_COMMAND_ID=1`.

Inspect current state:

```bash
go run ./cmd/memori backlog
go run ./cmd/memori board --watch --interval 5s
go run ./cmd/memori db status
go run ./cmd/memori auth status
```

### Human setup

Human writes are authenticated interactively.

1. Leave `MEMORI_PRINCIPAL` unset, or set it to `human`.
2. Configure a password once:

```bash
go run ./cmd/memori auth set-password
```

After that, human write commands prompt for the configured password.

### Agent or automation setup

For non-interactive mutation flows, declare an LLM principal explicitly:

```bash
export MEMORI_PRINCIPAL=llm
export MEMORI_LLM_PROVIDER=openai
export MEMORI_LLM_MODEL=gpt-5
```

If your automation needs stable externally supplied command IDs for retries or cross-tool correlation, also export:

```bash
export MEMORI_ALLOW_MANUAL_COMMAND_ID=1
```

## End-to-end flows

### 1. Human-managed issue lifecycle

This is the core issue-to-done path: create an issue, freeze its completion contract, verify the required gate, then close it.

```bash
go run ./cmd/memori init --issue-prefix mem
go run ./cmd/memori auth set-password

go run ./cmd/memori issue create \
  --key mem-a111111 \
  --type task \
  --title "Ship polished public README"

cat >/tmp/memori-gates.json <<'JSON'
{"gates":[{"id":"build","kind":"check","required":true,"criteria":{"command":"go test ./..."}}]}
JSON

go run ./cmd/memori gate template create \
  --id release-checks \
  --version 1 \
  --applies-to task \
  --file /tmp/memori-gates.json

go run ./cmd/memori gate set instantiate \
  --issue mem-a111111 \
  --template release-checks@1

go run ./cmd/memori gate set lock --issue mem-a111111
go run ./cmd/memori gate verify --issue mem-a111111 --gate build
go run ./cmd/memori issue update --key mem-a111111 --status done
```

What happens in this flow:

- the issue is stored in the local ledger
- the gate set freezes the completion contract for the issue’s current cycle
- `gate verify` executes the approved verifier command and stores evidence plus proof metadata
- `issue update --status done` succeeds only if required gates pass and child issues are already closed

### 2. Agent execution against the same ledger

Agents use the same issue model, but mutations carry explicit LLM provenance. In practice, agents should rely on approved gate templates for executable checks.

The example below passes explicit command IDs for deterministic demonstration. In normal agent usage, memori can generate command IDs automatically.

```bash
export MEMORI_PRINCIPAL=llm
export MEMORI_LLM_PROVIDER=openai
export MEMORI_LLM_MODEL=gpt-5
export MEMORI_ALLOW_MANUAL_COMMAND_ID=1

go run ./cmd/memori init --db /tmp/memori-agent.db --issue-prefix mem

go run ./cmd/memori issue create \
  --db /tmp/memori-agent.db \
  --key mem-a222222 \
  --type task \
  --title "Validate recovery flow" \
  --description "Exercise packet-first resume behavior" \
  --command-id demo-agent-create-01 \
  --json

go run ./cmd/memori issue update \
  --db /tmp/memori-agent.db \
  --key mem-a222222 \
  --status inprogress \
  --priority P1 \
  --command-id demo-agent-progress-01 \
  --json

go run ./cmd/memori issue next \
  --db /tmp/memori-agent.db \
  --agent agent-demo-1 \
  --json
```

For executable gates, the expected flow is:

1. agents can create executable templates, but unapproved versions stay pending human review
2. humans can inspect the pending review queue with `gate template pending` and approve the exact template version with `gate template approve`, or create it as a human-governed actor
3. the agent instantiates and locks that approved gate set for the issue
4. the agent runs `gate verify` and then marks the issue `done`

### 3. Live board view for terminal splits

Use `board` when you want a continuously refreshing terminal snapshot instead of rerunning multiple inspection commands by hand. When stdout is attached to a terminal and `--watch` is not set, `memori board` opens the interactive TUI automatically.

```bash
go run ./cmd/memori board
go run ./cmd/memori board --watch --interval 5s
go run ./cmd/memori board --agent writer-1 --watch --interval 3s
go run ./cmd/memori board --agent writer-1 --json
```

In the interactive TUI:

- `j` / `k` move through issues
- `h` / `l` switch lanes
- `[` jumps to the parent issue and `]` jumps to the first child
- `{` collapses children and `}` expands them
- `/` opens issue-id search and `enter` jumps to the selected result
- `space` toggles the detail pane, `?` opens help, and `q` exits

The board surfaces:

- active work (`InProgress`)
- blocked work (`Blocked`)
- ready work (`Todo`) ranked from existing `issue next` guidance
- likely next work, including continuity signals such as focus, packets, open loops, and gate state
- hierarchy context such as parent, child, depth, and sibling metadata for each issue

For split panes, keep one shell running `board --watch` and do mutations in another. Use `--agent` when you want the likely-next panel to reflect a specific worker's current focus and recovery packet state.

### 4. Resume and handoff flow

memori’s context commands are built for “pick the work back up” scenarios.

```bash
go run ./cmd/memori context checkpoint --session sess-20260307-01 --trigger manual

go run ./cmd/memori context packet build \
  --scope issue \
  --id mem-a111111 \
  --json

go run ./cmd/memori context packet build \
  --scope session \
  --id sess-20260307-01 \
  --json

go run ./cmd/memori context packet show --packet <issue-packet-id> --json
go run ./cmd/memori context packet use --agent writer-1 --packet <issue-packet-id> --json
go run ./cmd/memori context rehydrate --session sess-20260307-01 --json
go run ./cmd/memori context loops --issue mem-a111111 --json
```

Use an issue-scoped packet when you want to set agent focus around a specific work item. Use a session-scoped packet when you want `context rehydrate` to return the latest saved session payload directly.

Issue packets currently include:

- `goal`
- `state`
- `gates`
- `open_loops`
- `next_actions`
- `risks`
- provenance metadata, including the event cursor used to build the packet

`issue next` uses these continuity signals as part of triage, so an agent can prefer work that already has focus, packets, unresolved loops, or failing gates that need attention.

### 5. Replay and integrity checks

When you want to rebuild derived state from the ledger, use replay:

```bash
go run ./cmd/memori db replay --json
```

When you want to confirm the database and event chain are internally consistent, use:

```bash
go run ./cmd/memori db verify --json
go run ./cmd/memori db status --json
go run ./cmd/memori db backup --out /tmp/memori-backup.db --json
```

Use replay when projections need to be recomputed. Use rehydrate when a worker needs to resume efficiently from the latest packet or fallback context.

## Current operator loop

For day-to-day work, the shortest path is usually:

1. `memori board` or `memori board --agent <id>` to see active, blocked, ready, and likely-next work.
2. `memori issue next --agent <id> --json` when an agent needs a ranked continuity-aware recommendation.
3. `memori issue show --key <issue>` and `memori event log --entity <issue> --json` before editing.
4. `memori gate template list --json` when you need to find a close template before locking gates for a cycle.
5. `memori gate template pending --json` when you need to review executable templates that are still awaiting human approval.

## Command map

### Inspection

- `memori help`
- `memori backlog`
- `memori board`
- `memori issue show`
- `memori gate status`
- `memori event log`
- `memori db status`
- `memori auth status`

### Issue and gate mutation

- `memori init`
- `memori auth set-password`
- `memori issue create`
- `memori issue update`
- `memori issue link`
- `memori gate template create`
- `memori gate template approve`
- `memori gate template list`
- `memori gate template pending`
- `memori gate set instantiate`
- `memori gate set lock`
- `memori gate evaluate`
- `memori gate verify`

### Agent continuity

- `memori issue next`
- `memori board`
- `memori context checkpoint`
- `memori context packet build`
- `memori context packet show`
- `memori context packet use`
- `memori context rehydrate`
- `memori context loops`

### Database operations

- `memori db migrate`
- `memori db verify`
- `memori db backup`
- `memori db replay`

## Event entities you can inspect directly

`event log` accepts plain issue keys and explicit entity prefixes.

Examples:

- `memori event log --entity mem-a111111`
- `memori event log --entity session:sess-20260307-01`
- `memori event log --entity packet:<packet-id>`
- `memori event log --entity focus:writer-1`
- `memori event log --entity gate-template:release-checks@1`
- `memori event log --entity gate-set:<gate-set-id>`

## Development

Run the full test suite:

```bash
go test ./...
```

The main implementation lives in:

- `cmd/memori` — CLI entrypoint
- `internal/cli` — command routing and output contracts
- `internal/store` — domain logic, event append, projections, and replay
- `internal/dbschema` — migrations and schema verification
- `internal/provenance` — principal resolution and password credential logic

## Project status

memori already demonstrates the core product shape: local issue tracking, immutable audit history, gate-backed completion, and packet-based continuity in one CLI.

It should be evaluated as an advanced local tool rather than a finished platform. If you want a public-cloud tracker, team collaboration features, or remote synchronization, this repository is not aiming there yet. If you want a rigorous local system for human-plus-agent execution, that is exactly what it is being built for.
