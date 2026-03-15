<p align="center">
  <img src="assets/title-card.png" alt="memori title image" width="1200">
</p>

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

## System architecture

```mermaid
flowchart TD
    Human["Human operator"]
    Agent["Agent or automation"]
    CLI["memori CLI"]
    Auth["Auth and provenance layer"]
    Store["Store and command handlers"]
    Ledger[("Append-only event ledger")]
    Proj["Derived projections"]
    SQLite[("SQLite database")]
    Gates["Issue, gate, packet, focus, and session state"]
    Replay["db replay / db verify / db migrate"]
    Release["Release workflow and artifacts"]
    Install["Source install or release installer"]

    Human --> CLI
    Agent --> CLI
    CLI --> Auth
    Auth --> Store
    Store --> Ledger
    Store --> Proj
    Ledger --> Proj
    Ledger --> SQLite
    Proj --> SQLite
    Proj --> Gates
    Replay --> Ledger
    Replay --> Proj
    Release --> Install
    Install --> CLI
```

The CLI is the only product surface today. It writes append-only events, rebuilds derived state from that ledger, and exposes the resulting issue, gate, packet, focus, and session views from the local SQLite store.

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

## Adoption policy

License:

- memori is available under the MIT license. See [LICENSE](LICENSE).

Intended audience:

- teams experimenting with local-first human-and-agent workflows
- repositories that want issue tracking, replay, continuity, and gate-backed completion in one CLI

Stability expectations:

- the CLI is usable today, but the product is still early and evolving
- database migrations are supported forward through `memori db migrate`, but older binaries may not understand newer schema versions
- automation should prefer JSON output and explicit command IDs where reproducibility matters

Support expectations:

- this repository is maintained as an actively developed project, not a hosted service with uptime or compatibility guarantees
- adopters should review release notes, keep database backups, and validate upgrades with `memori db status` and `memori db verify`

## Install and run

Prerequisites:

- Go 1.25+
- access to `github.com/willbastian/memori`

Public module path:

```text
github.com/willbastian/memori
```

Install the CLI from source without cloning the repository:

```bash
go install github.com/willbastian/memori/cmd/memori@latest
memori version
memori help
```

If you want the current branch directly instead of the latest resolved module version, use:

```bash
go install github.com/willbastian/memori/cmd/memori@main
```

Install from published release artifacts without building from source:

```bash
curl -fsSL https://raw.githubusercontent.com/willbastian/memori/main/scripts/install_release.sh | bash
memori version
```

To pin a specific release:

```bash
curl -fsSL https://raw.githubusercontent.com/willbastian/memori/main/scripts/install_release.sh | bash -s -- --version v0.1.0
```

The supported installer channel in this repository is [install_release.sh](scripts/install_release.sh). It downloads the matching archive from the project's GitHub releases and installs `memori` into `~/.local/bin` by default. Maintenance for that installer flow lives in this repository alongside the release workflow and docs.

Or run from a local checkout:

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

## Release artifacts

Tagged releases build cross-platform archives for:

- macOS `amd64`
- macOS `arm64`
- Linux `amd64`
- Linux `arm64`

The automation lives in [.github/workflows/release.yml](.github/workflows/release.yml) and uses [scripts/build_release_artifacts.sh](scripts/build_release_artifacts.sh) so the same build flow can run locally or in GitHub Actions.

The test-and-coverage CI lives in [.github/workflows/ci.yml](.github/workflows/ci.yml) and uses [scripts/check_coverage_baseline.sh](scripts/check_coverage_baseline.sh). It runs on every branch push plus pull requests targeting `main`, executes `go test ./...`, and fails if total Go statement coverage drops materially below the committed baseline. The script includes a small default `0.25` percentage-point tolerance so harmless cross-platform coverage drift does not fail CI.

To cut a release from a tag:

```bash
git tag v0.1.0
git push origin v0.1.0
```

That workflow builds `tar.gz` archives plus `SHA256SUMS.txt` and attaches them to the matching GitHub release. You can also run the same archive build locally:

```bash
./scripts/build_release_artifacts.sh v0.1.0 dist
```

Release binaries embed:

- the CLI version string
- the source commit
- the build timestamp
- the binary's embedded schema head version

You can inspect that metadata with:

```bash
memori version
memori version --json
```

Schema compatibility expectations:

- a binary can inspect and migrate an older memori database up to its embedded schema head version
- if a database has already been migrated beyond the binary's reported `schema_head_version`, use a newer memori binary before making changes
- use `memori db status` to compare the current database version with the binary's expected schema head

## Adopt memori in a new repository

These steps assume you installed the `memori` binary. If you are working from a local checkout instead, replace `memori` with `go run ./cmd/memori`.

## Human and agent workflow

```mermaid
flowchart LR
    Pick["Pick work<br/>issue next / board / issue show"]
    Start["Start or update issue<br/>issue create / issue update --status inprogress"]
    Work["Do the work<br/>human edits or agent execution"]
    Context["Capture continuity<br/>context checkpoint / summarize / close / rehydrate"]
    Template{"Approved gate template<br/>available for issue type?"}
    Review["Human reviews or approves<br/>gate template pending / approve"]
    Freeze["Freeze completion contract<br/>gate set instantiate + lock"]
    Verify["Prove required gates<br/>gate verify"]
    Close["Close the issue<br/>issue update --status done"]
    Resume["Resume later from packets,<br/>sessions, and board context"]

    Pick --> Start
    Start --> Work
    Work --> Context
    Context --> Template
    Template -- no --> Review
    Review --> Freeze
    Template -- yes --> Freeze
    Freeze --> Verify
    Verify --> Close
    Context --> Resume
    Resume --> Pick
```

Humans and agents operate against the same ledger. The usual loop is: pick tracked work, move it into progress, do the implementation, capture continuity as you go, freeze the close contract with a gate set, verify the required proof, and only then mark the issue `done`.

Issue status transitions now bundle continuity into the normal work loop. `issue update --status inprogress` automatically starts or continues a session, refreshes the issue packet, and, when you pass `--agent`, updates that agent's saved focus as part of the same start-work step. `issue update --status blocked` automatically summarizes the active session and saves a fresh session packet, while `issue update --status done` does the same and closes the session as part of the handoff trail.

You can tune that behavior with continuity automation modes:
- `auto`: the default. Start, pause, and close issue transitions bundle the continuity writes directly into the command.
- `assist`: keep continuity explicit, but have `issue update` print the exact `context start`, `context save`, or `context save --close` command that matches the transition you just made.
- `manual`: disable automatic continuity for the command and skip the extra assist bundle guidance.

Choose a mode per command with `--continuity manual|assist|auto`, or set `MEMORI_CONTINUITY_MODE` to make it the session default. In practice, `auto` tends to fit agent-driven loops well, while humans who want tighter step-by-step control may prefer `assist` or `manual`.

### 1. Initialize project state

From the root of the repository you want to track:

```bash
memori init --issue-prefix acme
memori db status
memori backlog
```

The default database path is `.memori/memori.db` inside that repository. Issue keys will use the prefix you choose, for example `acme-a1b2c3d`.

### 2. Set up human writes

Human writes are authenticated interactively.

1. Leave `MEMORI_PRINCIPAL` unset, or set it to `human`.
2. Configure a password once:

```bash
memori auth set-password
memori auth status
```

After that, human write commands prompt for the configured password.

### 3. Set up agent or automation writes

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

### 4. Use it day to day

For humans:

```bash
memori board --watch --interval 5s
memori issue show --key acme-a1b2c3d --json
memori gate status --issue acme-a1b2c3d
```

For agents:

```bash
memori issue next --agent writer-1 --json
memori context checkpoint --json
memori context resume --agent writer-1 --json
memori context rehydrate --json
memori context summarize --json
```

New issues default to generated keys in `{prefix}-{shortSHA}` format when you omit `--key`. Mutation commands also generate command IDs automatically unless you explicitly opt into supplying your own with `MEMORI_ALLOW_MANUAL_COMMAND_ID=1`.

### 5. Upgrade safely

When you upgrade the binary in an existing repository:

```bash
memori version
memori db status
memori db backup --out /tmp/memori-pre-upgrade.db --json
memori db migrate --json
memori db verify --json
```

That keeps the binary version, schema version, and migration audit aligned before you resume normal work.

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
  --issue mem-a111111

go run ./cmd/memori gate set lock --issue mem-a111111
go run ./cmd/memori gate verify --issue mem-a111111 --gate build
go run ./cmd/memori issue update --key mem-a111111 --status done
```

What happens in this flow:

- the issue is stored in the local ledger
- `gate set instantiate` auto-selects the single eligible template for the issue type when you omit `--template`; if more than one template family fits, the CLI tells you to choose explicitly
- the gate set freezes the completion contract for the issue’s current cycle
- `gate verify` executes the approved verifier command and stores evidence plus proof metadata
- `issue update --status done` succeeds only if required gates pass and child issues are already closed

Issue metadata can still be revised later with append-only updates, including title changes such as:

```bash
go run ./cmd/memori issue update --key mem-a111111 --title "Ship public README and onboarding flow"
```

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
  --agent agent-demo-1 \
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

Template review states fit together like this:

- draft: the proposed gate definition still only exists in a file or command payload and has not been written to the ledger yet
- pending: an executable template was created by an LLM-governed actor, so it exists in the ledger but cannot be instantiated or verified until a human approves that exact version
- approved: a human approved the executable template version, or the executable template was created directly by a human-governed actor
- runnable: the approved template can be instantiated into a gate set for an issue cycle and then used by `gate verify`

A human reviewer can work the queue with:

```bash
go run ./cmd/memori gate template pending --json
go run ./cmd/memori gate template approve --id <template-id> --version <n> --json
go run ./cmd/memori gate template pending --json
```

After approval, that template version disappears from the pending queue and becomes runnable for gate-set instantiation.

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
- `f` toggles between the fast actionable view and an all-work history view that includes `Done` and `WontDo`
- `[` jumps to the parent issue and `]` jumps to the first child
- `{` collapses children and `}` expands them
- `/` opens issue-id search and `enter` jumps to the selected result
- `space` toggles the detail pane, `?` opens help, and `q` exits

The board surfaces:

- active work (`InProgress`)
- blocked work (`Blocked`)
- ready work (`Todo`) ranked from existing `issue next` guidance
- full parent/child hierarchy context in the `Active` and `Ready` lanes so related adjacent work stays visible even when siblings or parents are in a different status
- likely next work, including continuity signals such as focus, packets, open loops, and gate state
- completed and declined work through the all-work history view (`Done` and `WontDo`)
- hierarchy context such as parent, child, depth, and sibling metadata for each issue
- ambient continuity pressure for active, blocked, or resume-rich work when packets are stale, missing, or already helping

For split panes, keep one shell running `board --watch` and do mutations in another. When stdout is attached to a real terminal, watch mode redraws in place instead of appending endless snapshots. Use `--agent` when you want the likely-next panel to reflect a specific worker's current focus and recovery packet state.

### 4. Resume and handoff flow

memori’s context commands are built for “pick the work back up” scenarios.

```bash
go run ./cmd/memori context start --issue mem-a111111 --agent writer-1 --json

go run ./cmd/memori context save \
  --note "paused after reproducing the gate failure" \
  --close \
  --reason "handoff captured for the next worker" \
  --json

go run ./cmd/memori context checkpoint --trigger manual

go run ./cmd/memori context summarize \
  --note "paused after reproducing the gate failure" \
  --json

go run ./cmd/memori context close \
  --reason "handoff captured for the next worker" \
  --json

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
go run ./cmd/memori context resume --agent writer-1 --json
go run ./cmd/memori context rehydrate --json
go run ./cmd/memori context loops --issue mem-a111111 --json
```

Use `context start` when you want the happy-path “begin work” flow to checkpoint a session, build an issue packet, and optionally update an agent’s focus in one command.
Use `context save` when you want the happy-path “pause or hand off” flow to summarize the session, build a fresh session packet, and optionally close the session in one command.
Use `context resume` when you want the happy-path “pick work back up” flow to rehydrate the latest session payload and, with `--agent`, refresh saved focus in the same command.
When you omit `--session`, memori keeps the continuity flow ergonomic:
- `context checkpoint` continues the latest open session when one exists, or creates a fresh deterministic session id when one does not.
- `context summarize` and `context close` target the latest open session.
- `context resume` uses the same session-selection rules as `context rehydrate`, but names the action the way operators think about it.
- `context rehydrate` prefers the latest open session and falls back to the latest session when everything is already closed.
- Human output always tells you which session was used so you can copy it into explicit commands when you want tighter control.

Use an issue-scoped packet when you want to set agent focus around a specific work item. Use a session-scoped packet when you want `context rehydrate` to return the latest saved session payload directly.
Use `context summarize` to persist a structured session summary without ending the working window, and `context close` to mark that working window as finished with `ended_at` and the current `summary_event_id`.
When no saved active-session packet exists yet, `context rehydrate` falls back to recent session context chunks before using a raw event-only payload.
For closed sessions, `context rehydrate` prefers a closure-aware packet; otherwise it returns a synthesized closed-session summary so an older active packet cannot masquerade as the latest state.
Packet lookups keep the same JSON payload contract, but routing now uses normalized packet columns so replay and mixed historical data stay query-safe.

Issue packets currently include:

- `goal`
- `state`
- `decision_summary`
- `open_questions`
- `linked_work_items`
- `gates`
- `open_loops`
- `next_actions`
- `risks`
- `continuity` metadata, including compaction policy and recent relevant chunks
- provenance metadata, including the event cursor used to build the packet

`issue next` uses these continuity signals as part of triage, so an agent can prefer work that already has focus, packets, unresolved loops, or failing gates that need attention.

### 5. Replay and integrity checks

When you want to rebuild derived state from the ledger, use replay:

```bash
go run ./cmd/memori db replay --json
```

When you want to confirm the database and event chain are internally consistent, use:

```bash
go run ./cmd/memori db migrate --json
go run ./cmd/memori db verify --json
go run ./cmd/memori db status --json
go run ./cmd/memori db backup --out /tmp/memori-backup.db --json
```

`db migrate` now creates a restore-point backup before applying schema changes and returns the backup path in JSON output.
`db verify` checks both the event hash chain and the `schema_migrations` audit ledger so checksum drift or missing migration audit rows are reported alongside schema-version issues.

Use replay when projections need to be recomputed. Use rehydrate when a worker needs to resume efficiently from the latest packet or fallback context.

## Current operator loop

For day-to-day work, the shortest path is usually:

1. `memori board` or `memori board --agent <id>` to see active, blocked, ready, and likely-next work.
2. `memori issue next --agent <id> --json` when an agent needs a ranked continuity-aware recommendation.
3. `memori issue update --key <issue> --status inprogress --agent <id>` to start work and continuity in one step.
4. `memori issue show --key <issue>` and `memori event log --entity <issue> --json` before editing.
5. `memori issue update --key <issue> --status blocked --note "<handoff>"` or `--status done --note "<summary>" --reason "<close reason>"` to save continuity at pause and close.
6. `memori context resume --agent <id>` when returning to paused work from the latest saved session packet.
7. `memori version --json` when you need the binary build metadata and embedded schema head version.
8. `memori gate template list --json` when you need to find a close template before locking gates for a cycle.
9. `memori gate template pending --json` when you need to review executable templates that are still awaiting human approval.

## Command map

### Inspection

- `memori help`
- `memori version`
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
  - omit `--template` to auto-select the single eligible template for the issue type
  - pass `--template <template@version>` to override or resolve ambiguity explicitly
- `memori gate set lock`
- `memori gate evaluate`
- `memori gate verify`

### Agent continuity

- `memori issue next`
- `memori board`
- `memori context start`
- `memori context save`
- `memori context checkpoint`
- `memori context summarize`
- `memori context close`
- `memori context resume`
- `memori context packet build`
- `memori context packet show`
- `memori context packet use`
- `memori context rehydrate`
- `memori context loops`

Human-readable `issue create`, `issue update`, `issue show`, and `issue next` now surface continuity guidance when the current work state makes it relevant. In practice that means `todo`, `inprogress`, and `blocked` work will point you toward `context checkpoint`, `context summarize`, `context packet build`, or `context loops` instead of treating continuity as a separate subsystem you have to remember on your own.
When you move an issue into progress, `issue update --status inprogress` also starts or continues continuity automatically, and `--agent <id>` folds the focus update into that same command so start-work and resume context stay aligned. When you pause or finish work, `issue update --status blocked|done` saves continuity automatically from the latest open session; use `--note` to capture handoff detail, `--reason` to explain a done-path close, and `--skip-continuity` only when you need to bypass the default save behavior deliberately.
Human-readable `issue show`, `issue next`, and `board` also surface continuity state at the point where work starts or resumes, including whether a saved issue packet is fresh or stale, whether an open session already exists, and whether an agent already has saved focus on the work. They now add a `Continuity Pressure` section when handoff state is missing, stale, or already strong enough to make resume lightweight, so freshness and recovery risk stay visible in the same place you pick or inspect work. `issue show` and continuity-rich `issue next` flows now point to `memori context resume` explicitly so resume looks like one obvious action instead of packet and rehydrate choreography.

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

JSON event logs include `causation_id` and `correlation_id` so related commands can be traced across retries and multi-step workflows.

## Development

Run the full test suite:

```bash
go test ./...
```

Run the same full-suite coverage check that GitHub Actions enforces:

```bash
./scripts/check_coverage_baseline.sh
```

The coverage check compares against the committed baseline in `.github/coverage-baseline.txt` with a default `0.25` percentage-point tolerance. Override that buffer with `MEMORI_COVERAGE_TOLERANCE=<value>` if you need a stricter or looser local check.

Before starting a structural refactor of a large file, run a baseline regression pass first:

```bash
go test ./... -coverprofile=/tmp/memori-cover.out
go tool cover -func=/tmp/memori-cover.out | rg 'internal/(store/store.go|cli/cli.go|cli/board_tui.go|dbschema/dbschema.go)'
go test ./internal/store ./internal/cli ./internal/dbschema
```

Use that sequence to confirm the current baseline, then add or tighten behavior-level tests before moving code across files when a seam looks weakly protected.

The main implementation lives in:

- `cmd/memori` — CLI entrypoint
- `internal/cli` — command routing and output contracts
- `internal/store` — domain logic, event append, projections, and replay
- `internal/dbschema` — migrations and schema verification
- `internal/provenance` — principal resolution and password credential logic

## Project status

memori already demonstrates the core product shape: local issue tracking, immutable audit history, gate-backed completion, and packet-based continuity in one CLI.

It should be evaluated as an advanced local tool rather than a finished platform. If you want a public-cloud tracker, team collaboration features, or remote synchronization, this repository is not aiming there yet. If you want a rigorous local system for human-plus-agent execution, that is exactly what it is being built for.
