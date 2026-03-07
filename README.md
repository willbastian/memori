# memori

`memori` is a local-first issue tracker and context ledger for teams exploring disciplined human-plus-agent workflows.

Today this repository is a CLI-only Slice 1 implementation: it stores work in a local SQLite database, records mutations as append-only events, and adds explicit close gates, provenance, and context handoff primitives on top of ordinary issue tracking.

## What Exists Today

- Local issue ledger backed by SQLite at `.memori/memori.db` by default
- Issue create/show/update/link flows with rich context fields
- `backlog` and `issue next` commands for human and agent triage
- Append-only event log with replayable projections
- Versioned gate templates plus gate-set instantiate, lock, evaluate, verify, and status flows
- Close validation that blocks `done` until required gates pass and child issues are already closed
- Context checkpoint, packet build/show/use, rehydrate, and loop inspection commands
- DB status, migrate, verify, backup, and replay operations
- Provenance controls for human and LLM mutations

## Good Fit

- You want a local, auditable issue system instead of a hosted tracker.
- You care about append-only history, deterministic replay, and contract-style close checks.
- You want agent workflows to use the same issue system as humans.
- You are comfortable evaluating an early CLI from source.

## Not A Fit Yet

- You need a multi-user server, web UI, permissions model, or remote sync.
- You want lightweight task tracking with minimal ceremony.
- You need turnkey team onboarding without Go tooling or source checkout steps.
- You want mature integrations beyond the local CLI and database.

## Maturity Snapshot

- The command surface is substantial and the automated test suite is broad.
- The product shape is still early: this is a Slice 1 CLI, not a finished platform.
- Mutation provenance is intentionally strict.
- Context rehydration supports a raw-events fallback path, which is useful but also a sign the context bridge is still maturing.

## Quick Evaluation From A Source Checkout

Prerequisites:

- Go 1.25+
- A local clone of this repository

Examples below use `go run ./cmd/memori`. If you build your own binary, replace that prefix with `memori`.

Start with the human-oriented path:

```bash
go run ./cmd/memori help
go run ./cmd/memori init --issue-prefix mem
go run ./cmd/memori auth status
go run ./cmd/memori auth set-password
go run ./cmd/memori issue create --type task --title "Evaluate memori"
go run ./cmd/memori backlog
```

What to expect:

- `init` creates `.memori/memori.db` and records the issue key prefix.
- `auth set-password` is required before human-driven write commands succeed.
- `backlog` gives you the current work list; `issue show` and `event log` let you inspect detail and provenance.

## Non-Interactive Evaluation Flow

If you are validating the CLI in automation or from an agent environment, set an LLM principal explicitly:

```bash
export MEMORI_PRINCIPAL=llm
export MEMORI_LLM_PROVIDER=openai
export MEMORI_LLM_MODEL=gpt-5
export MEMORI_ALLOW_MANUAL_COMMAND_ID=1
```

Then you can exercise an end-to-end flow from a clean temp DB:

```bash
go run ./cmd/memori init --db /tmp/memori-eval.db --issue-prefix mem

go run ./cmd/memori issue create \
  --db /tmp/memori-eval.db \
  --key mem-a111111 \
  --type task \
  --title "Evaluate memori" \
  --description "Validate the local event-ledger workflow" \
  --acceptance-criteria "README examples match current behavior" \
  --command-id eval-readme-create-01 \
  --json

go run ./cmd/memori issue update \
  --db /tmp/memori-eval.db \
  --key mem-a111111 \
  --status inprogress \
  --priority P1 \
  --label evaluator \
  --command-id eval-readme-progress-01 \
  --json

go run ./cmd/memori backlog --db /tmp/memori-eval.db --json
go run ./cmd/memori issue show --db /tmp/memori-eval.db --key mem-a111111 --json
go run ./cmd/memori event log --db /tmp/memori-eval.db --entity mem-a111111 --json
```

To close that issue, define and lock a gate set, then verify the required gate:

```bash
cat >/tmp/memori-eval-gates.json <<'JSON'
{"gates":[{"id":"review","kind":"check","required":true,"criteria":{"command":"echo ok"}}]}
JSON

go run ./cmd/memori gate template create \
  --db /tmp/memori-eval.db \
  --id evaluator \
  --version 1 \
  --applies-to task \
  --file /tmp/memori-eval-gates.json \
  --json

go run ./cmd/memori gate set instantiate \
  --db /tmp/memori-eval.db \
  --issue mem-a111111 \
  --template evaluator@1 \
  --json

go run ./cmd/memori gate set lock \
  --db /tmp/memori-eval.db \
  --issue mem-a111111 \
  --json

go run ./cmd/memori gate verify \
  --db /tmp/memori-eval.db \
  --issue mem-a111111 \
  --gate review \
  --command-id eval-readme-verify-01 \
  --json

go run ./cmd/memori issue update \
  --db /tmp/memori-eval.db \
  --key mem-a111111 \
  --status done \
  --command-id eval-readme-done-01 \
  --json
```

## Command Map

Human workflows:

- `memori auth status`
- `memori backlog`
- `memori issue show`
- `memori gate status`
- `memori event log`
- `memori db status`

Agent and handoff workflows:

- `memori issue next`
- `memori context checkpoint`
- `memori context packet build`
- `memori context packet show`
- `memori context packet use`
- `memori context rehydrate`
- `memori context loops`

Create and update workflows:

- `memori init`
- `memori auth set-password`
- `memori issue create`
- `memori issue link`
- `memori issue update`
- `memori gate template create`
- `memori gate template list`
- `memori gate set instantiate`
- `memori gate set lock`
- `memori gate evaluate`
- `memori gate verify`
- `memori db migrate`
- `memori db verify`
- `memori db backup`
- `memori db replay`

## Operational Caveats

- Human mutations fail closed until a password is configured and verified interactively.
- LLM mutations require `MEMORI_PRINCIPAL=llm` plus provider and model metadata.
- Manual `--command-id` is reserved for automation and replay workflows and requires `MEMORI_ALLOW_MANUAL_COMMAND_ID=1`.
- Marking an issue `done` requires a locked gate set for the current cycle.
- Required locked gates must pass before `done` succeeds.
- Parent issues cannot close while any child issue is still open.
- The default mode is local-only; operational durability comes from the SQLite DB and event ledger, not from a remote service.

## FAQ

- Is this production-ready issue infrastructure?
  Not yet. The core contracts are real and well tested, but the repo is still at an early slice and best suited to evaluation, prototyping, and workflow design.

- What is the strongest reason to try it?
  If you want issue state, provenance, and close validation to live in one local system that both humans and agents can use, this repo already demonstrates that shape.

- What is the biggest adoption risk today?
  Operational friction. Human writes are intentionally strict, and there is no hosted control plane, sync layer, or GUI to absorb that complexity.

- Do I need gates to close work?
  Yes. Closing to `done` is contract-driven and requires a locked gate set for the current cycle, plus passing required gates.

- Does it support agent handoff today?
  Yes, in CLI form. Context checkpoints, packets, packet reuse, and rehydration are implemented, but the surrounding workflow is still maturing.

## Decision Checklist

- Try `memori` now if you want a local-first CLI with auditable history, explicit completion rules, and agent-aware context flows.
- Wait if you need collaboration features normally associated with a hosted tracker.
- Expect to read the CLI help and tests while evaluating; they are part of the current product surface.
