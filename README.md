# memori

`memori` is a local-first issue tracker and context ledger for disciplined human-plus-agent workflows.

Today the repository provides a CLI-first event-sourced system: it stores work in a local SQLite database, records mutations as append-only events, rebuilds projections through replay, and adds explicit close gates, provenance, and context handoff primitives on top of issue tracking.

## What Exists Today

- Local issue ledger backed by SQLite at `.memori/memori.db` by default
- Issue create/show/update/link flows with rich context fields
- `backlog` and continuity-aware `issue next` commands for human and agent triage
- Append-only event log with replayable projections across issue, session, packet, focus, gate-template, and gate-set entities
- Versioned gate templates plus gate-set instantiate, lock, evaluate, verify, and status flows
- Executable gate templates can be agent-authored, then human-approved before they become runnable
- Close validation that blocks `done` until required gates pass and child issues are already closed
- Close-proof capture that binds gate-set hash and verifier proof into `Done` transitions
- Context checkpoint, packet build/show/use, rehydrate, and loop inspection commands
- DB status, migrate, verify, backup, and replay operations
- Provenance controls for human and LLM mutations, including governance for executable gate criteria

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
- The product shape is still early: this is a local CLI, not a finished hosted platform.
- Mutation provenance is intentionally strict.
- Context rehydration supports both packet reuse and a raw-events fallback path.
- Replay is now expected to rebuild derived continuity state, not just core issue rows.

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
- `issue next --agent <id>` can prefer active-focus and recovery-ready work instead of using status alone.

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
go run ./cmd/memori issue next --db /tmp/memori-eval.db --agent evaluator-1 --json
```

You can also inspect non-issue entities directly from the event log:

- `event log --entity packet:<packet_id>`
- `event log --entity focus:<agent_id>`
- `event log --entity gate-template:<template_id@version>`
- `event log --entity gate-set:<gate_set_id>`

To close that issue non-interactively, define a non-executable required gate, then evaluate it with evidence:

```bash
cat >/tmp/memori-eval-gates.json <<'JSON'
{"gates":[{"id":"review","kind":"check","required":true,"criteria":{"ref":"manual-review"}}]}
JSON

go run ./cmd/memori gate template create \
  --db /tmp/memori-eval.db \
  --id evaluator \
  --version 1 \
  --applies-to task \
  --file /tmp/memori-eval-gates.json \
  --command-id eval-readme-template-01 \
  --json

go run ./cmd/memori gate set instantiate \
  --db /tmp/memori-eval.db \
  --issue mem-a111111 \
  --template evaluator@1 \
  --command-id eval-readme-instantiate-01 \
  --json

go run ./cmd/memori gate set lock \
  --db /tmp/memori-eval.db \
  --issue mem-a111111 \
  --command-id eval-readme-lock-01 \
  --json

go run ./cmd/memori gate evaluate \
  --db /tmp/memori-eval.db \
  --issue mem-a111111 \
  --gate review \
  --result PASS \
  --evidence docs://readme-review \
  --command-id eval-readme-evaluate-01 \
  --json

go run ./cmd/memori issue update \
  --db /tmp/memori-eval.db \
  --key mem-a111111 \
  --status done \
  --command-id eval-readme-done-01 \
  --json
```

If you want to exercise `gate verify`, create the executable gate template, then approve that exact template version through a human-governed path after `auth set-password`. LLM principals can draft executable templates and later instantiate or verify approved ones, but they should not rely on executable templates before approval.

## Replay Vs Resume

Use `db replay` when you want to rebuild database truth from the event ledger.

- Projection tables look stale, inconsistent, or damaged.
- Projection logic changed and you want derived state recomputed from events.
- You want to validate that the ledger alone can reconstruct issue, gate, packet, focus, and loop state.

Use the resume-oriented commands when you want to continue execution efficiently from existing context.

- `issue next --agent <id>` selects the best continuity-aware work item for an agent.
- `context packet build` snapshots current issue or session state into a reusable recovery packet.
- `context packet use` restores agent focus to a packet and makes resume intent explicit.
- `context rehydrate` returns the best available recovery payload, using packets first and raw-events fallback when needed.

Short version:

- Replay rebuilds system state.
- Resume rebuilds worker momentum.

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
- `memori gate template approve`
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
- Executable templates are draftable by agents, but they are not runnable until a human approves that exact template version with `gate template approve`.
- Marking an issue `done` requires a locked gate set for the current cycle.
- Required locked gates must pass before `done` succeeds.
- `done` captures close proof from the current locked gate set, including verifier evidence and gate-set hash.
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
  Yes, in CLI form. Context checkpoints, packets, packet reuse, focus tracking, continuity-aware `issue next`, and rehydration are implemented, but the surrounding workflow is still maturing.

## Decision Checklist

- Try `memori` now if you want a local-first CLI with auditable history, explicit completion rules, and agent-aware context flows.
- Wait if you need collaboration features normally associated with a hosted tracker.
- Expect to read the CLI help and tests while evaluating; they are part of the current product surface.
