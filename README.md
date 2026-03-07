# memori (Slice 1 scaffold)

This repo now contains the first implementation slice for a local event-ledger-backed issue CLI.

## Implemented in this slice

- `memori init`
- `memori auth status`
- `memori auth set-password`
- Event store foundation (`events` append-only, hash chain columns, idempotency key)
- Immutable gate schema foundation (`gate_templates`, `gate_sets`, `gate_set_items` + immutability triggers)
- Close-validator contract on `issue update --status done` (checks required locked gates from source-of-truth data)
- Minimal projection table (`work_items`)
- `memori issue create`
- `memori issue link`
- `memori issue update`
- `memori issue show`
- `memori issue next`
- `memori gate template create`
- `memori gate template list`
- `memori gate set instantiate`
- `memori gate set lock`
- `memori gate evaluate`
- `memori gate verify`
- `memori gate status`
- `memori context checkpoint`
- `memori context rehydrate`
- `memori context packet build|show|use`
- `memori context loops`
- `memori event log`
- `memori db status`
- `memori db migrate`
- `memori db verify`
- `memori db backup`
- `memori db replay` (rebuild projections from the event ledger)

## Quick start

```bash
# initialize DB
memori init --issue-prefix mem

# inspect migration status
memori db status

# apply pending migrations (uses goose)
memori db migrate

# verify version alignment
memori db verify

# write restorable snapshot
memori db backup --out /tmp/memori-backup.db

# configure human mutation auth once per DB
memori auth set-password

# create an issue (command_id auto-generated)
memori issue create --type task --title "First ticket"

# create with richer context
memori issue create \
  --type task \
  --title "Improve issue context" \
  --description "Add richer fields for agent/human handoff" \
  --acceptance-criteria "Issue show returns context-rich payload" \
  --reference "https://example.com/spec" \
  --json

# show issue
memori issue show --key <issue-key>

# move issue to in-progress
memori issue update --key <issue-key> --status inprogress

# update context fields without changing status
memori issue update \
  --key <issue-key> \
  --priority P1 \
  --label backend \
  --label ux \
  --description "Updated context" \
  --acceptance-criteria "Readable in issue show output" \
  --reference "notes.md" \
  --json

# re-link child to parent
memori issue link --child <child-key> --parent <parent-key>

# ask memori for the next actionable issue
memori issue next --agent codex-1

# inspect event ledger for the issue
memori event log --entity <issue-key>

# evaluate a gate
memori gate evaluate \
  --issue <issue-key> \
  --gate build \
  --result PASS \
  --evidence "ci://run/123"

# verifier-executed gate evaluation (recommended)
memori gate verify \
  --issue <issue-key> \
  --gate build

# inspect gate status for current locked gate set (or a specific cycle)
memori gate status --issue <issue-key> [--cycle <n>]

# close validation requires:
# - a locked gate set for the issue's current cycle
# - PASS for every required gate
# - proof-bearing evaluations from verifier execution (runner + exit_code + gate_set_hash)

# create template definition file and register versioned gate template
cat > /tmp/quality-gates.json <<'JSON'
{"gates":[{"id":"build","kind":"check","required":true,"criteria":{"command":"go test ./..."}},{"id":"lint","kind":"check","required":false}]}
JSON
memori gate template create \
  --id quality \
  --version 1 \
  --applies-to task \
  --file /tmp/quality-gates.json

# instantiate and lock a gate set for the issue's current cycle
memori gate set instantiate --issue <issue-key> --template quality@1
memori gate set lock --issue <issue-key>

# list templates (optionally filtered by issue type)
memori gate template list --type task

# checkpoint and rehydrate context
memori context checkpoint --session sess-1 --trigger manual
memori context packet build --scope issue --id <issue-key>
memori context loops --issue <issue-key>
memori context rehydrate --session sess-1

# rebuild projections from events
memori db replay
```

`--json` is supported on read/list commands in this slice and on write commands that return structured envelopes.

## Provenance controls

- Human principals must configure a password with `memori auth set-password` before mutating commands will write.
- Human mutation verification is interactive and intentionally fails closed on non-terminal stdin.
- Mutation actors are derived from trusted runtime principal context, not `--actor`.
- Human actors use `human:<system-username>`.
- LLM actors use `llm:<provider>:<model>` and require `MEMORI_PRINCIPAL=llm`, `MEMORI_LLM_PROVIDER`, and `MEMORI_LLM_MODEL`.
- Event-backed mutating commands generate `command_id` automatically using `cmdv1-<operation>-<utcstamp>-<entropy>`.
- Manual `--command-id` is reserved for automation/replay workflows and requires `MEMORI_ALLOW_MANUAL_COMMAND_ID=1`.
- Existing event rows remain valid; the new controls apply to future writes only.

Human-readable output is grouped around likely workflows:

- `memori help` separates human workflows, agent workflows, and create/update commands.
- Happy-path text output highlights the action that succeeded and includes suggested next commands when available.
- `MEMORI_COLOR=auto|always|never` controls ANSI color in human-readable output.
- `NO_COLOR`, `CLICOLOR=0`, `CLICOLOR_FORCE=1`, and `FORCE_COLOR=1` are also honored.

Operational notes:

- Password material is stored hashed at rest in `human_auth_credentials` using PBKDF2-HMAC-SHA256 with a per-credential salt and enforced minimum iteration count.
- Rotating the password updates the single `default` credential row and preserves existing event history.
- If human auth is not yet configured, mutation commands return an actionable setup error that points to `memori auth set-password`.

Issue key format:

- `{prefix}-{shortSHA}` (example: `wrk-a1b2c3d`)
- Issue type is stored in `--type`; do not encode type names (`epic|story|task|bug`) into the key prefix.
- Prefix is project-wide and set at init (`--issue-prefix`); all new issue keys must use that prefix.
