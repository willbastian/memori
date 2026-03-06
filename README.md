# memori (Slice 1 scaffold)

This repo now contains the first implementation slice for a local event-ledger-backed issue CLI.

## Implemented in this slice

- `memori init`
- Event store foundation (`events` append-only, hash chain columns, idempotency key)
- Minimal projection table (`work_items`)
- `memori issue create`
- `memori issue show`
- `memori event log`
- `memori db replay` (rebuild projections from the event ledger)

## Quick start

```bash
# initialize DB
memori init

# create an issue
memori issue create --type task --title "First ticket"

# show issue
memori issue show --id <issue-id>

# inspect event ledger for the issue
memori event log --entity <issue-id>

# rebuild projections from events
memori db replay
```

`--json` is supported on read/list commands in this slice and on `init`/`issue create` for structured automation.
