---
title: 'dbx Observability and Hooks'
linkTitle: 'Observability'
description: 'Hooks, HookEvent, Duration, Metadata for logging and tracing'
weight: 9
---

## Observability and Hooks

Hooks run before and after each DB operation. Use them for logging, metrics, tracing, and slow-query detection.

## HookEvent

`HookEvent` carries operation details:

| Field | Description |
|-------|-------------|
| `Operation` | query, exec, query_row, begin_tx, commit_tx, rollback_tx, auto_migrate, validate_schema |
| `Statement` | High-level statement name (if any) |
| `SQL` | Actual SQL string |
| `Args` | Bound arguments |
| `Table` | Target table (if known) |
| `StartedAt` | Timestamp when the operation started |
| `Duration` | Elapsed time (set in After) |
| `RowsAffected` | For exec operations |
| `Err` | Error (if any) |
| `Metadata` | Arbitrary key-value pairs for trace_id, request_id, etc. |

## Duration and StartedAt

Use `StartedAt` and `Duration` for slow-query detection and latency metrics:

```go
dbx.NewWithOptions(raw, dialect,
    dbx.WithHooks(dbx.HookFuncs{
        AfterFunc: func(_ context.Context, event *dbx.HookEvent) {
            if event.Duration > 100*time.Millisecond {
                slog.Warn("slow query", "sql", event.SQL, "duration", event.Duration)
            }
        },
    }),
)
```

## Metadata for Trace and Request ID

Set `Metadata` in Before to pass trace_id, request_id, or other context. Values are included in dbx logs when present:

```go
dbx.NewWithOptions(raw, dialect,
    dbx.WithHooks(dbx.HookFuncs{
        BeforeFunc: func(ctx context.Context, event *dbx.HookEvent) (context.Context, error) {
            if tid := ctx.Value("trace_id"); tid != nil {
                event.SetMetadata("trace_id", tid)
            }
            if rid := ctx.Value("request_id"); rid != nil {
                event.SetMetadata("request_id", rid)
            }
            return ctx, nil
        },
    }),
)
```

`SetMetadata` initializes the map if needed; use it to avoid nil map panics.

## Context

`Before` and `After` receive `context.Context`. Hooks can read trace/request IDs from context (e.g. via middleware) and copy them into `event.Metadata` for logging or metrics.

## Runtime Node Logs

When `WithDebug(true)` is enabled, dbx also emits stage-level runtime logs with message `dbx runtime node` and attribute `node=<name>`.

Common node groups:

- `build.*` — query build pipeline (`build.start`, `build.done`, `build.error`)
- `exec*` / `query_*` — bound query execution and scan stages
- `schema.*` — schema plan/validate/auto-migrate stages
- `relation.load.*` — relation loading stages (`single`, `multi`, `many_to_many`)
- `sql.*` — SQL statement helpers (`sql.bind.*`, `sql.list.*`, `sql.scalar.*`, etc.)

Recommended aggregation keys:

- `node`
- `statement`
- `operation`
- `table`
- `error`
