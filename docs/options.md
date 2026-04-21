---
title: 'dbx Options'
linkTitle: 'Options'
description: 'Functional options, presets, and Open for dbx'
weight: 8
---

## Options

Options use the functional Option pattern. They are composable; later options override earlier ones.

## Open (connection managed by dbx)

Use `Open` when you want dbx to own the connection. No need to pass `*sql.DB`; dbx opens and manages it internally.

```go
db, err := dbx.Open(
    dbx.WithDriver("sqlite"),
    dbx.WithDSN("file:app.db"),
    dbx.WithDialect(sqlite.New()),
    dbx.ApplyOptions(dbx.WithDebug(true)),
)
if err != nil {
    return err
}
defer db.Close()
```

Required: `WithDriver`, `WithDSN`, `WithDialect`. If any is missing, `Open` returns `ErrMissingDriver`, `ErrMissingDSN`, or `ErrMissingDialect`. Use `ApplyOptions` to pass `Option` (WithLogger, WithHooks, WithDebug).

## Presets

| Preset | Use case |
|--------|----------|
| `DefaultOptions()` | Explicit defaults (returns `nil`). Same as passing no options. |
| `ProductionOptions()` | Production: debug off. Same as defaults; use when you want explicitness. |
| `TestOptions()` | Tests: debug on so SQL is logged. |

## Usage

```go
// Defaults
core := dbx.New(raw, dialect)

// Preset + override
core, err := dbx.NewWithOptions(raw, dialect, append(dbx.TestOptions(), dbx.WithLogger(myLogger))...)
if err != nil {
    return err
}

// Custom combination
core, err := dbx.NewWithOptions(raw, dialect,
    dbx.WithLogger(logger),
    dbx.WithDebug(true),
    dbx.WithHooks(dbx.HookFuncs{AfterFunc: myAfterHook}),
)
if err != nil {
    return err
}
```

## Options table

| Option | Default | Description |
|--------|---------|-------------|
| `WithLogger(logger)` | `slog.Default()` | Logger for operation events. When debug=false, only errors are logged. |
| `WithHooks(hooks...)` | `[]` | Hooks run before/after each operation. Additive; call multiple times or pass multiple hooks to combine. See [Observability](./observability) for slow-query detection, Metadata (trace_id, request_id), and more. |
| `WithDebug(enabled)` | `false` | When true, all operations are logged at Debug level. Use in dev/tests to inspect SQL. |
| `WithNodeID(nodeID)` | auto derived from hostname | DB node id used by the built-in Snowflake generator. |
| `WithIDGenerator(generator)` | built-in generator | Overrides the built-in ID generator for this DB instance. |

## Composition

Options are applied in order. Later options override earlier for the same field. Hooks are appended, not replaced:

```go
// Logger from myLogger, debug on, hooks = [h1, h2]
dbx.NewWithOptions(raw, d,
    dbx.WithHooks(h1),
    dbx.WithLogger(myLogger),
    dbx.WithDebug(true),
    dbx.WithHooks(h2),
)
```

`WithNodeID` and `WithIDGenerator` are mutually exclusive. Passing both returns an error.

## Error Handling

```go
core, err := dbx.NewWithOptions(raw, d, dbx.WithNodeID(0))
if err != nil {
    if errors.Is(err, dbx.ErrInvalidNodeID) {
        var out *dbx.NodeIDOutOfRangeError
        if errors.As(err, &out) {
            // out.NodeID, out.Min, out.Max
        }
    }
    return err
}
_ = core
```
