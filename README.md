## dbx

`dbx` is a schema-first, generic-first ORM core built on top of `database/sql`.
It keeps database metadata in `Schema[E]`, keeps entities as data carriers, and currently covers these core pipelines:

- typed schema and relation modeling
- typed query DSL and SQL rendering
- mapper / struct-mapper reads with codec support
- pure SQL execution through `sqltmpl` statements
- relation loading for `BelongsTo` / `HasOne` / `HasMany` / `ManyToMany`
- schema planning, validation, conservative auto-migrate, and migration runner
- runtime logging, hooks, transactions, and benchmark coverage

## Current Status

The current `dbx` implementation includes:

- `Schema[E]` as the single source of database metadata
- `Column[E, T]` and typed relation refs
- query DSL support for aggregates, subqueries, CTE, `UNION ALL`, `CASE WHEN`, batch insert, `INSERT ... SELECT`, upsert, and `RETURNING`
- `StructMapper[E]` (schema-less pure DTO mapping) and `Mapper[E]` (schema-bound, for CRUD/relation load); `RowsScanner` as read contract
- field codecs with built-in `json`, `text`, `unix_time`, `unix_milli_time`, `unix_nano_time`, `rfc3339_time`, and `rfc3339nano_time`
- scoped custom codecs via `mapperx.WithMapperCodecs(...)`
- `DB.SQL()` / `Tx.SQL()` as the pure SQL execution entry
- relation loading APIs and relation-aware join helpers
- `PlanSchemaChanges`, `ValidateSchemas`, `AutoMigrate`, and `MigrationPlan.SQLPreview()`
- `dbx/migrate` runner for Go migrations and Flyway-style SQL migrations
- benchmark coverage across mapper, query builder, SQL executor, relation loading, schema planning, and migrate runner

## Internal Engines

The public API remains `dbx`-centric. Internally, the current implementation uses:

- `scan` for the read-side scan pipeline
- `Atlas` for schema planning / validation on supported dialects
- `goose` as the migration runner engine inside `dbx/migrate`
- `hot` for runtime cache storage

These are implementation details. The exposed API is still `dbx`, `dbx/sqltmpl`, and `dbx/migrate`.

## Package Layout

- Core ORM API: `github.com/arcgolabs/dbx`
- Generic repository: `github.com/arcgolabs/dbx/repository` (see [Repository Mode](./repository))
- Active record facade: `github.com/arcgolabs/dbx/activerecord` (see [Active Record Mode](./active-record))
- Shared dialect contracts: `github.com/arcgolabs/dbx/dialect` (see [Dialect](./dialect))
- Built-in query + schema dialects:
    - `github.com/arcgolabs/dbx/dialect/sqlite`
    - `github.com/arcgolabs/dbx/dialect/postgres`
    - `github.com/arcgolabs/dbx/dialect/mysql`
- SQL template engine in the same ecosystem:
    - `github.com/arcgolabs/dbx/sqltmpl`
- Migration runner package:
    - `github.com/arcgolabs/dbx/migrate`

## Documentation Map

- Start here: [Getting Started](./getting-started)
- Schema declaration and modeling: [Schema Design](./schema-design)
- End-to-end CRUD: [CRUD Tutorial](./tutorial-crud)
- Relation loading walkthrough: [Relations Tutorial](./tutorial-relations)
- Schema planning and migration: [Migration Tutorial](./tutorial-migration)
- Pure SQL with templates: [Pure SQL Tutorial](./tutorial-pure-sql)
- ID strategies and runtime generator configuration: [ID Generation](./id-generation)
- Index declaration and migration behavior: [Indexes](./indexes)
- Runtime options: [Options](./options)
- Logging and hooks: [Observability](./observability)
- Production rollout checklist: [Production Checklist](./production-checklist)
- API quick lookup: [API Quick Reference](./api-reference)
- Generic repository abstraction: [Repository Mode](./repository)
- Active record facade: [Active Record Mode](./active-record)
- Dialect abstraction: [Dialect](./dialect)
- dbx + pure SQL templates: [sqltmpl Integration](./sqltmpl-integration)
- Runnable examples: [Examples](./examples)
- Benchmark notes: [Benchmarks](./benchmarks)
- Maintainer conventions: [Style Guide](./STYLE.md)

## Install / Import

```bash
go get github.com/arcgolabs/dbx@latest
go get github.com/arcgolabs/dbx/sqltmpl@latest
go get github.com/arcgolabs/dbx/migrate@latest
```

## Open (connection managed by dbx)

Use `Open` when you want dbx to own the connection—no need to pass `*sql.DB`:

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

Required: `WithDriver`, `WithDSN`, `WithDialect`. Missing any returns `ErrMissingDriver`, `ErrMissingDSN`, or `ErrMissingDialect`. Use `ApplyOptions` for `WithLogger`, `WithHooks`, `WithDebug`.

## Schema First

Schema owns database metadata. Entities only carry field mapping tags.

```go
package main

import "github.com/arcgolabs/dbx"

type Role struct {
    ID   int64  `dbx:"id"`
    Name string `dbx:"name"`
}

type User struct {
    ID       int64  `dbx:"id"`
    Username string `dbx:"username"`
    Email    string `dbx:"email_address"`
    Status   int    `dbx:"status"`
    RoleID   int64  `dbx:"role_id"`
}

type RoleSchema struct {
    schemax.Schema[Role]
    ID   columnx.Column[Role, int64]  `dbx:"id,pk"`
    Name columnx.Column[Role, string] `dbx:"name,unique"`
}

type UserSchema struct {
    schemax.Schema[User]
    ID       columnx.Column[User, int64]   `dbx:"id,pk"`
    Username columnx.Column[User, string]  `dbx:"username"`
    Email    columnx.Column[User, string]  `dbx:"email_address,unique"`
    Status   columnx.Column[User, int]     `dbx:"status,default=1"`
    RoleID   columnx.Column[User, int64]   `dbx:"role_id,ref=roles.id,ondelete=cascade"`
    Role     relationx.BelongsTo[User, Role] `rel:"table=roles,local=role_id,target=id"`
}

var Roles = schemax.MustSchema("roles", RoleSchema{})
var Users = schemax.MustSchema("users", UserSchema{})
```

For explicit typed ID strategy configuration, use marker types:

```go
type Event struct {
    ID   int64  `dbx:"id"`
    Name string `dbx:"name"`
}

type EventSchema struct {
    schemax.Schema[Event]
    ID   columnx.IDColumn[Event, int64, idgen.IDSnowflake] `dbx:"id,pk"`
    Name columnx.Column[Event, string]                   `dbx:"name"`
}

var Events = schemax.MustSchema("events", EventSchema{})
```

## Query DSL

`dbx` renders typed queries into `BoundQuery`, then executes them through `DB` or `Tx`. For "build once, execute many" reuse, call `Build` once and use `ExecBound`, `QueryAllBound`, `QueryCursorBound`, or `QueryEachBound` in a loop:

```go
query := querydsl.SelectFrom(Users, Users.ID, Users.Username).Where(Users.Status.Eq(1))
bound, _ := dbx.Build(session, query)
for range batches {
    items, _ := dbx.QueryAllBound[User](ctx, session, bound, mapper)
    // ...
}
```

```go
statusLabel := querydsl.CaseWhen[string](Users.Status.Eq(1), "active").
    When(Users.Status.Eq(2), "blocked").
    Else("unknown").
    As("status_label")

type ActiveUsersSource struct {
    querydsl.Table
    ID       querydsl.Column[int64]  `dbx:"id"`
    Username querydsl.Column[string] `dbx:"username"`
}

activeUsers := querydsl.MustSource("active_users", ActiveUsersSource{})

query := querydsl.SelectFrom(activeUsers, activeUsers.ID, activeUsers.Username, statusLabel).
    With("active_users",
        querydsl.SelectFrom(Users, Users.ID, Users.Username).
            Where(Users.Status.Eq(1)),
    ).
    UnionAll(
        querydsl.SelectFrom(Users, Users.ID, Users.Username, statusLabel).
            Where(Users.Status.Ne(1)),
    )
```

## Mapper, StructMapper, and Codecs

- **StructMapper[E]** — schema-less pure DTO mapping. Use for arbitrary SQL (SQLList, SQLGet, QueryAll) when no Schema is available. Maps result columns to struct fields by name from struct tags.
- **Mapper[E]** — schema-bound; extends StructMapper with a schema-derived field subset. Use for CRUD, relation load, repository when you have a Schema.
- **RowsScanner[E]** — read contract; both implement it. Dependency: StructMapper is independent; Mapper depends on Schema.

```go
type Preferences struct {
    Theme string   `json:"theme"`
    Flags []string `json:"flags"`
}

type Account struct {
    ID          int64       `dbx:"id"`
    Preferences Preferences `dbx:"preferences,codec=json"`
    Tags        []string    `dbx:"tags,codec=csv"`
}

csvCodec := dbx.NewCodec[[]string](
    "csv",
    func(src any) ([]string, error) { /* ... */ },
    func(values []string) (any, error) { /* ... */ },
)

mapper := mapperx.MustStructMapperWithOptions[Account](
    mapperx.WithMapperCodecs(csvCodec),
)
```

## Relation Loading

`dbx` now supports batch relation loading in addition to join helpers.

```go
loader := relationload.New[User, Role](core, Users, Roles)

if err := loader.BelongsTo(
    ctx,
    collectionx.NewList[User](users...),
    Users.Role,
    func(index int, user User, role mo.Option[Role]) User {
        // attach resolved role here
        return user
    },
); err != nil {
    panic(err)
}
```

## Pure SQL Entry

`sqltmpl` stays responsible for template compile / render / validate. `dbx` owns execution, transaction handling, hooks, logging, and the shared `PageRequest` pagination model.

```go
//go:embed sql/**/*.sql
var sqlFS embed.FS

registry := sqltmpl.NewRegistry(sqlFS, core.Dialect())

items, err := sqlexec.List[UserSummary](
	ctx,
	core,
	registry.MustStatement("sql/user/find_active.sql"),
	sqltmpl.WithPage(struct {
		Status int `dbx:"status"`
	}{Status: 1}, sqltmpl.Page(1, 20)),
	mapperx.MustStructMapper[UserSummary](),
)
if err != nil {
	panic(err)
}

type FindActiveParams struct {
	Status int `dbx:"status"`
}

typedStmt := sqlstmt.For[FindActiveParams](registry.MustStatement("sql/user/find_active.sql"))
items, err = sqlexec.ListTyped[FindActiveParams, UserSummary](
	ctx,
	core,
	typedStmt,
	sqltmpl.WithPage(FindActiveParams{Status: 1}, sqltmpl.Page(1, 20)),
	mapperx.MustStructMapper[UserSummary](),
)
```

Pure SQL helpers:

- `db.SQL().Exec(...)` / `tx.SQL().Exec(...)`
- `sqlexec.List[T](...)`
- `sqlexec.ListTyped[P, T](...)`
- `sqlexec.Get[T](...)`
- `sqlexec.Find[T](...)`
- `sqlexec.Scalar[T](...)`
- `sqlexec.ScalarOption[T](...)`

`SQLFind` and `SQLScalarOption` return `mo.Option[T]`.

## Schema Planning and Migration Runner

`dbx` supports schema planning, validation, SQL preview, conservative auto-migrate, and a separate migration runner.

```go
plan, err := schemamigrate.PlanSchemaChanges(ctx, core, Roles, Users)
if err != nil {
    panic(err)
}

for _, sqlText := range plan.SQLPreview() {
    fmt.Println(sqlText)
}

runner := migrate.NewRunner(core.SQLDB(), core.Dialect(), migrate.RunnerOptions{ValidateHash: true})
_, err = runner.UpGo(ctx, migrate.NewGoMigration("1", "create users", up, nil))
if err != nil {
    panic(err)
}
```

Current behavior:

- build missing tables
- add missing columns
- add missing indexes
- add missing foreign keys and checks when the dialect supports it
- stop and report when a manual migration is required

## Options and Presets

Options use the functional Option pattern and are composable (later overrides earlier). Presets:

- `DefaultOptions()` — explicit defaults (same as no options)
- `ProductionOptions()` — debug off; combine with `WithLogger` as needed
- `TestOptions()` — debug on for SQL logging; combine with `WithLogger`, `WithHooks`

Individual options: `WithLogger(logger)` (default: slog.Default), `WithHooks(hooks...)` (additive), `WithDebug(enabled)` (default: false). See [Options](./options) for details.  
For typed primary-key strategy configuration, see [ID Generation](./id-generation).

## Runtime Logging and Hooks

`DB` and `Tx` provide runtime observation hooks and `slog` debug logging. Pure SQL statements also carry their statement names into hook events and debug logs. For slow-query detection, Duration, Metadata (trace_id, request_id), see [Observability](./observability).

```go
core := dbx.NewWithOptions(
    sqlDB,
    sqlite.New(),
    dbx.WithLogger(logger),
    dbx.WithDebug(true),
    dbx.WithHooks(dbx.HookFuncs{
        AfterFunc: func(_ context.Context, event *dbx.HookEvent) {
            fmt.Println(event.Operation, event.Statement)
        },
    }),
)
```

## Error and Behavior Model

- `ErrNilDB`, `ErrNilEntity`, mapper binding errors, and schema validation errors stay explicit.
- Repository mode uses typed error layers (`ErrNotFound`, `ErrConflict`, `ErrValidation`, `ErrVersionConflict`).
- Option-style helpers (for example `SQLFind`, repository `*Option` methods) separate "not found" from execution failures.
- Schema planning and auto-migrate follow conservative behavior; destructive evolution requires explicit operator control.

## Benchmarks

`dbx` now includes benchmark coverage for its major pipelines.

Run locally:

```bash
go test ./dbx -run '^$' -bench .
go test ./dbx/migrate -run '^$' -bench .
```

Covered areas:

- mapper metadata and scan path
- codec-aware reads and write assignments
- query build and SQL render
- relation loading
- schema planning / validation / SQL preview
- SQL executor helpers
- migration file source and runner

## Examples

- Example guide: [dbx examples](./examples)
- Runnable examples:
    - [examples/basic](https://github.com/arcgolabs/dbx/tree/main/examples/basic)
    - [examples/codec](https://github.com/arcgolabs/dbx/tree/main/examples/codec)
    - [examples/mutation](https://github.com/arcgolabs/dbx/tree/main/examples/mutation)
    - [examples/query_advanced](https://github.com/arcgolabs/dbx/tree/main/examples/query_advanced)
    - [examples/relations](https://github.com/arcgolabs/dbx/tree/main/examples/relations)
    - [examples/migration](https://github.com/arcgolabs/dbx/tree/main/examples/migration)
    - [examples/pure_sql](https://github.com/arcgolabs/dbx/tree/main/examples/pure_sql)

## Verification

```bash
go test ./...
go test ./examples/...
go run ./examples/basic
go run ./examples/codec
go run ./examples/mutation
go run ./examples/query_advanced
go run ./examples/relations
go run ./examples/migration
go run ./examples/pure_sql
```

## Integration Guide

- With `configx`: externalize driver, DSN, dialect, and migration toggles.
- With `logx` / `observabilityx`: attach SQL debug/hook signals with cardinality-safe metadata.
