---
title: 'dbx examples'
linkTitle: 'examples'
description: 'Runnable examples for dbx'
weight: 10
---

## dbx Examples

This page collects the runnable `examples` programs and maps them to the API surface they demonstrate.

## Run Locally

Run from the `examples` module:

```bash
cd examples
go run ./basic
go run ./codec
go run ./mutation
go run ./query_advanced
go run ./relations
go run ./migration
go run ./pure_sql
go run ./id_generation
```

You can also run directly from the repository root:

```bash
go run ./examples/basic
go run ./examples/codec
go run ./examples/mutation
go run ./examples/query_advanced
go run ./examples/relations
go run ./examples/migration
go run ./examples/pure_sql
go run ./examples/id_generation
```

## Example Matrix

| Example | Focus | Directory |
| --- | --- | --- |
| `basic` | schema-first modeling, mapper scan, projection, tx, debug SQL, hooks | [examples/basic](https://github.com/arcgolabs/dbx/tree/main/examples/basic) |
| `codec` | built-in codecs, scoped custom codecs, struct mapper reads, mapper write assignments | [examples/codec](https://github.com/arcgolabs/dbx/tree/main/examples/codec) |
| `mutation` | aggregate queries, subqueries, batch insert, insert-select, upsert, returning | [examples/mutation](https://github.com/arcgolabs/dbx/tree/main/examples/mutation) |
| `query_advanced` | `WITH`, `UNION ALL`, `CASE WHEN`, named tables, result columns | [examples/query_advanced](https://github.com/arcgolabs/dbx/tree/main/examples/query_advanced) |
| `relations` | alias + relation metadata + `JoinRelation`, plus `LoadBelongsTo` and `LoadManyToMany` | [examples/relations](https://github.com/arcgolabs/dbx/tree/main/examples/relations) |
| `migration` | `PlanSchemaChanges`, `SQLPreview`, `AutoMigrate`, `ValidateSchemas`, `migrate.NewRunner(core.SQLDB(), core.Dialect(), ...).UpGo/UpSQL` | [examples/migration](https://github.com/arcgolabs/dbx/tree/main/examples/migration) |
| `pure_sql` | `sqltmplx` registry, shared `PageRequest` pagination, `sqlexec.List/Get/Find/Scalar`, statement-name logging, `tx.SQL().Exec(...)` | [examples/pure_sql](https://github.com/arcgolabs/dbx/tree/main/examples/pure_sql) |
| `id_generation` | typed ID strategy markers: `IDAuto`, `IDSnowflake`, `IDUUIDv7`, and `IDColumn` | [examples/id_generation](https://github.com/arcgolabs/dbx/tree/main/examples/id_generation) |

## Coverage (by topic)

Taken together, the examples exercise:

- schema as the single metadata source; aggregates, subqueries, batch insert, insert-select, upsert, `RETURNING`
- advanced DSL: `WITH`, `UNION ALL`, `CASE WHEN`
- mapper scans, field codecs, scoped custom codecs via `mapperx.WithMapperCodecs(...)`
- relation join helpers and `LoadBelongsTo` / `LoadManyToMany`
- pure SQL via `sqltmplx` registry, shared `PageRequest`, and `dbx.SQL*`
- typed ID strategies via `IDColumn` markers
- `PlanSchemaChanges`, `ValidateSchemas`, `AutoMigrate`, and the `dbx/migrate` runner
- optional `slog` SQL debug logging and hooks

## Example: Codec and StructMapper

```go
mapper := mapperx.MustStructMapperWithOptions[shared.Account](
    mapperx.WithMapperCodecs(csvCodec),
)

items, err := dbx.QueryAll[shared.Account](
    ctx,
    core,
    querydsl.Select(querydsl.AllColumns(catalog.Accounts).Values()...).From(catalog.Accounts),
    mapper,
)
if err != nil {
    panic(err)
}
```

## Example: Advanced Query DSL

```go
statusLabel := querydsl.CaseWhen[string](catalog.Users.Status.Eq(1), "active").
    Else("inactive").
    As("status_label")

activeUsers := querydsl.NamedTable("active_users")
activeID := columnx.Named[int64](activeUsers, "id")
activeName := columnx.Named[string](activeUsers, "username")

query := querydsl.Select(activeID, activeName, statusLabel).
    With("active_users",
        querydsl.Select(catalog.Users.ID, catalog.Users.Username).
            From(catalog.Users).
            Where(catalog.Users.Status.Eq(1)),
    ).
    From(activeUsers)
```

## Example: Relation Loading

```go
if err := relationload.LoadBelongsTo[shared.User, shared.Role](
    ctx,
    core,
    users,
    catalog.Users,
    userMapper,
    catalog.Users.Role,
    catalog.Roles,
    roleMapper,
    func(index int, user shared.User, role mo.Option[shared.Role]) shared.User {
        // attach role
        return user
    },
); err != nil {
    panic(err)
}
```

## Example: Schema Plan Preview and Runner

```go
plan, err := schemamigrate.PlanSchemaChanges(ctx, core, catalog.Roles, catalog.Users, catalog.UserRoles)
if err != nil {
    panic(err)
}

for _, sqlText := range plan.SQLPreview() {
    fmt.Println(sqlText)
}

runner := migrate.NewRunner(core.SQLDB(), core.Dialect(), migrate.RunnerOptions{ValidateHash: true})
_, err = runner.UpSQL(ctx, source)
if err != nil {
    panic(err)
}
```

## Example: Pure SQL With sqltmplx

```go
registry := sqltmplx.NewRegistry(sqlFS, core.Dialect())

items, err := sqlexec.List[shared.UserSummary](
	ctx,
	core,
	registry.MustStatement("sql/user/find_active.sql"),
	sqltmplx.WithPage(struct {
		Status int `dbx:"status"`
	}{Status: 1}, sqltmplx.Page(1, 20)),
	mapperx.MustStructMapper[shared.UserSummary](),
)
if err != nil {
	panic(err)
}
```
