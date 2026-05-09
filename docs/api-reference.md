---
title: 'API Quick Reference'
linkTitle: 'api-reference'
description: 'Quick lookup for core dbx and sqltmpl-related APIs'
weight: 18
---

## API Quick Reference

## Open and DB Construction

- `dbx.Open(options...)` - dbx manages SQL connection lifecycle.
- `dbx.New(rawDB, dialect)` - construct session wrapper with existing `*sql.DB`.
- `dbx.NewWithOptions(rawDB, dialect, opts...)` - construct with runtime options and validation.
- `dbx.MustNewWithOptions(...)` - panic-on-error variant for tests/examples.

## Schema and Mapper

- `schemax.MustSchema(table, schemaStruct)` - bind schema metadata.
- `mapperx.MustMapper[T](schema)` - schema-aware mapper.
- `mapperx.MustStructMapper[T]()` - schema-less DTO mapper.
- `mapper.InsertAssignments(session, schema, entity)` - generate insert assignments (including ID generation).

## Query and Execute

- `querydsl.SelectFrom(schema, columns...).Where(...)`
- `querydsl.SelectInto[T](columns...).From(schema).Where(...)`
- `querydsl.SelectValue(column).From(schema).Where(...)` for typed scalar subqueries
- `querydsl.From(schema).Select(columns...).Where(...)`
- `querydsl.Like(stringExpr, pattern)` / `querydsl.NotLike(stringExpr, pattern)` for string-only predicates.
- `querydsl.CompareValue(expr, op, value)` / `CompareOperand(expr, op, other)` / `CompareQuery(expr, op, subquery)` for typed operand comparisons.
- `column.In(...)` / `column.NotIn(...)` / `column.InQuery(...)` / `column.NotInQuery(...)` for typed set predicates.
- `column.Between(lower, upper)` / `column.NotBetween(lower, upper)` for typed range predicates.
- `querydsl.Sum(expr)` / `Avg(expr)` / `Min(expr)` / `Max(expr)` accept typed operands, including columns and typed CASE expressions.
- `querydsl.InsertInto(schema).Values(assignments.Values()...)`
- `querydsl.Update(schema).Set(...).Where(...)`
- `querydsl.DeleteFrom(schema).Where(...)`
- `dbx.Exec(ctx, session, query)` / `dbx.QueryAll[T](ctx, session, query, scanner)` / `dbx.QueryTyped[T](ctx, session, typedQuery)`
- `dbx.GetTyped[T](ctx, session, typedQuery)` for exactly one typed result row.
- `dbx.FindTyped[T](ctx, session, typedQuery)` for optional typed result rows.
- `dbx.QueryScalar[T](ctx, session, querydsl.SelectValue(column).From(schema))` for exactly one typed scalar row.
- `dbx.QueryScalarOption[T](ctx, session, scalarQuery)` for optional typed scalar rows.
- `dbx.QueryAllTyped[T]`, `dbx.QueryOneTyped[T]`, and `dbx.QueryOptionTyped[T]` when you need an explicit mapper.
- `dbx.Build(session, query)` then `ExecBound` / `QueryAllBound[T]` / `QueryOneBound[T]` / `QueryOptionBound[T]` / `QueryScalarBound[T]` for reuse.
- `paging.Page(page, pageSize)` / `paging.NewRequest(page, pageSize)` for shared pagination requests.
- `paging.NewResult(items, total, request)` / `paging.MapResult(...)` for pagination metadata.

## Repository Pagination

- `repository.Page(page, pageSize)` and `repository.PageByRequest(request)` specs.
- `repo.ListPage(ctx, query, page, pageSize)` and `repo.ListPageRequest(ctx, query, request)`.
- `repo.ListPageSpec(ctx, page, pageSize, specs...)` and `repo.ListPageSpecRequest(ctx, request, specs...)`.
- `repository.Query(repo).Where(...).List(ctx)` / `Find(ctx)` / `FirstOption(ctx)` for fluent repository queries.
- `repository.PageRequest` / `repository.PageResult[T]` are aliases of the shared `paging` pagination model.

## Active Record

- `activerecord.New[E](db, schema)` / `NewWithOptions[E](db, schema, opts...)`.
- `store.First(ctx, specs...)` / `store.Find(ctx, specs...)` for spec-based single model reads.
- `activerecord.By(store, Users.ID).Find(ctx, id)` / `FindOption(ctx, id)` for typed key reads.
- `store.List(ctx, specs...)` / `store.ListPage(ctx, request, specs...)` for model collections.

## Migration and Schema Validation

- `schemamigrate.PlanSchemaChanges(ctx, session, schemas...)`
- `schemamigrate.ValidateSchemas(ctx, session, schemas...)`
- `schemamigrate.AutoMigrate(ctx, session, schemas...)`
- `plan.SQLPreview()`
- `migrate.NewRunner(sqlDB, dialect, options)` - construct migration runner
- `migrate.NewGoMigration(version, description, up, down, databases...)` - optional `databases` names bind a Go migration to one or more dialects; empty means run for all
- `(*Runner).UpGo(ctx, migrations...)`
- `(*Runner).UpGoTo(ctx, version, migrations...)`
- `(*Runner).DownGoTo(ctx, version, migrations...)`
- `(*Runner).UpSQL(ctx, source)` / `(*Runner).UpSQLTo(ctx, version, source)` / `(*Runner).DownSQLTo(ctx, version, source)`
- `(*Runner).UpSQLFor(ctx, dialect, source)` / `(*Runner).UpSQLToFor(ctx, version, dialect, source)` / `(*Runner).DownSQLToFor(ctx, version, dialect, source)`
- `FileSource.Database` or `FileSource.ForDialect(...)`: set explicit SQL migration dialect suffix filter, e.g. `source.ForDialect(migrate.DialectMySQL)`
- `(*Runner).PendingGo(ctx, migrations...)`
- `(*Runner).PendingSQL(ctx, source)`
- `(*Runner).PendingSQLFor(ctx, dialect, source)`
- `(*Runner).StatusGo(ctx, migrations...)`
- `(*Runner).StatusSQL(ctx, source)`
- `(*Runner).StatusSQLFor(ctx, dialect, source)`
- `(*Runner).StatusAll(ctx, goMigrations, source)`
- `(*Runner).PendingAll(ctx, goMigrations, source)`
- `(*Runner).ValidateApplyAll(spec migrate.MigrationApplySpec)` - validate high-level migration spec before execution.
- `(*Runner).ApplyAll(ctx, spec migrate.MigrationApplySpec)` (high-level orchestration, low-level methods above remain)
- `(*Runner).Applied(ctx)`
- `migrate.MigrationStatusBundle`, `migrate.MigrationPendingBundle`, `migrate.MigrationApplySpec`, `migrate.MigrationTarget`

## ID Generation Options

- `dbx.WithNodeID(nodeID)`
- `dbx.WithIDGenerator(generator)`
- `idgen.NewSnowflake(nodeID)`
- `idgen.ResolveNodeIDFromHostName()`

## sqltmpl Integration

- `sqltmpl.New(dialect, options...)`
- `sqltmpl.WithTemplateCacheSize(size)` - configure the compiled-template LRU cache for `Engine.Render` / `Compile`.
- `sqltmpl.WithValidator(validator)` - validate rendered SQL during development or CI.
- `sqltmpl.NewRegistry(fs, dialect)`
- `registry.MustStatement(path)`
- `registry.Statement("sql/user/find.sql")` resolves `sql/user/find_<dialect>.sql`, then `sql/user/find__<dialect>.sql`, then the base file.
- `registry.StatementFor("sql/user/find.sql", dialect)` uses a call-site dialect for suffix resolution and placeholder rendering.
- `sqltmpl.Page(page, pageSize)` / `sqltmpl.NewPageRequest(page, pageSize)`
- `sqltmpl.WithPage(params, request)` / `sqltmpl.WithTypedPage[P](params, request)`
- `template.RenderPage(params, request)` / `template.BindPage(params, request)`
- `sqlexec.List[T]` / `Get[T]` / `Find[T]` / `Scalar[T]` / `ScalarOption[T]`
- `sqlstmt.For[P](statement)` or `sqlstmt.NewTyped[P](name, binder)` + `sqlexec.ListTyped[P, T]` / `GetTyped[P, T]` / `FindTyped[P, T]` / `ScalarTyped[P, T]` / `ScalarOptionTyped[P, T]`

## Common Error Sentinels and Types

- `dbx.ErrMissingDriver`, `dbx.ErrMissingDSN`, `dbx.ErrMissingDialect`
- `dbx.ErrIDGeneratorNodeIDConflict`
- `dbx.ErrTooManyRows`
- `dbx.ErrInvalidNodeID`
- `*dbx.NodeIDOutOfRangeError`
