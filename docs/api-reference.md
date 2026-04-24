---
title: 'API Quick Reference'
linkTitle: 'api-reference'
description: 'Quick lookup for core dbx and sqltmplx-related APIs'
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

- `querydsl.Select(...).From(...).Where(...)`
- `querydsl.InsertInto(schema).Values(assignments.Values()...)`
- `querydsl.Update(schema).Set(...).Where(...)`
- `querydsl.DeleteFrom(schema).Where(...)`
- `dbx.Exec(ctx, session, query)` / `dbx.QueryAll[T](ctx, session, query, scanner)`
- `dbx.Build(session, query)` then `ExecBound` / `QueryAllBound[T]` for reuse.
- `paging.Page(page, pageSize)` / `paging.NewRequest(page, pageSize)` for shared pagination requests.
- `paging.NewResult(items, total, request)` / `paging.MapResult(...)` for pagination metadata.

## Repository Pagination

- `repository.Page(page, pageSize)` and `repository.PageByRequest(request)` specs.
- `repo.ListPage(ctx, query, page, pageSize)` and `repo.ListPageRequest(ctx, query, request)`.
- `repo.ListPageSpec(ctx, page, pageSize, specs...)` and `repo.ListPageSpecRequest(ctx, request, specs...)`.
- `repository.PageRequest` / `repository.PageResult[T]` are aliases of the shared `paging` pagination model.

## Migration and Schema Validation

- `schemamigrate.PlanSchemaChanges(ctx, session, schemas...)`
- `schemamigrate.ValidateSchemas(ctx, session, schemas...)`
- `schemamigrate.AutoMigrate(ctx, session, schemas...)`
- `plan.SQLPreview()`

## ID Generation Options

- `dbx.WithNodeID(nodeID)`
- `dbx.WithIDGenerator(generator)`
- `idgen.NewSnowflake(nodeID)`
- `idgen.ResolveNodeIDFromHostName()`

## sqltmplx Integration

- `sqltmplx.New(dialect, options...)`
- `sqltmplx.WithTemplateCacheSize(size)` - configure the compiled-template LRU cache for `Engine.Render` / `Compile`.
- `sqltmplx.WithValidator(validator)` - validate rendered SQL during development or CI.
- `sqltmplx.NewRegistry(fs, dialect)`
- `registry.MustStatement(path)`
- `sqltmplx.Page(page, pageSize)` / `sqltmplx.NewPageRequest(page, pageSize)`
- `sqltmplx.WithPage(params, request)`
- `template.RenderPage(params, request)` / `template.BindPage(params, request)`
- `sqlexec.List[T]` / `Get[T]` / `Find[T]` / `Scalar[T]` / `ScalarOption[T]`

## Common Error Sentinels and Types

- `dbx.ErrMissingDriver`, `dbx.ErrMissingDSN`, `dbx.ErrMissingDialect`
- `dbx.ErrIDGeneratorNodeIDConflict`
- `dbx.ErrInvalidNodeID`
- `*dbx.NodeIDOutOfRangeError`
