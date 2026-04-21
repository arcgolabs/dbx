---
title: 'Production Checklist'
linkTitle: 'production-checklist'
description: 'Recommended production configuration for dbx and sqltmplx'
weight: 17
---

## Production Checklist

Use this checklist before production rollout.

## When to Use

- You are preparing for first production release.
- You want pre-release review criteria for schema, IDs, indexes, and migrations.
- You need a repeatable release checklist for teams.

## 1) Dialect and Connection

- choose explicit dialect package (`sqlite.New()`, `postgres.New()`, `mysql.New()`)
- verify DSN and connection pool sizing in your `database/sql` setup
- enable SQL debug logging only in non-production environments

## 2) Schema Declaration

- use `schemax.Schema[E]` as single metadata source
- avoid duplicated table metadata outside schema types
- define relation metadata with typed relation fields

## 3) ID Strategy

- declare ID strategy in schema (`IDColumn[..., ..., Marker]`)
- use `WithNodeID(nodeID)` for Snowflake in multi-instance deployments
- do not set both `WithNodeID` and `WithIDGenerator` in the same DB options
- handle `ErrInvalidNodeID` / `NodeIDOutOfRangeError` with `errors.Is/As`

## 4) Indexing

- declare single-column indexes with `index` / `unique` tag options
- declare composite indexes with `dbx.Index` / `dbx.Unique`
- verify generated index SQL with `PlanSchemaChanges(...).SQLPreview()`

## 5) Migrations

- use `PlanSchemaChanges` in CI to inspect DDL changes
- run `ValidateSchemas` against production-like snapshots before release
- use `AutoMigrate` conservatively and keep manual migration scripts for destructive changes

## 6) Pure SQL (sqltmplx)

- keep SQL in `.sql` files and use registry statements (`MustStatement`) for reuse
- rely on the default compiled-template cache for repeated inline `Engine.Render`; tune with `WithTemplateCacheSize`
- enable parser-backed validator in development pipelines
- avoid dynamic SQL string concatenation in runtime code

## 7) Observability

- attach hooks for operation-level metrics and tracing
- propagate request metadata (`trace_id`, `request_id`) into DB context
- monitor slow-query thresholds and operation durations

## Related Docs

- [Getting Started](./getting-started)
- [Schema Design](./schema-design)
- [ID Generation](./id-generation)
- [Indexes](./indexes)
- [Migration Tutorial](./tutorial-migration)
- [sqltmplx Integration](./sqltmplx-integration)

## Pitfalls

- Treating checklist items as optional often causes runtime surprises during scale-out.
- Running with implicit defaults in multi-instance deployments can create ID risks.
- Lack of migration preview and validation in CI increases rollback probability.

## Verify

```bash
go test ./dbx/...
go test ./dbx/sqltmplx/...
```
