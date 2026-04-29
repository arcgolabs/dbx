---
title: 'dbx Dialect'
linkTitle: 'Dialect'
description: 'Dialect abstraction for dbx and sqltmpl'
weight: 11
---

## dialect

Dialect abstraction for dbx and sqltmpl. Capabilities are layered; implement only what you need.

## Capability layers

| Layer | Interface | Required | Used by |
|-------|-----------|----------|---------|
| **Contract** | `Name()`, `BindVar(n)` | Yes | sqltmpl render, dbx render, validate |
| **Dialect** | Contract + `QuoteIdent`, `RenderLimitOffset` | Yes for query DSL | dbx query build |
| **QueryFeaturesProvider** | `QueryFeatures()` | Optional | dbx render (upsert, RETURNING, excluded ref). Fallback: `DefaultQueryFeatures(name)` for known dialects |
| **SchemaDialect** | Dialect + DDL/inspect (in dbx) | Optional | schema migrate, AutoMigrate |

## Adding a new dialect

1. Implement `dialect.Dialect` (Contract + QuoteIdent + RenderLimitOffset).
2. Implement `dialect.QueryFeaturesProvider` to declare upsert/returning support (or rely on `DefaultQueryFeatures` if your dialect matches a known one).
3. For schema migration: implement `schemamigrate.Dialect` (BuildCreateTable, InspectTable, etc.) or rely on Atlas when supported.
4. For sqltmpl validation: register a parser via `validate.Register(dialectName, factory)`.

No need to add dialect branches in render.go, schema_migrate_atlas.go, or sqltmpl—capabilities are declared via interfaces.
