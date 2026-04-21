---
title: 'dbx Benchmarks'
linkTitle: 'benchmarks'
description: 'Benchmark scope, commands, and optimization notes'
weight: 11
---

## Benchmarks

This page consolidates benchmark guidance previously documented in package-level markdown files.

## Run

```bash
go test ./dbx ./dbx/migrate -run '^$' -bench . -benchmem -count=3
```

## Memory vs IO Backends

SQLite benchmarks usually run in two modes:

- **Memory**: `:memory:` (CPU + alloc focused)
- **IO**: temp-file SQLite (closer to production latency)

If Memory is much faster than IO, the path is typically I/O-bound. If numbers are close, the path is more CPU-bound.

## Bottleneck Snapshot

- `ValidateSchemas*` / `PlanSchemaChanges*`: schema diff and migration planning
- relation loading (`LoadManyToMany`, `LoadBelongsTo`, `LoadHasMany`): query count + scan cost
- query + scan (`QueryAll*`, `SQLList`, `SQLGet`): read-path hot spots
- render/build (`Build*`): SQL construction overhead

## Optimization Priorities

- prioritize schema planning cache and short-circuit on matched schemas
- reduce round-trips for relation loading where possible
- reuse bound statements in hot paths (`Build` once; execute many)
- keep mapper and scan path allocation-conscious

## Related Docs

- Core overview: [dbx](./)
- SQL templates: [sqltmplx integration](./sqltmplx-integration)
- Runnable examples: [Examples](./examples)
