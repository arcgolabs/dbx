---
title: 'Indexes'
linkTitle: 'indexes'
description: 'Single-column and composite index configuration in dbx'
weight: 10
---

## Indexes

`dbx` supports index declaration at schema level. Migration planning and `AutoMigrate` read these declarations and generate missing indexes.

## When to Use

- You want schema-owned index definitions.
- You need both simple and composite indexes.
- You want migration preview to include index DDL.

## Minimal Project Layout

```text
.
├── go.mod
└── internal
    └── schema
        └── user.go
```

## Single-Column Indexes

Declare indexes directly in column tags:

```go
type UserSchema struct {
	schemax.Schema[User]
	ID       columnx.Column[User, int64]  `dbx:"id,pk"`
	Username columnx.Column[User, string] `dbx:"username,index"`
	Email    columnx.Column[User, string] `dbx:"email,unique"`
}
```

- `index` / `indexed`: non-unique index
- `unique`: unique index

## Composite Indexes

Use dedicated fields with `idx` tag:

```go
type UserSchema struct {
	schemax.Schema[User]
	TenantID columnx.Column[User, int64]  `dbx:"tenant_id"`
	Username columnx.Column[User, string] `dbx:"username"`
	Email    columnx.Column[User, string] `dbx:"email"`

	ByTenantAndUsername schemax.Index[User]  `idx:"columns=tenant_id|username"`
	UniqueTenantEmail   schemax.Unique[User] `idx:"columns=tenant_id|email"`
}
```

## Naming Convention

When names are not explicitly provided:

- primary key: `pk_<table>`
- normal index: `idx_<table>_<column_or_columns>`
- unique index: `ux_<table>_<column_or_columns>`

## Migration Behavior

- `PlanSchemaChanges` reports missing indexes in SQL preview.
- `AutoMigrate` creates missing indexes conservatively.
- Existing index definitions are inspected by dialect-specific introspection logic.

## Pitfalls

- `idx:"columns=..."` must reference schema column names, not struct field names.
- Over-indexing frequently updated columns can hurt write throughput.
- Rely on explicit composite indexes for multi-column query filters; separate single-column indexes are not equivalent.

## Verify

```bash
go test ./... -run Migrate
```
