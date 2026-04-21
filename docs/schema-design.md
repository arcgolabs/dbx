---
title: 'Schema Design'
linkTitle: 'schema-design'
description: 'How to declare schema, relations, ID strategy, and indexes in dbx'
weight: 8
---

## Schema Design

`dbx` is schema-first: database metadata lives in schema structs, entities stay as data carriers.

## When to Use

- You want table structure, constraints, and relations in typed Go declarations.
- You need consistent metadata for query building and migrations.
- You want ID strategy and indexes declared at schema level.

## Minimal Project Layout

```text
.
├── go.mod
├── internal
│   └── schema
│       └── user.go
└── main.go
```

## Complete Example

```go
package main

import (
	columnx "github.com/DaiYuANg/arcgo/dbx/column"
	"github.com/DaiYuANg/arcgo/dbx/idgen"
	relationx "github.com/DaiYuANg/arcgo/dbx/relation"
	schemax "github.com/DaiYuANg/arcgo/dbx/schema"
)

type Role struct {
	ID   int64  `dbx:"id"`
	Name string `dbx:"name"`
}

type User struct {
	ID       int64  `dbx:"id"`
	TenantID int64  `dbx:"tenant_id"`
	RoleID   int64  `dbx:"role_id"`
	Username string `dbx:"username"`
	Email    string `dbx:"email"`
	Status   int    `dbx:"status"`
}

type RoleSchema struct {
	schemax.Schema[Role]
	ID   columnx.IDColumn[Role, int64, idgen.IDSnowflake] `dbx:"id,pk"`
	Name columnx.Column[Role, string]                   `dbx:"name,unique"`
}

type UserSchema struct {
	schemax.Schema[User]
	ID       columnx.IDColumn[User, int64, idgen.IDSnowflake] `dbx:"id,pk"`
	TenantID columnx.Column[User, int64]                    `dbx:"tenant_id,index"`
	RoleID   columnx.Column[User, int64]                    `dbx:"role_id,ref=roles.id,ondelete=cascade,index"`
	Username columnx.Column[User, string]                   `dbx:"username,index"`
	Email    columnx.Column[User, string]                   `dbx:"email,unique"`
	Status   columnx.Column[User, int]                      `dbx:"status,default=1,index"`
	Role     relationx.BelongsTo[User, Role]                  `rel:"table=roles,local=role_id,target=id"`

	// Composite non-unique index: (tenant_id, username)
	Lookup schemax.Index[User] `idx:"columns=tenant_id|username"`

	// Composite unique index: (tenant_id, email)
	UniquePerTenant schemax.Unique[User] `idx:"columns=tenant_id|email"`
}

var Roles = schemax.MustSchema("roles", RoleSchema{})
var Users = schemax.MustSchema("users", UserSchema{})
```

## Declaration Rules

- Use `schemax.Schema[E]` as the first embedded field in each schema struct.
- Use `columnx.Column[E, T]` for regular fields.
- Use relation fields (`BelongsTo`, `HasOne`, `HasMany`, `ManyToMany`) for relation metadata.
- Use `columnx.IDColumn[E, T, Marker]` for explicit PK generation strategy.

## ID Strategy in Schema

Recommended strategy declaration is type-level:

```go
ID columnx.IDColumn[Order, string, idgen.IDULID] `dbx:"id,pk"`
```

See full strategy matrix and runtime generator options in [ID Generation](./id-generation).

## Index Declaration in Schema

- Single-column index via tag: `dbx:"field,index"`
- Single-column unique via tag: `dbx:"field,unique"`
- Composite index via `schemax.Index[E]` + `idx:"columns=..."`
- Composite unique via `schemax.Unique[E]` + `idx:"columns=..."`

See more patterns in [Indexes](./indexes).

## Pitfalls

- Defining duplicated table metadata outside schema leads to drift.
- Missing `rel` tag keys (`table/local/target`) breaks relation load planning.
- Composite index fields must use valid schema column names in `idx:"columns=..."`.

## Verify

```bash
go test ./dbx/...
```
