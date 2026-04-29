---
title: 'Repository Mode'
linkTitle: 'repository'
description: 'Generic repository abstraction on top of dbx schema-first core'
weight: 19
---

## Repository Mode

`dbx/repository` is a thin abstraction over `dbx` core APIs. It keeps schema-first typing while offering service-friendly CRUD methods.

## When to Use

- You prefer explicit domain repositories over active-record style methods.
- You want transaction boundaries and query behavior centralized per aggregate.
- You need reusable CRUD, pagination, upsert, and spec-based filtering.

## Complete Example

```go
package main

import (
	"context"
	"database/sql"

	"github.com/arcgolabs/dbx"
	columnx "github.com/arcgolabs/dbx/column"
	"github.com/arcgolabs/dbx/dialect/sqlite"
	"github.com/arcgolabs/dbx/idgen"
	"github.com/arcgolabs/dbx/repository"
	"github.com/arcgolabs/dbx/schemamigrate"
	schemax "github.com/arcgolabs/dbx/schema"
	"github.com/arcgolabs/dbx/sqltmpl"

	_ "modernc.org/sqlite"
)

type User struct {
	ID   int64  `dbx:"id"`
	Name string `dbx:"name"`
}

type UserSchema struct {
	schemax.Schema[User]
	ID   columnx.IDColumn[User, int64, idgen.IDSnowflake] `dbx:"id,pk"`
	Name columnx.Column[User, string]                   `dbx:"name,index"`
}

var Users = schemax.MustSchema("users", UserSchema{})

func main() {
	ctx := context.Background()
	raw, _ := sql.Open("sqlite", "file:repo_example.db?cache=shared")
	core := dbx.MustNewWithOptions(raw, sqlite.New())
	_, _ = schemamigrate.AutoMigrate(ctx, core, Users)

	repo := repository.NewWithOptions[User](core, Users, repository.WithByIDNotFoundAsError(true))
	_ = repo.CreateMany(ctx, &User{Name: "alice"}, &User{Name: "bob"})
	_ = repo.Upsert(ctx, &User{ID: 1, Name: "alice-v2"})
	page, _ := repo.ListPageSpecRequest(ctx, sqltmpl.Page(1, 20), repository.Where(Users.Name.Eq("alice-v2")))
	_ = page.HasNext
}
```

## API Highlights

- CRUD: `Create`, `CreateMany`, `List`, `First`, `Update`, `Delete`
- PK helpers: `GetByID`, `UpdateByID`, `DeleteByID`
- Composite key helpers: `GetByKey`, `UpdateByKey`, `DeleteByKey`
- Pagination: `paging.Request`, `paging.Result`, `ListPage`, `ListPageRequest`, `ListPageSpec`, `ListPageSpecRequest`
- Upsert: `Upsert(ctx, entity, conflictColumns...)`
- Transactions: `InTx`
- Specs: `Where`, `OrderBy`, `Limit`, `Offset`, `Page`, `PageByRequest`
- Optional single-row reads: `GetByIDOption`, `GetByKeyOption`, `FirstOption`, `FirstSpecOption` (see below)

## Pagination

Use `paging.Page(page, pageSize)` or `paging.NewRequest(page, pageSize)` when you need one pagination model across repository, active-record, and template SQL code paths.

```go
request := sqltmpl.Page(1, 20)

page, err := repo.ListPageSpecRequest(
	ctx,
	request,
	repository.Where(Users.Name.Eq("alice")),
	repository.OrderBy(Users.ID.Desc()),
)
if err != nil {
	return err
}

_ = page.Items
_ = page.TotalPages
_ = page.HasNext
```

`ListPage(ctx, query, page, pageSize)` and `ListPageSpec(ctx, page, pageSize, specs...)` remain available for direct page/size calls. The `*Request` variants are preferred when the page request is passed through service boundaries or reused by `sqltmpl`.

## Optional reads (`mo.Option`)

For “maybe one row” queries, you can use parallel methods that return `github.com/samber/mo.Option` instead of treating “not found” as `repository.ErrNotFound`:

- `GetByIDOption`, `GetByKeyOption`, `FirstOption`, `FirstSpecOption`

Semantics:

- When the underlying `GetByID` / `GetByKey` / `First` would return `repository.ErrNotFound`, the `Option` variant returns `mo.None[E]()` and **no error** (`error == nil`).
- Any other failure (invalid key, DB error, etc.) still returns a non-nil `error` and `mo.None[E]()`.
- Empty composite `Key{}` still surfaces as `repository.ValidationError` from `GetByKey` / `GetByKeyOption` (not folded into `Option`).

When to use which:

- Prefer `GetByID` / `First` + `errors.Is(err, repository.ErrNotFound)` when not-found is exceptional or you want uniform `if err != nil` handling.
- Prefer `*Option` when absence is a normal outcome and you want `(mo.Option[E], error)` to separate “missing row” from “real errors”, consistent with `sqlexec.Find` / `sqlexec.ScalarOption`.

```go
// Assumes User, UserSchema, Users, repo, and ctx from the "Complete Example" above.

byID, err := repo.GetByIDOption(ctx, int64(42))
if err != nil {
	return err
}
if user, ok := byID.Get(); ok {
	_ = user.Name
}

byName, err := repo.FirstSpecOption(ctx, repository.Where(Users.Name.Eq("alice")))
if err != nil {
	return err
}
_, _ = byName.Get()
```

## Error Model

- `ErrNotFound`
- `ErrConflict` (`ConflictError`)
- `ErrValidation` (`ValidationError`)
- `ErrVersionConflict` (`VersionConflictError`)

`WithByIDNotFoundAsError(true)` enables strict by-id mutation semantics (`RowsAffected=0 => ErrNotFound`).
