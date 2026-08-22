---
title: 'Active Record Mode'
linkTitle: 'active-record'
description: 'Thin active-record facade over dbx repository.Base'
weight: 20
---

## Active Record Mode

Package: `github.com/arcgolabs/dbx/activerecord`.

`activerecord` is a small layer on top of `github.com/arcgolabs/dbx/repository`. It wraps entities in `Model` values that delegate persistence to the same `repository.Base` you would use in repository mode. There is no second query engine.

## When to Use

- You want instance-oriented APIs (`Save`, `Reload`, `Delete`) on a loaded or new entity.
- You still want schema-first typing and access to the full repository API via `Store.Repository()`.

## `Store` and `Model`

- `activerecord.New[E](db *dbx.DB, schema S) *Store[E, S]` holds an internal `*repository.Base[E, S]`.
- `activerecord.NewWithOptions[E](db *dbx.DB, schema S, opts ...repository.Option) *Store[E, S]` passes repository options through at construction.
- `Store.Repository() *repository.Base[E, S]` is the escape hatch for bulk ops, specs, transactions, etc.
- `Store.Wrap(entity *E) *Model[E, S]` attaches an entity pointer to the store.
- `Store.FindByKey`, `Store.First`, and `Store.List` return `*Model` values. Errors include `repository.ErrNotFound` when applicable.
- `Store.Find` / `Store.FirstOption` return optional spec-based single-row reads.
- `Store.FindByKeySet` is the typed composite-key helper built from `repository.KeySet`.
- `store.By(Users.ID)` is the typed single-column lookup helper for `Find`, `FindOption`, and `Exists`.
- `store.ListResult(ctx, typedQuery)` and `ScalarResult` execute typed querydsl projections through the underlying repository.
- `Model.Entity() *E`, `Model.Key() repository.Key`: `Key` is a defensive copy of the current primary key map.
- `Model.Save` inserts when key is empty or all key parts are zero; otherwise it updates by key. If update affects no row, it falls back to create for the row-missing case.
- `Model.Reload`, `Model.Delete`: operate by key.

## Optional Finds (`mo.Option`)

Parallel to repository's `*Option` reads:

- `store.By(Users.ID).FindOption(ctx, id)`
- `Store.FindByKeyOption(ctx, key) (mo.Option[*Model[E, S]], error)`
- `Store.Find(ctx, specs...)` / `Store.FirstOption(ctx, specs...)`

When the row is missing, these return `mo.None[*Model[E, S]]()` with `nil` error, matching `repo.By(...).GetOption` / `GetByKeyOption` semantics. Other errors still return a non-nil `error`.

## Complete Example

```go
package main

import (
	"context"
	"database/sql"

	"github.com/arcgolabs/dbx"
	"github.com/arcgolabs/dbx/activerecord"
	columnx "github.com/arcgolabs/dbx/column"
	"github.com/arcgolabs/dbx/dialect/sqlite"
	"github.com/arcgolabs/dbx/idgen"
	"github.com/arcgolabs/dbx/repository"
	"github.com/arcgolabs/dbx/schemamigrate"
	schemax "github.com/arcgolabs/dbx/schema"

	_ "modernc.org/sqlite"
)

type User struct {
	ID   int64  `dbx:"id"`
	Name string `dbx:"name"`
}

type UserSchema struct {
	schemax.Schema[User]
	ID   columnx.IDColumn[User, int64, idgen.IDSnowflake] `dbx:"id,pk"`
	Name columnx.Column[User, string]                     `dbx:"name"`
}

var Users = schemax.MustSchema("users", UserSchema{})

func main() {
	ctx := context.Background()
	raw, _ := sql.Open("sqlite", "file:ar_example.db?cache=shared")
	core := dbx.MustNewWithOptions(raw, sqlite.New())
	_, _ = schemamigrate.AutoMigrate(ctx, core, Users)

	store := activerecord.New[User](core, Users)
	m := store.Wrap(&User{Name: "alice"})
	_ = m.Save(ctx)

	opt, err := store.By(Users.ID).FindOption(ctx, m.Entity().ID)
	if err != nil {
		return
	}
	_, _ = opt.Get()

	byName, _ := store.Find(ctx, repository.Where(Users.Name.Eq("alice")))
	_, _ = byName.Get()
}
```

`FindOption` and `Find` return `mo.Option[*Model[User, UserSchema]]` from `github.com/samber/mo`; add that import if you reference `mo.Some` / `mo.None` explicitly.

For DTO projections that should not be wrapped in `Model`, use the typed query helpers:

```go
type UserSummary struct {
	ID   int64  `dbx:"id"`
	Name string `dbx:"name"`
}

query := querydsl.SelectFromInto[UserSummary](Users, Users.ID, Users.Name).
	Where(Users.Name.Eq("alice"))

rows, err := store.ListResult(ctx, query)
if err != nil {
	return err
}

_, _ = rows.GetFirst()
```

## See Also

- [Repository Mode](./repository) - underlying `repository.Base` API, specs, errors, and `mo.Option` read helpers.
