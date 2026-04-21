---
title: 'Active Record Mode'
linkTitle: 'active-record'
description: 'Thin active-record facade over dbx repository.Base'
weight: 20
---

## Active Record Mode

Package: `github.com/DaiYuANg/arcgo/dbx/activerecord`.

`activerecord` is a small layer on top of `github.com/DaiYuANg/arcgo/dbx/repository`. It wraps entities in `Model` values that delegate persistence to the same `repository.Base` you would use in repository mode—no second query engine.

## When to Use

- You want instance-oriented APIs (`Save`, `Reload`, `Delete`) on a loaded or new entity.
- You still want schema-first typing and access to the full repository API via `Store.Repository()`.

## `Store` and `Model`

- `activerecord.New[E](db *dbx.DB, schema S) *Store[E, S]` — holds an internal `*repository.Base[E, S]`.
- `Store.Repository() *repository.Base[E, S]` — escape hatch for bulk ops, specs, transactions, etc.
- `Store.Wrap(entity *E) *Model[E, S]` — attach an entity pointer to the store.
- `Store.FindByID`, `Store.FindByKey`, `Store.List` — return `*Model` (errors include `repository.ErrNotFound` when applicable).
- `Model.Entity() *E`, `Model.Key() repository.Key` — `Key` is a defensive copy of the current primary key map.
- `Model.Save` — insert when key is empty or all key parts are zero; otherwise update by key (if update affects no row, falls back to create for the “row missing” case).
- `Model.Reload`, `Model.Delete` — by key.

## Optional finds (`mo.Option`)

Parallel to repository’s `*Option` reads:

- `Store.FindByIDOption(ctx, id) (mo.Option[*Model[E, S]], error)`
- `Store.FindByKeyOption(ctx, key) (mo.Option[*Model[E, S]], error)`

When the row is missing, these return `mo.None[*Model[E, S]]()` with `nil` error, matching `repository.GetByIDOption` / `GetByKeyOption` semantics. Other errors still return a non-nil `error`.

## Complete Example

```go
package main

import (
	"context"
	"database/sql"

	"github.com/DaiYuANg/arcgo/dbx"
	"github.com/DaiYuANg/arcgo/dbx/activerecord"
	columnx "github.com/DaiYuANg/arcgo/dbx/column"
	"github.com/DaiYuANg/arcgo/dbx/dialect/sqlite"
	"github.com/DaiYuANg/arcgo/dbx/idgen"
	"github.com/DaiYuANg/arcgo/dbx/repository"
	"github.com/DaiYuANg/arcgo/dbx/schemamigrate"
	schemax "github.com/DaiYuANg/arcgo/dbx/schema"
)

type User struct {
	ID   int64  `dbx:"id"`
	Name string `dbx:"name"`
}

type UserSchema struct {
	schemax.Schema[User]
	ID   columnx.IDColumn[User, int64, idgen.IDSnowflake] `dbx:"id,pk"`
	Name columnx.Column[User, string] `dbx:"name"`
}

var Users = schemax.MustSchema("users", UserSchema{})

func main() {
	ctx := context.Background()
	raw, _ := sql.Open("sqlite3", "file:ar_example.db?cache=shared")
	core := dbx.MustNewWithOptions(raw, sqlite.New())
	_, _ = schemamigrate.AutoMigrate(ctx, core, Users)

	store := activerecord.New[User](core, Users)
	m := store.Wrap(&User{Name: "alice"})
	_ = m.Save(ctx)

	opt, err := store.FindByIDOption(ctx, m.Entity().ID)
	if err != nil {
		return
	}
	_, _ = opt.Get()

	_, _ = store.Repository().ListSpec(ctx, repository.Where(Users.Name.Eq("alice")))
}
```

`FindByIDOption` returns `mo.Option[*Model[User, UserSchema]]` from `github.com/samber/mo`; add that import if you reference `mo.Some` / `mo.None` explicitly.

## See Also

- [Repository Mode](./repository) — underlying `repository.Base` API, specs, errors, and `mo.Option` read helpers.
