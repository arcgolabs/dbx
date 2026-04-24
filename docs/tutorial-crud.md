---
title: 'CRUD Tutorial'
linkTitle: 'tutorial-crud'
description: 'End-to-end CRUD tutorial with complete runnable dbx code'
weight: 13
---

## CRUD Tutorial

This page shows a full CRUD flow with `dbx`: create table, insert, query, update, delete.

## When to Use

- You need a baseline CRUD implementation with typed schema APIs.
- You want one file that demonstrates write/read/update/delete flow.
- You want a migration-safe first example for onboarding.

## Minimal Project Layout

```text
.
├── go.mod
└── main.go
```

## Complete Runnable Example

```go
package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"

	"github.com/arcgolabs/dbx"
	columnx "github.com/arcgolabs/dbx/column"
	"github.com/arcgolabs/dbx/dialect/sqlite"
	"github.com/arcgolabs/dbx/idgen"
	mapperx "github.com/arcgolabs/dbx/mapper"
	"github.com/arcgolabs/dbx/querydsl"
	"github.com/arcgolabs/dbx/schemamigrate"
	schemax "github.com/arcgolabs/dbx/schema"

	_ "modernc.org/sqlite"
)

type User struct {
	ID       int64  `dbx:"id"`
	Username string `dbx:"username"`
	Email    string `dbx:"email"`
	Status   int    `dbx:"status"`
}

type UserSchema struct {
	schemax.Schema[User]
	ID       columnx.IDColumn[User, int64, idgen.IDSnowflake] `dbx:"id,pk"`
	Username columnx.Column[User, string]                   `dbx:"username,index"`
	Email    columnx.Column[User, string]                   `dbx:"email,unique"`
	Status   columnx.Column[User, int]                      `dbx:"status,default=1,index"`
}

var Users = schemax.MustSchema("users", UserSchema{})

func main() {
	ctx := context.Background()
	raw, err := sql.Open("sqlite", "file:dbx_crud.db?cache=shared")
	if err != nil {
		log.Fatal(err)
	}
	defer raw.Close()

	core, err := dbx.NewWithOptions(raw, sqlite.New(), dbx.WithDebug(true))
	if err != nil {
		log.Fatal(err)
	}
	if _, err := schemamigrate.AutoMigrate(ctx, core, Users); err != nil {
		log.Fatal(err)
	}

	mapper := mapperx.MustMapper[User](Users)

	// Create
	u := &User{Username: "alice", Email: "alice@example.com", Status: 1}
	assignments, err := mapper.InsertAssignments(core, Users, u)
	if err != nil {
		log.Fatal(err)
	}
	if _, err := dbx.Exec(ctx, core, querydsl.InsertInto(Users).Values(assignments.Values()...)); err != nil {
		log.Fatal(err)
	}

	// Read
	list, err := dbx.QueryAll[User](
		ctx, core,
		querydsl.Select(querydsl.AllColumns(Users).Values()...).From(Users).Where(Users.Username.Eq("alice")),
		mapper,
	)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("query rows=%d\n", list.Len())

	// Update
	if _, err := dbx.Exec(
		ctx, core,
		querydsl.Update(Users).
			Set(Users.Status.Set(2)).
			Where(Users.Username.Eq("alice")),
	); err != nil {
		log.Fatal(err)
	}

	// Delete
	if _, err := dbx.Exec(ctx, core, querydsl.DeleteFrom(Users).Where(Users.Username.Eq("alice"))); err != nil {
		log.Fatal(err)
	}
}
```

## Notes

- Insert auto-ID generation comes from schema marker + DB runtime generator.
- Query/Update/Delete are all typed through schema columns.
- For high-frequency read queries, use `Build` once and execute bound query many times.

## Pitfalls

- Missing unique/index constraints in schema can cause accidental duplicates or slow lookups.
- Skipping error checks from `InsertAssignments` hides ID-generation issues.
- Rebuilding the same query in tight loops instead of bound-query reuse increases CPU overhead.

## Verify

```bash
go test ./...
go run .
```
