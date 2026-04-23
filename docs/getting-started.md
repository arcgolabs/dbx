---
title: 'dbx Getting Started'
linkTitle: 'getting-started'
description: 'Build and run your first dbx app'
weight: 7
---

## Getting Started

This guide shows a complete, runnable dbx example from schema definition to query execution.

## When to Use

- You are starting a new service with `database/sql`.
- You want schema-first metadata with typed query APIs.
- You want one minimal runnable sample as baseline.

## Minimal Project Layout

```text
.
├── go.mod
└── main.go
```

## 1) Install Dependencies

```bash
go get github.com/arcgolabs/dbx
go get github.com/arcgolabs/dbx/dialect/sqlite
go get github.com/mattn/go-sqlite3
```

## 2) Create `main.go`

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

	_ "github.com/mattn/go-sqlite3"
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

	raw, err := sql.Open("sqlite3", "file:dbx_getting_started.db?cache=shared")
	if err != nil {
		log.Fatal(err)
	}
	defer raw.Close()

	core, err := dbx.NewWithOptions(
		raw,
		sqlite.New(),
		dbx.WithDebug(true),
	)
	if err != nil {
		log.Fatal(err)
	}

	// Create/align table structure based on schema metadata.
	if _, err := schemamigrate.AutoMigrate(ctx, core, Users); err != nil {
		log.Fatal(err)
	}

	mapper := mapperx.MustMapper[User](Users)
	alice := &User{
		Username: "alice",
		Email:    "alice@example.com",
		Status:   1,
	}

	assignments, err := mapper.InsertAssignments(core, Users, alice)
	if err != nil {
		log.Fatal(err)
	}

	if _, err := dbx.Exec(ctx, core, querydsl.InsertInto(Users).Values(assignments.Values()...)); err != nil {
		log.Fatal(err)
	}

	items, err := dbx.QueryAll(
		ctx,
		core,
		querydsl.Select(querydsl.AllColumns(Users).Values()...).From(Users).Where(Users.Status.Eq(1)),
		mapper,
	)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("active users: %d\n", items.Len())
	items.Range(func(_ int, item User) bool {
		fmt.Printf("id=%d username=%s email=%s status=%d\n", item.ID, item.Username, item.Email, item.Status)
		return true
	})
}
```

## 3) Run

```bash
go run .
```

## Pitfalls

- Forgetting `AutoMigrate` before first write often causes "no such table" errors.
- Mixing schema metadata across multiple structs for one table creates confusion; keep one schema source.
- Using `dbx.WithNodeID` and `dbx.WithIDGenerator` together is invalid.

## Verify

```bash
go test ./...
go run .
```

## Next Steps

- ID strategy and production guidance: [ID Generation](./id-generation)
- Runtime options: [Options](./options)
- Logging and hooks: [Observability](./observability)
- Full runnable examples: [Examples](./examples)
