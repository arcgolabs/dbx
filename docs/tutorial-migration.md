---
title: 'Migration Tutorial'
linkTitle: 'tutorial-migration'
description: 'Plan schema changes, preview SQL, and execute migrations'
weight: 15
---

## Migration Tutorial

This tutorial covers planning, SQL preview, validation, and auto-migrate.

## When to Use

- You need deterministic visibility into DDL before rollout.
- You want CI-level schema compatibility checks.
- You want conservative auto-migration for additive changes.

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

	"github.com/DaiYuANg/arcgo/dbx"
	columnx "github.com/DaiYuANg/arcgo/dbx/column"
	"github.com/DaiYuANg/arcgo/dbx/dialect/sqlite"
	"github.com/DaiYuANg/arcgo/dbx/idgen"
	"github.com/DaiYuANg/arcgo/dbx/schemamigrate"
	schemax "github.com/DaiYuANg/arcgo/dbx/schema"

	_ "github.com/mattn/go-sqlite3"
)

type User struct {
	ID       int64  `dbx:"id"`
	Username string `dbx:"username"`
	Email    string `dbx:"email"`
}

type UserSchema struct {
	schemax.Schema[User]
	ID       columnx.IDColumn[User, int64, idgen.IDSnowflake] `dbx:"id,pk"`
	Username columnx.Column[User, string]                   `dbx:"username,index"`
	Email    columnx.Column[User, string]                   `dbx:"email,unique"`
}

var Users = schemax.MustSchema("users", UserSchema{})

func main() {
	ctx := context.Background()
	raw, err := sql.Open("sqlite3", "file:dbx_migrate.db?cache=shared")
	if err != nil {
		log.Fatal(err)
	}
	defer raw.Close()

	core, err := dbx.NewWithOptions(raw, sqlite.New())
	if err != nil {
		log.Fatal(err)
	}

	plan, err := schemamigrate.PlanSchemaChanges(ctx, core, Users)
	if err != nil {
		log.Fatal(err)
	}
	plan.SQLPreview().Range(func(_ int, sqlText string) bool {
		fmt.Println(sqlText)
		return true
	})

	if _, err := schemamigrate.ValidateSchemas(ctx, core, Users); err != nil {
		fmt.Println("validate before migrate:", err)
	}

	if _, err := schemamigrate.AutoMigrate(ctx, core, Users); err != nil {
		log.Fatal(err)
	}
}
```

## Pitfalls

- Treating `AutoMigrate` as a destructive migration engine is risky; keep manual migrations for breaking changes.
- Skipping `PlanSchemaChanges().SQLPreview()` reduces deploy confidence.
- Not validating against production-like snapshots can hide dialect-specific differences.

## Verify

```bash
go test ./dbx/... -run Migrate
go run .
```
