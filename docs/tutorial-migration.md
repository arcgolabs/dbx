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

	"github.com/arcgolabs/dbx"
	columnx "github.com/arcgolabs/dbx/column"
	"github.com/arcgolabs/dbx/migrate"
	"github.com/arcgolabs/dbx/dialect/sqlite"
	"github.com/arcgolabs/dbx/idgen"
	"github.com/arcgolabs/dbx/schemamigrate"
	schemax "github.com/arcgolabs/dbx/schema"

	_ "modernc.org/sqlite"
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
	raw, err := sql.Open("sqlite", "file:dbx_migrate.db?cache=shared")
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

## Flyway-Style SQL / Programmatic Go Migrations

dbx supports both SQL-script migration and Go callback migration in the same runner abstraction.

```go
func runMigrations(ctx context.Context, core *dbx.DB) error {
	runner := migrate.NewRunner(core.SQLDB(), core.Dialect(), migrate.RunnerOptions{
		HistoryTable: "schema_history",
		ValidateHash: true,
	})

	// Go migrations: apply exactly up to version 3.
	if _, err := runner.UpGoTo(ctx, 3,
		migrate.NewGoMigration("1", "create users", upUsers, downUsers),
		migrate.NewGoMigration("2", "seed roles", upRoles, nil, migrate.DialectSQLite), // only runs on sqlite
		migrate.NewGoMigration("3", "add constraints", upConstraints, downConstraints, migrate.DialectMySQL, migrate.DialectPostgres),
	); err != nil {
		return err
	}

	// SQL migrations from embed.FS.
	// Supported naming: V2__seed_users.sql (all dialects),
	// V2__seed_users_sqlite.sql, or V2__seed_users__sqlite.sql (only sqlite).
	source := migrate.FileSource{
		FS:  sqlFS,
		Dir: "migrations",
	}
	if _, err := runner.UpSQLToFor(ctx, 2, migrate.DialectSQLite, source); err != nil {
		return err
	}
	return nil
}

func downMigrations(ctx context.Context, core *dbx.DB) error {
	runner := migrate.NewRunner(core.SQLDB(), core.Dialect(), migrate.RunnerOptions{ValidateHash: true})

	// Roll back SQL migrations down to version 0 (all versioned migrations).
	if _, err := runner.DownSQLTo(ctx, 0, migrate.FileSource{FS: sqlFS, Dir: "migrations"}); err != nil {
		return err
	}

	// Roll back Go migrations to version 1.
	_, err := runner.DownGoTo(ctx, 1,
		migrate.NewGoMigration("1", "create users", upUsers, downUsers),
		migrate.NewGoMigration("2", "seed roles", upRoles, nil),
		migrate.NewGoMigration("3", "add constraints", upConstraints, downConstraints),
	)
	return err
}
```

You can also inspect both Go and SQL status in one pass before applying.

```go
status, err := runner.StatusAll(ctx,
	[]migrate.Migration{
		migrate.NewGoMigration("1", "create users", upUsers, downUsers),
	},
	&migrate.FileSource{FS: sqlFS, Dir: "migrations"},
)
if err != nil {
	return err
}

for i := range status.Go.Len() {
	item, ok := status.Go.Get(i)
	if !ok {
		continue
	}
	if item.State != migrate.MigrationStateApplied {
		fmt.Printf("go migration %s: %s\\n", item.Version, item.State)
	}
}
for i := range status.SQL.Len() {
	item, ok := status.SQL.Get(i)
	if !ok {
		continue
	}
	if item.State != migrate.MigrationStateApplied {
		fmt.Printf("sql migration %s (%s): %s\\n", item.Version, item.Description, item.State)
	}
}
```

You can also use `PendingGo`, `PendingSQL`, `PendingAll` for rollout orchestration.

For teams that want a single call for both sources, `ApplyAll` is available as an orchestration helper.
It delegates to the low-level `Up*/Down*/Status*` methods and keeps those APIs unchanged.
`ValidateApplyAll` lets you validate direction/target parameters before executing.

```go
_, err := runner.ApplyAll(ctx, migrate.MigrationApplySpec{
	Direction: migrate.DirectionUp,
	GoMigrations: []migrate.Migration{
		migrate.NewGoMigration("1", "create users", upUsers, downUsers),
	},
	SQLSource: &migrate.FileSource{FS: sqlFS, Dir: "migrations"},
})
if err != nil {
	return err
}

if err := runner.ValidateApplyAll(migrate.MigrationApplySpec{
	Direction: migrate.DirectionUp,
	Target:    &migrate.MigrationTarget{Version: 1},
	GoMigrations: []migrate.Migration{
		migrate.NewGoMigration("1", "create users", upUsers, downUsers),
	},
	SQLSource: &migrate.FileSource{FS: sqlFS, Dir: "migrations"},
}); err != nil {
	return err
}
_, err = runner.ApplyAll(ctx, migrate.MigrationApplySpec{
	Direction: migrate.DirectionUp,
	Target:    &migrate.MigrationTarget{Version: 1},
	GoMigrations: []migrate.Migration{
		migrate.NewGoMigration("1", "create users", upUsers, downUsers),
	},
	SQLSource: &migrate.FileSource{FS: sqlFS, Dir: "migrations"},
})
if err != nil {
	return err
}
```

## Pitfalls

- Treating `AutoMigrate` as a destructive migration engine is risky; keep manual migrations for breaking changes.
- Skipping `PlanSchemaChanges().SQLPreview()` reduces deploy confidence.
- Not validating against production-like snapshots can hide dialect-specific differences.
- `schemamigrate` is intended for schema synchronization during development, while `migrate` is the durable path for explicit SQL/Go migration history.

## Verify

```bash
go test ./... -run Migrate
go run .
```
