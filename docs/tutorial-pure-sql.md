---
title: 'Pure SQL Tutorial'
linkTitle: 'tutorial-pure-sql'
description: 'Use dbx SQL helpers with sqltmpl statements'
weight: 16
---

## Pure SQL Tutorial

This tutorial shows how to execute `.sql` templates with `sqltmpl` and `dbx.SQL*` helpers.

## When to Use

- Complex SQL is easier to maintain in `.sql` files.
- You want SQL template reuse and parameterized rendering.
- You want dbx hooks/logging/tx while staying SQL-first.

## Project Layout

```text
.
├── main.go
└── sql
    └── user
        └── find_active.sql
```

Example `sql/user/find_active.sql`:

```sql
SELECT id, username
FROM users
/*%where */
/*%if present(Status) */
  AND status = /* Status */1
/*%end */
/*%end */
ORDER BY id DESC
LIMIT /* Page.Limit */20 OFFSET /* Page.Offset */0
```

## Complete Runnable `main.go`

```go
package main

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"log"

	"github.com/arcgolabs/dbx"
	"github.com/arcgolabs/dbx/dialect/sqlite"
	mapperx "github.com/arcgolabs/dbx/mapper"
	"github.com/arcgolabs/dbx/sqlexec"
	"github.com/arcgolabs/dbx/sqltmpl"

	_ "modernc.org/sqlite"
)

//go:embed sql/**/*.sql
var sqlFS embed.FS

type UserSummary struct {
	ID       int64  `dbx:"id"`
	Username string `dbx:"username"`
}

func main() {
	ctx := context.Background()

	raw, err := sql.Open("sqlite", "file:dbx_pure_sql.db?cache=shared")
	if err != nil {
		log.Fatal(err)
	}
	defer raw.Close()

	core, err := dbx.NewWithOptions(raw, sqlite.New())
	if err != nil {
		log.Fatal(err)
	}

	registry := sqltmpl.NewRegistry(sqlFS, core.Dialect())
	stmt := registry.MustStatement("sql/user/find_active.sql")

	items, err := sqlexec.List[UserSummary](
		ctx,
		core,
		stmt,
		sqltmpl.WithPage(struct {
			Status int `dbx:"status"`
		}{Status: 1}, sqltmpl.Page(1, 20)),
		mapperx.MustStructMapper[UserSummary](),
	)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("active rows=%d\n", items.Len())
}
```

## Startup Precheck

For embedded SQL files, load and check templates once during startup or CI:

```go
registry := sqltmpl.NewRegistry(sqlFS, core.Dialect())

if _, err := registry.PreloadAll(); err != nil {
	return err
}

reports, err := registry.CheckAll(map[string]any{
	"sql/user/find_active.sql": sqltmpl.WithPage(struct {
		Status int `dbx:"status"`
	}{Status: 1}, sqltmpl.Page(1, 20)),
})
if err != nil {
	return err
}

reports.Range(func(_ int, report sqltmpl.CheckReport) bool {
	if report.Err != nil {
		log.Printf("sql template %s failed at %s: %v", report.Name, report.Stage, report.Err)
	}
	return true
})
```

## Pitfalls

- Re-resolving statement text repeatedly in hot loops adds overhead; cache `MustStatement(...)`.
- Parameter names in templates must match struct/map fields.
- Avoid mixing manual string concatenation with template-driven SQL.

## Verify

```bash
go test ./sqltmpl/...
go run .
```
