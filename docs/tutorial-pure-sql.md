---
title: 'Pure SQL Tutorial'
linkTitle: 'tutorial-pure-sql'
description: 'Use dbx SQL helpers with sqltmplx statements'
weight: 16
---

## Pure SQL Tutorial

This tutorial shows how to execute `.sql` templates with `sqltmplx` and `dbx.SQL*` helpers.

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

	"github.com/DaiYuANg/arcgo/dbx"
	"github.com/DaiYuANg/arcgo/dbx/dialect/sqlite"
	mapperx "github.com/DaiYuANg/arcgo/dbx/mapper"
	"github.com/DaiYuANg/arcgo/dbx/sqlexec"
	"github.com/DaiYuANg/arcgo/dbx/sqltmplx"

	_ "github.com/mattn/go-sqlite3"
)

//go:embed sql/**/*.sql
var sqlFS embed.FS

type UserSummary struct {
	ID       int64  `dbx:"id"`
	Username string `dbx:"username"`
}

func main() {
	ctx := context.Background()

	raw, err := sql.Open("sqlite3", "file:dbx_pure_sql.db?cache=shared")
	if err != nil {
		log.Fatal(err)
	}
	defer raw.Close()

	core, err := dbx.NewWithOptions(raw, sqlite.New())
	if err != nil {
		log.Fatal(err)
	}

	registry := sqltmplx.NewRegistry(sqlFS, core.Dialect())
	stmt := registry.MustStatement("sql/user/find_active.sql")

	items, err := sqlexec.List(
		ctx,
		core,
		stmt,
		sqltmplx.WithPage(struct {
			Status int `dbx:"status"`
		}{Status: 1}, sqltmplx.Page(1, 20)),
		mapperx.MustStructMapper[UserSummary](),
	)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("active rows=%d\n", items.Len())
}
```

## Pitfalls

- Re-resolving statement text repeatedly in hot loops adds overhead; cache `MustStatement(...)`.
- Parameter names in templates must match struct/map fields.
- Avoid mixing manual string concatenation with template-driven SQL.

## Verify

```bash
go test ./dbx/sqltmplx/...
go run .
```
