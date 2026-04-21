---
title: 'sqltmplx Integration'
linkTitle: 'sqltmplx'
description: 'Use sqltmplx with dbx for pure SQL execution'
weight: 12
---

## sqltmplx Integration

`dbx/sqltmplx` is the SQL template renderer. `dbx` handles execution, transaction, hooks, and logging.

## When to Use

- Query logic is easier to maintain in SQL files.
- You want statement reuse and parser validation during development.
- You still want dbx runtime behavior (hooks/logging/tx) for SQL templates.

## Install / Import

```bash
go get github.com/DaiYuANg/arcgo/dbx@latest
go get github.com/DaiYuANg/arcgo/dbx/sqltmplx@latest
```

## Template features (quick reference)

- `/*%if expr */ ... /*%end */`
- `/*%where */ ... /*%end */`
- `/*%set */ ... /*%end */`
- Doma-style placeholders: `/* Name */'alice'`
- Slice expansion: `/* IDs */(1, 2, 3)`
- Expression helpers: `empty(x)`, `blank(x)`, `present(x)`
- Struct binding by field name first, then `sqltmpl`, `db`, `json` aliases
- Shared pagination helpers: `WithPage`, `RenderPage`, `BindPage` with `Page.Limit` / `Page.Offset`

## Minimal Project Layout

```text
.
├── go.mod
├── main.go
└── sql
    └── user
        └── find_active.sql
```

## Complete Example

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
)

//go:embed sql/**/*.sql
var sqlFS embed.FS

type UserSummary struct {
	ID       int64  `dbx:"id"`
	Username string `dbx:"username"`
}

func main() {
	ctx := context.Background()

	raw, err := sql.Open("sqlite3", "file:dbx_sqltmplx.db?cache=shared")
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

	fmt.Printf("rows=%d\n", items.Len())
}
```

## When to Use It

- SQL is complex and easier to maintain as `.sql` files.
- You want parser-backed SQL validation in development.
- You still want dbx execution hooks/logging/transactions.

## Template Cache

`Engine.Render` and `Engine.Compile` use a compiled-template LRU cache by default. The default cache size is 128 entries, keyed by template name and text. This makes repeated rendering of the same inline template close to precompiled `Template.Render` without changing call sites.

```go
engine := sqltmplx.New(core.Dialect())

// Disable the compiled-template cache when templates are intentionally one-shot.
engineNoCache := sqltmplx.New(core.Dialect(), sqltmplx.WithTemplateCacheSize(0))
```

For file-backed SQL, prefer `Registry` / `MustStatement` so statement names stay stable for hooks and logs.

## Pagination

`sqltmplx` reuses `paging.Request` directly. In SQL templates, bind the normalized page under `Page`:

```sql
SELECT id, username
FROM users
WHERE status = /* status */1
ORDER BY id DESC
LIMIT /* Page.Limit */20 OFFSET /* Page.Offset */0
```

For direct template rendering, use `RenderPage` / `BindPage`:

```go
bound, err := template.RenderPage(params, sqltmplx.Page(1, 20))
```

When executing through `dbx.SQL*`, use `WithPage` to overlay the shared request onto your existing params:

```go
params := sqltmplx.WithPage(struct {
	Status int `dbx:"status"`
}{Status: 1}, sqltmplx.Page(1, 20))
```

## Related Docs

- dbx pure SQL helpers: [dbx](./)
- Runnable examples (repository):
  - [examples/sqltmplx/basic](https://github.com/DaiYuANg/arcgo/tree/main/examples/sqltmplx/basic)
  - [examples/sqltmplx/postgres](https://github.com/DaiYuANg/arcgo/tree/main/examples/sqltmplx/postgres)
  - [examples/sqltmplx/sqlite_update](https://github.com/DaiYuANg/arcgo/tree/main/examples/sqltmplx/sqlite_update)
  - [examples/sqltmplx/precompile](https://github.com/DaiYuANg/arcgo/tree/main/examples/sqltmplx/precompile)

## Pitfalls

- Calling `registry.MustStatement(...)` repeatedly in hot loops adds avoidable overhead; cache statement once.
- Placeholder names in SQL templates must match bound struct/map fields.
- Avoid mixing ad-hoc SQL string concatenation with template-based rendering.

## Verify

```bash
go test ./dbx/sqltmplx/...
go run .
```
