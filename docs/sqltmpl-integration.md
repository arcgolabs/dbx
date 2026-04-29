---
title: 'sqltmpl Integration'
linkTitle: 'sqltmpl'
description: 'Use sqltmpl with dbx for pure SQL execution'
weight: 12
---

## sqltmpl Integration

`dbx/sqltmpl` is the SQL template renderer. `dbx` handles execution, transaction, hooks, and logging.

## When to Use

- Query logic is easier to maintain in SQL files.
- You want statement reuse and parser validation during development.
- You still want dbx runtime behavior (hooks/logging/tx) for SQL templates.

## Install / Import

```bash
go get github.com/arcgolabs/dbx@latest
go get github.com/arcgolabs/dbx/sqltmpl@latest
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

	raw, err := sql.Open("sqlite", "file:dbx_sqltmpl.db?cache=shared")
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
engine := sqltmpl.New(core.Dialect())

// Disable the compiled-template cache when templates are intentionally one-shot.
engineNoCache := sqltmpl.New(core.Dialect(), sqltmpl.WithTemplateCacheSize(0))
```

For file-backed SQL, prefer `Registry` / `MustStatement` so statement names stay stable for hooks and logs.

## Metadata and Prechecks

Compiled templates expose static metadata. Use this for documentation, CI checks, or startup diagnostics:

```go
template := registry.MustStatement("sql/user/find_active.sql")
metadata := template.Metadata()

fmt.Println(metadata.StatementType)
fmt.Println(metadata.Parameters.Values())
fmt.Println(metadata.SpreadParameters.Values())
fmt.Println(metadata.Conditions.Values())
```

Inline templates can use the same pipeline through `Engine`:

```go
metadata, err := engine.AnalyzeNamed("user/find.sql", `
SELECT id, username
FROM users
WHERE status = /* status */1
`)
if err != nil {
	return err
}

report, err := engine.CheckNamed("user/find.sql", `
SELECT id, username
FROM users
WHERE status = /* status */1
`, struct {
	Status int
}{Status: 1})
if err != nil {
	return err
}

fmt.Println(metadata.StatementType)
fmt.Println(report.Stage)
fmt.Println(report.SQL)
```

For file-backed templates, preload and check the registry at startup:

```go
if _, err := registry.PreloadAll(); err != nil {
	return err
}

reports, err := registry.CheckAll(map[string]any{
	"sql/user/find_active.sql": struct {
		Status int
	}{Status: 1},
})
if err != nil {
	return err
}

reports.Range(func(_ int, report sqltmpl.CheckReport) bool {
	if report.Err != nil {
		fmt.Printf("%s failed at %s: %v\n", report.Name, report.Stage, report.Err)
	}
	return true
})
```

`CheckReport.Stage` is one of `compile`, `load`, `render`, `analyze`, or `ok`.

## Pagination

`sqltmpl` reuses `paging.Request` directly. In SQL templates, bind the normalized page under `Page`:

```sql
SELECT id, username
FROM users
WHERE status = /* status */1
ORDER BY id DESC
LIMIT /* Page.Limit */20 OFFSET /* Page.Offset */0
```

For direct template rendering, use `RenderPage` / `BindPage`:

```go
bound, err := template.RenderPage(params, sqltmpl.Page(1, 20))
```

When executing through `dbx.SQL*`, use `WithPage` to overlay the shared request onto your existing params:

```go
params := sqltmpl.WithPage(struct {
	Status int `dbx:"status"`
}{Status: 1}, sqltmpl.Page(1, 20))
```

## Related Docs

- dbx pure SQL helpers: [dbx](./)
- Runnable example: [examples/pure_sql](https://github.com/arcgolabs/dbx/tree/main/examples/pure_sql)

## Pitfalls

- Calling `registry.MustStatement(...)` repeatedly in hot loops adds avoidable overhead; cache statement once.
- Placeholder names in SQL templates must match bound struct/map fields.
- Avoid mixing ad-hoc SQL string concatenation with template-based rendering.

## Verify

```bash
	go test ./sqltmpl/...
go run .
```
