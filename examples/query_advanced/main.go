// Package main demonstrates advanced dbx query composition examples.
package main

import (
	"context"
	"fmt"

	collectionx "github.com/arcgolabs/collectionx/list"
	"github.com/arcgolabs/dbx"
	"github.com/arcgolabs/dbx/examples/internal/shared"
	mapperx "github.com/arcgolabs/dbx/mapper"
	"github.com/arcgolabs/dbx/querydsl"
	"github.com/arcgolabs/dbx/schemamigrate"
)

type activeUserRow struct {
	ID       int64  `dbx:"id"`
	Username string `dbx:"username"`
}

type activeUsersSource struct {
	querydsl.Table
	ID       querydsl.Column[int64]  `dbx:"id"`
	Username querydsl.Column[string] `dbx:"username"`
}

type labeledUserRow struct {
	ID          int64  `dbx:"id"`
	Username    string `dbx:"username"`
	StatusLabel string `dbx:"status_label"`
}

type unionLabelRow struct {
	Label string `dbx:"label"`
}

func main() {
	ctx := context.Background()
	catalog := shared.NewCatalog()

	core, closeDB, err := shared.OpenSQLite("dbx-query-advanced", dbx.WithLogger(shared.NewLogger()), dbx.WithDebug(true))
	if err != nil {
		panic(err)
	}
	defer closeOrPanic(closeDB)

	_, err = schemamigrate.AutoMigrate(ctx, core, catalog.Roles, catalog.Users, catalog.UserRoles)
	if err != nil {
		panic(err)
	}
	err = shared.SeedDemoData(ctx, core, catalog)
	if err != nil {
		panic(err)
	}

	activeUsers := querydsl.MustSource("active_users", activeUsersSource{})
	activeQuery := querydsl.SelectFrom(activeUsers, activeUsers.ID, activeUsers.Username).
		With("active_users",
			querydsl.SelectFrom(catalog.Users, catalog.Users.ID, catalog.Users.Username).
				Where(catalog.Users.Status.Eq(1)),
		).
		OrderBy(activeUsers.ID.Asc())

	activeRows, err := dbx.QueryAll[activeUserRow](ctx, core, activeQuery, mapperx.MustStructMapper[activeUserRow]())
	if err != nil {
		panic(err)
	}
	printActiveRows(activeRows)

	statusLabel := querydsl.CaseWhen[string](catalog.Users.Status.Eq(1), "active").
		When(catalog.Users.Status.Eq(0), "inactive").
		Else("unknown")
	labeledQuery := querydsl.SelectFrom(
		catalog.Users,
		catalog.Users.ID,
		catalog.Users.Username,
		statusLabel.As("status_label"),
	).OrderBy(catalog.Users.ID.Asc())

	labeledRows, err := dbx.QueryAll[labeledUserRow](ctx, core, labeledQuery, mapperx.MustStructMapper[labeledUserRow]())
	if err != nil {
		panic(err)
	}
	printLabeledRows(labeledRows)

	label := querydsl.Result[string]("label")
	unionQuery := querydsl.SelectFrom(catalog.Users, catalog.Users.Username.As("label")).
		Where(catalog.Users.Status.Eq(1)).
		UnionAll(
			querydsl.SelectFrom(catalog.Roles, catalog.Roles.Name.As("label")),
		).
		OrderBy(label.Asc())

	unionRows, err := dbx.QueryAll[unionLabelRow](ctx, core, unionQuery, mapperx.MustStructMapper[unionLabelRow]())
	if err != nil {
		panic(err)
	}
	printUnionRows(unionRows)
}

func printActiveRows(rows *collectionx.List[activeUserRow]) {
	printLine("cte query:")
	rows.Range(func(_ int, row activeUserRow) bool {
		printFormat("- id=%d username=%s\n", row.ID, row.Username)
		return true
	})
}

func printLabeledRows(rows *collectionx.List[labeledUserRow]) {
	printLine("case query:")
	rows.Range(func(_ int, row labeledUserRow) bool {
		printFormat("- id=%d username=%s status=%s\n", row.ID, row.Username, row.StatusLabel)
		return true
	})
}

func printUnionRows(rows *collectionx.List[unionLabelRow]) {
	printLine("union query:")
	rows.Range(func(_ int, row unionLabelRow) bool {
		printFormat("- label=%s\n", row.Label)
		return true
	})
}

func closeOrPanic(closeFn func() error) {
	if err := closeFn(); err != nil {
		panic(err)
	}
}

func printLine(text string) {
	if _, err := fmt.Println(text); err != nil {
		panic(err)
	}
}

func printFormat(format string, args ...any) {
	if _, err := fmt.Printf(format, args...); err != nil {
		panic(err)
	}
}
