// Package main demonstrates dbx mutation and returning-query patterns.
package main

import (
	"context"
	"fmt"

	"github.com/arcgolabs/collectionx"
	"github.com/arcgolabs/dbx"
	columnx "github.com/arcgolabs/dbx/column"
	"github.com/arcgolabs/dbx/examples/internal/shared"
	mapperx "github.com/arcgolabs/dbx/mapper"
	"github.com/arcgolabs/dbx/querydsl"
	schemax "github.com/arcgolabs/dbx/schema"
	"github.com/arcgolabs/dbx/schemamigrate"
)

type statusSummary struct {
	Status    int   `dbx:"status"`
	UserCount int64 `dbx:"user_count"`
}

type userNameRow struct {
	Username string `dbx:"username"`
}

type userArchive struct {
	ID       int64  `dbx:"id"`
	Username string `dbx:"username"`
	Status   int    `dbx:"status"`
}

type userArchiveSchema struct {
	schemax.Schema[userArchive]
	ID       columnx.Column[userArchive, int64]  `dbx:"id,pk,auto"`
	Username columnx.Column[userArchive, string] `dbx:"username,unique"`
	Status   columnx.Column[userArchive, int]    `dbx:"status"`
}

func main() {
	ctx := context.Background()
	catalog := shared.NewCatalog()
	archive := schemax.MustSchema("user_archive", userArchiveSchema{})

	core, closeDB := openMutationDB()
	defer closeOrPanic(closeDB)

	prepareMutationData(ctx, core, catalog, archive)

	printStatusSummaries(queryStatusSummaries(ctx, core, catalog))
	printUserNameRows("users resolved by subquery + exists:", queryAdminUsers(ctx, core, catalog))

	archiveMapper := mapperx.MustMapper[userArchive](archive)
	printArchiveRows("insert-select returning:", insertArchiveFromSelect(ctx, core, catalog, archive, archiveMapper))
	printArchiveRows("batch insert returning:", batchInsertArchive(ctx, core, archive, archiveMapper))
	printArchiveRows("upsert returning:", upsertArchive(ctx, core, archive, archiveMapper))
}

func openMutationDB() (*dbx.DB, func() error) {
	core, closeDB, err := shared.OpenSQLite(
		"dbx-mutation",
		dbx.WithLogger(shared.NewLogger()),
		dbx.WithDebug(true),
	)
	if err != nil {
		panic(err)
	}

	return core, closeDB
}

func prepareMutationData(ctx context.Context, core *dbx.DB, catalog shared.Catalog, archive userArchiveSchema) {
	_, err := schemamigrate.AutoMigrate(ctx, core, catalog.Roles, catalog.Users, catalog.UserRoles, archive)
	if err != nil {
		panic(err)
	}
	err = shared.SeedDemoData(ctx, core, catalog)
	if err != nil {
		panic(err)
	}
}

func queryStatusSummaries(ctx context.Context, core *dbx.DB, catalog shared.Catalog) collectionx.List[statusSummary] {
	rows, err := dbx.QueryAll[statusSummary](
		ctx,
		core,
		querydsl.Select(
			catalog.Users.Status,
			querydsl.CountAll().As("user_count"),
		).
			From(catalog.Users).
			GroupBy(catalog.Users.Status).
			Having(querydsl.CountAll().Gt(int64(0))).
			OrderBy(catalog.Users.Status.Asc()),
		mapperx.MustStructMapper[statusSummary](),
	)
	if err != nil {
		panic(err)
	}

	return rows
}

func queryAdminUsers(ctx context.Context, core *dbx.DB, catalog shared.Catalog) collectionx.List[userNameRow] {
	adminRoleIDs := querydsl.Select(catalog.Roles.ID).
		From(catalog.Roles).
		Where(catalog.Roles.Name.Eq("admin"))

	rows, err := dbx.QueryAll[userNameRow](
		ctx,
		core,
		querydsl.Select(catalog.Users.Username).
			From(catalog.Users).
			Where(querydsl.And(
				catalog.Users.RoleID.InQuery(adminRoleIDs),
				querydsl.Exists(
					querydsl.Select(catalog.UserRoles.UserID).
						From(catalog.UserRoles).
						Where(catalog.UserRoles.UserID.EqColumn(catalog.Users.ID)).
						Limit(1),
				),
			)),
		mapperx.MustStructMapper[userNameRow](),
	)
	if err != nil {
		panic(err)
	}

	return rows
}

func insertArchiveFromSelect(
	ctx context.Context,
	core *dbx.DB,
	catalog shared.Catalog,
	archive userArchiveSchema,
	archiveMapper mapperx.Mapper[userArchive],
) collectionx.List[userArchive] {
	rows, err := dbx.QueryAll[userArchive](
		ctx,
		core,
		querydsl.InsertInto(archive).
			Columns(archive.Username, archive.Status).
			FromSelect(
				querydsl.Select(catalog.Users.Username, catalog.Users.Status).
					From(catalog.Users).
					Where(catalog.Users.Status.Eq(1)).
					OrderBy(catalog.Users.ID.Asc()),
			).
			Returning(archive.ID, archive.Username, archive.Status),
		archiveMapper,
	)
	if err != nil {
		panic(err)
	}

	return rows
}

func batchInsertArchive(
	ctx context.Context,
	core *dbx.DB,
	archive userArchiveSchema,
	archiveMapper mapperx.Mapper[userArchive],
) collectionx.List[userArchive] {
	rows, err := dbx.QueryAll[userArchive](
		ctx,
		core,
		querydsl.InsertInto(archive).
			Values(
				archive.Username.Set("eve"),
				archive.Status.Set(1),
			).
			Values(
				archive.Username.Set("mallory"),
				archive.Status.Set(0),
			).
			Returning(archive.ID, archive.Username, archive.Status),
		archiveMapper,
	)
	if err != nil {
		panic(err)
	}

	return rows
}

func upsertArchive(
	ctx context.Context,
	core *dbx.DB,
	archive userArchiveSchema,
	archiveMapper mapperx.Mapper[userArchive],
) collectionx.List[userArchive] {
	rows, err := dbx.QueryAll[userArchive](
		ctx,
		core,
		querydsl.InsertInto(archive).
			Values(
				archive.Username.Set("alice"),
				archive.Status.Set(9),
			).
			OnConflict(archive.Username).
			DoUpdateSet(archive.Status.SetExcluded()).
			Returning(archive.ID, archive.Username, archive.Status),
		archiveMapper,
	)
	if err != nil {
		panic(err)
	}

	return rows
}

func printStatusSummaries(rows collectionx.List[statusSummary]) {
	printLine("aggregate status counts:")
	rows.Range(func(_ int, row statusSummary) bool {
		printFormat("- status=%d count=%d\n", row.Status, row.UserCount)
		return true
	})
}

func printUserNameRows(title string, rows collectionx.List[userNameRow]) {
	printLine(title)
	rows.Range(func(_ int, row userNameRow) bool {
		printFormat("- username=%s\n", row.Username)
		return true
	})
}

func printArchiveRows(title string, rows collectionx.List[userArchive]) {
	printLine(title)
	rows.Range(func(_ int, row userArchive) bool {
		printFormat("- id=%d username=%s status=%d\n", row.ID, row.Username, row.Status)
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
