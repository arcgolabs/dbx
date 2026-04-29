// Package main demonstrates basic dbx CRUD and transaction flows.
package main

import (
	"context"
	"fmt"

	collectionx "github.com/arcgolabs/collectionx/list"
	"github.com/arcgolabs/dbx"
	"github.com/arcgolabs/dbx/examples/internal/shared"
	mapperx "github.com/arcgolabs/dbx/mapper"
	projectionx "github.com/arcgolabs/dbx/projection"
	"github.com/arcgolabs/dbx/querydsl"
	"github.com/arcgolabs/dbx/schemamigrate"
)

func main() {
	ctx := context.Background()
	catalog := shared.NewCatalog()

	core, closeDB := openBasicDB()
	defer closeOrPanic(closeDB)

	prepareBasicData(ctx, core, catalog)

	printActiveUsers(queryActiveUsers(ctx, core, catalog))
	printUserSummaries(queryUserSummaries(ctx, core, catalog))
	updateUserStatus(ctx, core, catalog, "bob", 2)
	printUpdatedStatus(queryUsersByUsername(ctx, core, catalog, "bob"))
	printLine("basic example completed")
}

func openBasicDB() (*dbx.DB, func() error) {
	core, closeDB, err := shared.OpenSQLite(
		"dbx-basic",
		dbx.WithLogger(shared.NewLogger()),
		dbx.WithDebug(true),
		dbx.WithHooks(dbx.HookFuncs{
			AfterFunc: func(_ context.Context, event *dbx.HookEvent) {
				if event.Operation == dbx.OperationAutoMigrate && event.Err == nil {
					printLine("hook: auto_migrate finished")
				}
			},
		}),
	)
	if err != nil {
		panic(err)
	}

	return core, closeDB
}

func prepareBasicData(ctx context.Context, core *dbx.DB, catalog shared.Catalog) {
	_, err := schemamigrate.AutoMigrate(ctx, core, catalog.Roles, catalog.Users, catalog.UserRoles)
	if err != nil {
		panic(err)
	}
	err = shared.SeedDemoData(ctx, core, catalog)
	if err != nil {
		panic(err)
	}
}

func queryActiveUsers(ctx context.Context, core *dbx.DB, catalog shared.Catalog) *collectionx.List[shared.User] {
	userMapper := mapperx.MustMapper[shared.User](catalog.Users)
	users, err := dbx.QueryAll[shared.User](
		ctx,
		core,
		querydsl.Select(querydsl.AllColumns(catalog.Users).Values()...).
			From(catalog.Users).
			Where(catalog.Users.Status.Eq(1)).
			OrderBy(catalog.Users.ID.Asc()),
		userMapper,
	)
	if err != nil {
		panic(err)
	}

	return users
}

func printActiveUsers(users *collectionx.List[shared.User]) {
	printLine("active users:")
	users.Range(func(_ int, user shared.User) bool {
		printFormat("- id=%d username=%s email=%s role_id=%d\n", user.ID, user.Username, user.Email, user.RoleID)
		return true
	})
}

func queryUserSummaries(ctx context.Context, core *dbx.DB, catalog shared.Catalog) *collectionx.List[shared.UserSummary] {
	summaryMapper := mapperx.MustMapper[shared.UserSummary](catalog.Users)
	summaries, err := dbx.QueryAll[shared.UserSummary](
		ctx,
		core,
		projectionx.MustSelect(catalog.Users, summaryMapper).OrderBy(catalog.Users.ID.Asc()),
		summaryMapper,
	)
	if err != nil {
		panic(err)
	}

	return summaries
}

func printUserSummaries(summaries *collectionx.List[shared.UserSummary]) {
	printLine("projected summaries:")
	summaries.Range(func(_ int, summary shared.UserSummary) bool {
		printFormat("- id=%d username=%s email=%s\n", summary.ID, summary.Username, summary.Email)
		return true
	})
}

func updateUserStatus(ctx context.Context, core *dbx.DB, catalog shared.Catalog, username string, status int) {
	tx, err := core.BeginTx(ctx, nil)
	if err != nil {
		panic(err)
	}

	_, err = dbx.Exec(
		ctx,
		tx,
		querydsl.Update(catalog.Users).
			Set(catalog.Users.Status.Set(status)).
			Where(catalog.Users.Username.Eq(username)),
	)
	if err != nil {
		rollbackOrPanic(ctx, tx)
		panic(err)
	}

	commitOrPanic(ctx, tx)
}

func queryUsersByUsername(ctx context.Context, core *dbx.DB, catalog shared.Catalog, username string) *collectionx.List[shared.User] {
	userMapper := mapperx.MustMapper[shared.User](catalog.Users)
	users, err := dbx.QueryAll[shared.User](
		ctx,
		core,
		querydsl.Select(querydsl.AllColumns(catalog.Users).Values()...).
			From(catalog.Users).
			Where(catalog.Users.Username.Eq(username)),
		userMapper,
	)
	if err != nil {
		panic(err)
	}

	return users
}

func printUpdatedStatus(users *collectionx.List[shared.User]) {
	user, ok := users.GetFirst()
	if !ok {
		panic("expected updated user")
	}
	printFormat("bob status after tx update: %d\n", user.Status)
}

func rollbackOrPanic(ctx context.Context, tx *dbx.Tx) {
	err := tx.RollbackContext(ctx)
	if err != nil {
		panic(err)
	}
}

func commitOrPanic(ctx context.Context, tx *dbx.Tx) {
	err := tx.CommitContext(ctx)
	if err != nil {
		panic(err)
	}
}

func closeOrPanic(closeFn func() error) {
	err := closeFn()
	if err != nil {
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
