// Package main demonstrates dbx SQL template usage with handwritten SQL.
package main

import (
	"context"
	"embed"
	"fmt"

	collectionx "github.com/arcgolabs/collectionx/list"
	"github.com/arcgolabs/dbx"
	"github.com/arcgolabs/dbx/examples/internal/shared"
	mapperx "github.com/arcgolabs/dbx/mapper"
	"github.com/arcgolabs/dbx/schemamigrate"
	"github.com/arcgolabs/dbx/sqlexec"
	"github.com/arcgolabs/dbx/sqltmpl"
)

//go:embed sql/**/*.sql
var sqlFS embed.FS

func main() {
	ctx := context.Background()
	catalog := shared.NewCatalog()

	core, closeDB, registry := openPureSQLDB()
	defer closeOrPanic(closeDB)

	preparePureSQLData(ctx, core, catalog)
	printActiveUsers(runActiveUserQuery(ctx, core, registry))
	printActiveCount(runActiveUserCount(ctx, core, registry))
	updatePureSQLUserStatus(ctx, core, registry, "bob", 2)
	printUpdatedUser(runUserByUsername(ctx, core, registry, "bob"))
}

func openPureSQLDB() (*dbx.DB, func() error, *sqltmpl.Registry) {
	core, closeDB, err := shared.OpenSQLite(
		"dbx-pure-sql",
		dbx.WithLogger(shared.NewLogger()),
		dbx.WithDebug(true),
	)
	if err != nil {
		panic(err)
	}

	return core, closeDB, sqltmpl.NewRegistry(sqlFS, core.Dialect())
}

func preparePureSQLData(ctx context.Context, core *dbx.DB, catalog shared.Catalog) {
	_, err := schemamigrate.AutoMigrate(ctx, core, catalog.Roles, catalog.Users, catalog.UserRoles)
	if err != nil {
		panic(err)
	}
	err = shared.SeedDemoData(ctx, core, catalog)
	if err != nil {
		panic(err)
	}
}

func runActiveUserQuery(ctx context.Context, core *dbx.DB, registry *sqltmpl.Registry) *collectionx.List[shared.UserSummary] {
	users, err := sqlexec.List[shared.UserSummary](
		ctx,
		core,
		registry.MustStatement("sql/user/find_active.sql"),
		sqltmpl.WithPage(struct {
			Status int `dbx:"status"`
		}{Status: 1}, sqltmpl.Page(1, 20)),
		mapperx.MustStructMapper[shared.UserSummary](),
	)
	if err != nil {
		panic(err)
	}

	return users
}

func printActiveUsers(users *collectionx.List[shared.UserSummary]) {
	printLine("active users from pure sql:")
	users.Range(func(_ int, user shared.UserSummary) bool {
		printFormat("- id=%d username=%s email=%s\n", user.ID, user.Username, user.Email)
		return true
	})
}

func runActiveUserCount(ctx context.Context, core *dbx.DB, registry *sqltmpl.Registry) int64 {
	total, err := sqlexec.Scalar[int64](
		ctx,
		core,
		registry.MustStatement("sql/user/count_by_status.sql"),
		struct {
			Status int `dbx:"status"`
		}{Status: 1},
	)
	if err != nil {
		panic(err)
	}

	return total
}

func printActiveCount(total int64) {
	printFormat("active user count: %d\n", total)
}

func updatePureSQLUserStatus(ctx context.Context, core *dbx.DB, registry *sqltmpl.Registry, username string, status int) {
	tx, err := core.BeginTx(ctx, nil)
	if err != nil {
		panic(err)
	}

	_, err = tx.SQL().Exec(
		ctx,
		registry.MustStatement("sql/user/update_status.sql"),
		struct {
			Status   int    `dbx:"status"`
			Username string `dbx:"username"`
		}{
			Status:   status,
			Username: username,
		},
	)
	if err != nil {
		rollbackOrPanic(ctx, tx)
		panic(err)
	}

	commitOrPanic(ctx, tx)
}

func runUserByUsername(ctx context.Context, core *dbx.DB, registry *sqltmpl.Registry, username string) shared.User {
	user, err := sqlexec.Get[shared.User](
		ctx,
		core,
		registry.MustStatement("sql/user/find_by_username.sql"),
		struct {
			Username string `dbx:"username"`
		}{Username: username},
		mapperx.MustStructMapper[shared.User](),
	)
	if err != nil {
		panic(err)
	}

	return user
}

func printUpdatedUser(user shared.User) {
	printFormat("bob status after pure sql update: %d\n", user.Status)
}

func rollbackOrPanic(ctx context.Context, tx *dbx.Tx) {
	if err := tx.RollbackContext(ctx); err != nil {
		panic(err)
	}
}

func commitOrPanic(ctx context.Context, tx *dbx.Tx) {
	if err := tx.CommitContext(ctx); err != nil {
		panic(err)
	}
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
