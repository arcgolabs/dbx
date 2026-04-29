package sqlite_test

import (
	"context"
	"database/sql"
	schemax "github.com/arcgolabs/dbx/schema"
	"reflect"
	"testing"

	collectionx "github.com/arcgolabs/collectionx/list"
	"github.com/arcgolabs/dbx"
	sqlitedialect "github.com/arcgolabs/dbx/dialect/sqlite"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

func TestBuildCreateTable(t *testing.T) {
	bound, err := sqlitedialect.New().BuildCreateTable(schemax.TableSpec{
		Name: "users",
		Columns: collectionx.NewList[schemax.ColumnMeta](
			schemax.ColumnMeta{Name: "id", Table: "users", GoType: reflect.TypeFor[int64](), PrimaryKey: true, AutoIncrement: true},
			schemax.ColumnMeta{Name: "username", Table: "users", GoType: reflect.TypeFor[string]()},
			schemax.ColumnMeta{Name: "email_address", Table: "users", GoType: reflect.TypeFor[string]()},
			schemax.ColumnMeta{Name: "role_id", Table: "users", GoType: reflect.TypeFor[int64]()},
			schemax.ColumnMeta{Name: "status", Table: "users", GoType: reflect.TypeFor[int]()},
		),
		PrimaryKey: &schemax.PrimaryKeyMeta{
			Name:    "pk_users",
			Table:   "users",
			Columns: collectionx.NewList[string]("id"),
		},
		ForeignKeys: collectionx.NewList[schemax.ForeignKeyMeta](
			schemax.ForeignKeyMeta{
				Name:          "fk_users_role_id",
				Table:         "users",
				Columns:       collectionx.NewList[string]("role_id"),
				TargetTable:   "roles",
				TargetColumns: collectionx.NewList[string]("id"),
				OnDelete:      schemax.ReferentialCascade,
			},
		),
		Checks: collectionx.NewList[schemax.CheckMeta](
			schemax.CheckMeta{
				Name:       "ck_users_status",
				Table:      "users",
				Expression: "status >= 0",
			},
		),
	})
	require.NoError(t, err)

	expected := `CREATE TABLE IF NOT EXISTS "users" ("id" INTEGER PRIMARY KEY AUTOINCREMENT, "username" TEXT NOT NULL, "email_address" TEXT NOT NULL, "role_id" INTEGER NOT NULL, "status" INTEGER NOT NULL, CONSTRAINT "fk_users_role_id" FOREIGN KEY ("role_id") REFERENCES "roles" ("id") ON DELETE CASCADE, CONSTRAINT "ck_users_status" CHECK (status >= 0))`
	require.Equal(t, expected, bound.SQL)
}

func TestInspectTable(t *testing.T) {
	ctx := context.Background()
	db := openSQLiteDB(t)

	execSQLiteStatements(ctx, t, db,
		"PRAGMA foreign_keys = ON",
		`CREATE TABLE roles (id INTEGER PRIMARY KEY)`,
		`CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, username TEXT NOT NULL, email_address TEXT NOT NULL, role_id INTEGER NOT NULL, status INTEGER NOT NULL, CONSTRAINT fk_users_role_id FOREIGN KEY (role_id) REFERENCES roles (id) ON DELETE CASCADE, CONSTRAINT ck_users_status CHECK (status >= 0))`,
		`CREATE INDEX idx_users_username ON users(username)`,
		`CREATE UNIQUE INDEX ux_users_email_address ON users(email_address)`,
	)

	dialect := sqlitedialect.New()
	core := dbx.New(db, dialect)
	state, err := dialect.InspectTable(ctx, core, "users")
	require.NoError(t, err)

	require.True(t, state.Exists)
	require.Equal(t, 5, state.Columns.Len())
	require.Equal(t, 2, state.Indexes.Len())

	require.NotNil(t, state.PrimaryKey)
	require.Equal(t, []string{"id"}, state.PrimaryKey.Columns.Values())

	require.Equal(t, 1, state.ForeignKeys.Len())
	foreignKey, ok := state.ForeignKeys.Get(0)
	require.True(t, ok)
	require.Equal(t, "roles", foreignKey.TargetTable)

	require.Equal(t, 1, state.Checks.Len())
	check, ok := state.Checks.Get(0)
	require.True(t, ok)
	require.Equal(t, "status >= 0", check.Expression)
}

func openSQLiteDB(tb testing.TB) *sql.DB {
	tb.Helper()

	db, err := sql.Open("sqlite", ":memory:")
	require.NoError(tb, err)

	tb.Cleanup(func() {
		if closeErr := db.Close(); closeErr != nil {
			tb.Errorf("close sqlite db: %v", closeErr)
		}
	})

	return db
}

func execSQLiteStatements(ctx context.Context, tb testing.TB, db *sql.DB, statements ...string) {
	tb.Helper()

	for _, statement := range statements {
		_, err := db.ExecContext(ctx, statement)
		require.NoError(tb, err)
	}
}
