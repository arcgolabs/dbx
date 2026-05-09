package migrate_test

import (
	"io/fs"
	"path/filepath"
	"testing"
	"testing/fstest"

	collectionx "github.com/arcgolabs/collectionx/list"
	"github.com/arcgolabs/dbx/migrate"
	"github.com/stretchr/testify/require"
)

func TestSafeJoinPath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		base   string
		name   string
		wantOk bool
	}{
		{"migrations", "V1__init.sql", true},
		{"migrations", "subdir/V2__add.sql", true},
		{"migrations", "..", false},
		{"migrations", "../etc/passwd", false},
		{"migrations", "sub/../../etc/passwd", false},
		{".", "V1__init.sql", true},
	}
	for _, tt := range tests {
		source := migrate.FileSource{
			FS:  fs.FS(fakeFSWithEntry{name: tt.name}),
			Dir: tt.base,
		}
		items, err := source.List()
		if tt.wantOk {
			require.NoError(t, err, "base=%q name=%q", tt.base, tt.name)
			expect := filepath.Clean(filepath.Join(tt.base, tt.name))
			require.Equal(t, 1, items.Len(), "base=%q name=%q", tt.base, tt.name)
			item, ok := items.Get(0)
			require.True(t, ok, "base=%q name=%q", tt.base, tt.name)
			require.Equal(t, filepath.ToSlash(expect), item.UpPath, "base=%q name=%q", tt.base, tt.name)
		} else {
			require.Error(t, err, "base=%q name=%q should reject path traversal", tt.base, tt.name)
		}
	}
}

func TestFileSourceList_RejectsPathTraversal(t *testing.T) {
	t.Parallel()

	// MapFS with a name that would trigger path traversal when joined
	source := migrate.FileSource{
		FS:  fs.FS(fakeFSWithTraversal{}),
		Dir: "sql",
	}
	_, err := source.List()
	require.Error(t, err)
	require.Contains(t, err.Error(), "path traversal")
}

func TestParseVersionedFilenameWithDatabaseSelector(t *testing.T) {
	t.Parallel()

	parsed, err := migrate.ParseVersionedFilename("V1__create_user.sql")
	require.NoError(t, err)
	require.Equal(t, "1", parsed.Version)
	require.Equal(t, "create user", parsed.Description)
	require.Equal(t, migrate.KindSQL, parsed.Kind)
	require.Equal(t, migrate.DirectionUp, parsed.Direction)
	require.Equal(t, migrate.DialectAny, parsed.Database)

	parsed, err = migrate.ParseVersionedFilename("V12__create_user__sqlite.sql")
	require.NoError(t, err)
	require.Equal(t, "12", parsed.Version)
	require.Equal(t, "create user", parsed.Description)
	require.Equal(t, migrate.DialectSQLite, parsed.Database)

	parsed, err = migrate.ParseVersionedFilename("V13__create_user_sqlite.sql")
	require.NoError(t, err)
	require.Equal(t, "13", parsed.Version)
	require.Equal(t, "create user", parsed.Description)
	require.Equal(t, migrate.DialectSQLite, parsed.Database)

	parsed, err = migrate.ParseVersionedFilename("R__refresh_cache__MySQL.sql")
	require.NoError(t, err)
	require.Equal(t, "", parsed.Version)
	require.Equal(t, "refresh cache", parsed.Description)
	require.Equal(t, migrate.KindRepeatable, parsed.Kind)
	require.Equal(t, migrate.DirectionUp, parsed.Direction)
	require.Equal(t, migrate.DialectMySQL, parsed.Database)

	parsed, err = migrate.ParseVersionedFilename("R__refresh_cache_mysql.sql")
	require.NoError(t, err)
	require.Equal(t, "", parsed.Version)
	require.Equal(t, "refresh cache", parsed.Description)
	require.Equal(t, migrate.KindRepeatable, parsed.Kind)
	require.Equal(t, migrate.DirectionUp, parsed.Direction)
	require.Equal(t, migrate.DialectMySQL, parsed.Database)
}

func TestFileSourceList_RespectsDatabaseFilter(t *testing.T) {
	t.Parallel()

	baseSource := databaseFilterSource()

	items, err := baseSource.List()
	require.NoError(t, err)
	requireMigrationDatabases(t, items, map[string][]migrate.DialectName{
		"1": {migrate.DialectAny},
		"2": {migrate.DialectSQLite, migrate.DialectMySQL},
		"3": {migrate.DialectSQLite},
		"4": {migrate.DialectPostgres},
		"5": {migrate.DialectSQLite},
	})

	items, err = baseSource.ForDialect(migrate.DialectSQLite).List()
	require.NoError(t, err)
	requireMigrationDatabases(t, items, map[string][]migrate.DialectName{
		"1": {migrate.DialectAny},
		"2": {migrate.DialectSQLite},
		"3": {migrate.DialectSQLite},
		"5": {migrate.DialectSQLite},
	})

	items, err = migrate.FileSource{
		FS:       baseSource.FS,
		Dir:      baseSource.Dir,
		Database: migrate.DialectMySQL,
	}.List()
	require.NoError(t, err)
	requireMigrationDatabases(t, items, map[string][]migrate.DialectName{
		"1": {migrate.DialectAny},
		"2": {migrate.DialectMySQL},
	})
}

func databaseFilterSource() migrate.FileSource {
	return migrate.FileSource{
		FS: fstest.MapFS{
			"sql/V1__create_users.sql":            &fstest.MapFile{Data: []byte("CREATE TABLE users (id INTEGER PRIMARY KEY);\n")},
			"sql/V2__seed_users__sqlite.sql":      &fstest.MapFile{Data: []byte("INSERT INTO users (id) VALUES (1);\n")},
			"sql/V2__seed_users__mysql.sql":       &fstest.MapFile{Data: []byte("INSERT INTO users (id) VALUES (1);\n")},
			"sql/V3__seed_roles__sqlite.sql":      &fstest.MapFile{Data: []byte("INSERT INTO roles (id) VALUES (1);\n")},
			"sql/V4__seed_roles__postgresql.sql":  &fstest.MapFile{Data: []byte("INSERT INTO roles (id) VALUES (1);\n")},
			"sql/V5__seed_permissions_sqlite.sql": &fstest.MapFile{Data: []byte("INSERT INTO permissions (id) VALUES (1);\n")},
		},
		Dir: "sql",
	}
}

func requireMigrationDatabases(
	t *testing.T,
	items *collectionx.List[migrate.SQLMigration],
	expected map[string][]migrate.DialectName,
) {
	t.Helper()

	actual := migrationDatabaseCounts(t, items)
	require.Equal(t, expectedMigrationCount(expected), items.Len())
	for version, databases := range expected {
		for _, database := range databases {
			require.Equal(t, 1, actual[version][database], "version=%s database=%s", version, database)
		}
	}
}

func migrationDatabaseCounts(
	t *testing.T,
	items *collectionx.List[migrate.SQLMigration],
) map[string]map[migrate.DialectName]int {
	t.Helper()

	actual := make(map[string]map[migrate.DialectName]int, items.Len())
	for i := range items.Len() {
		item, ok := items.Get(i)
		require.True(t, ok)
		if actual[item.Version] == nil {
			actual[item.Version] = make(map[migrate.DialectName]int)
		}
		actual[item.Version][item.Database]++
	}
	return actual
}

func expectedMigrationCount(expected map[string][]migrate.DialectName) int {
	count := 0
	for _, databases := range expected {
		count += len(databases)
	}
	return count
}

func TestParseDialectName(t *testing.T) {
	t.Parallel()

	parsed, err := migrate.ParseDialectName("mysql")
	require.NoError(t, err)
	require.Equal(t, migrate.DialectMySQL, parsed)

	parsed, err = migrate.ParseDialectName("PostgreSQL")
	require.NoError(t, err)
	require.Equal(t, migrate.DialectPostgres, parsed)

	parsed, err = migrate.ParseDialectName("  sqlite3  ")
	require.NoError(t, err)
	require.Equal(t, migrate.DialectSQLite, parsed)

	_, err = migrate.ParseDialectName("oracle")
	require.Error(t, err)
}

type testDialectContract struct{}

func (testDialectContract) Name() string {
	return "sqlite"
}

func (testDialectContract) BindVar(_ int) string { return "" }

func (testDialectContract) QuoteIdent(ident string) string { return ident }

func (testDialectContract) RenderLimitOffset(*int, *int) (string, error) { return "", nil }

func TestDialectFromDialect(t *testing.T) {
	t.Parallel()

	database, err := migrate.DialectFromDialect(testDialectContract{})
	require.NoError(t, err)
	require.Equal(t, migrate.DialectSQLite, database)

	_, err = migrate.DialectFromDialect(nil)
	require.Error(t, err)
}

type fakeFSWithEntry struct {
	name string
}

func (fakeFSWithEntry) Open(string) (fs.File, error) { return nil, fs.ErrNotExist }

func (f fakeFSWithEntry) ReadDir(string) ([]fs.DirEntry, error) {
	return []fs.DirEntry{&fakeDirEntry{name: f.name}}, nil
}

type fakeFSWithTraversal struct{}

func (fakeFSWithTraversal) Open(string) (fs.File, error) {
	panic("not used")
}

func (fakeFSWithTraversal) ReadDir(name string) ([]fs.DirEntry, error) {
	if name != "sql" {
		return nil, fs.ErrNotExist
	}
	return []fs.DirEntry{&fakeDirEntry{name: "../evil.sql"}}, nil
}

type fakeDirEntry struct {
	name string
}

func (e *fakeDirEntry) Name() string               { return e.name }
func (e *fakeDirEntry) IsDir() bool                { return false }
func (e *fakeDirEntry) Type() fs.FileMode          { return 0 }
func (e *fakeDirEntry) Info() (fs.FileInfo, error) { return nil, fs.ErrNotExist }
