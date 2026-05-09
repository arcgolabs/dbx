package migrate_test

import (
	"io/fs"
	"path/filepath"
	"testing"
	"testing/fstest"

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

	parsed, err = migrate.ParseVersionedFilename("R__refresh_cache__MySQL.sql")
	require.NoError(t, err)
	require.Equal(t, "", parsed.Version)
	require.Equal(t, "refresh cache", parsed.Description)
	require.Equal(t, migrate.KindRepeatable, parsed.Kind)
	require.Equal(t, migrate.DirectionUp, parsed.Direction)
	require.Equal(t, migrate.DialectMySQL, parsed.Database)
}

func TestFileSourceList_RespectsDatabaseFilter(t *testing.T) {
	t.Parallel()

	baseSource := migrate.FileSource{
		FS: fstest.MapFS{
			"sql/V1__create_users.sql":           &fstest.MapFile{Data: []byte("CREATE TABLE users (id INTEGER PRIMARY KEY);\n")},
			"sql/V2__seed_users__sqlite.sql":     &fstest.MapFile{Data: []byte("INSERT INTO users (id) VALUES (1);\n")},
			"sql/V2__seed_users__mysql.sql":      &fstest.MapFile{Data: []byte("INSERT INTO users (id) VALUES (1);\n")},
			"sql/V3__seed_roles__sqlite.sql":     &fstest.MapFile{Data: []byte("INSERT INTO roles (id) VALUES (1);\n")},
			"sql/V4__seed_roles__postgresql.sql": &fstest.MapFile{Data: []byte("INSERT INTO roles (id) VALUES (1);\n")},
		},
		Dir: "sql",
	}

	items, err := baseSource.List()
	require.NoError(t, err)
	require.Equal(t, 5, items.Len())
	var hasV1Any, hasV2Sqlite, hasV2Mysql, hasV3Sqlite, hasV4Pg bool
	for i := 0; i < items.Len(); i++ {
		item, ok := items.Get(i)
		require.True(t, ok)
		switch item.Version {
		case "1":
			hasV1Any = hasV1Any || item.Database == migrate.DialectAny
		case "2":
			if item.Database == migrate.DialectSQLite {
				hasV2Sqlite = true
			}
			if item.Database == migrate.DialectMySQL {
				hasV2Mysql = true
			}
		case "3":
			hasV3Sqlite = true
		case "4":
			hasV4Pg = true
		}
	}
	require.True(t, hasV1Any)
	require.True(t, hasV2Sqlite)
	require.True(t, hasV2Mysql)
	require.True(t, hasV3Sqlite)
	require.True(t, hasV4Pg)

	items, err = migrate.FileSource{
		FS:       baseSource.FS,
		Dir:      baseSource.Dir,
		Database: migrate.DialectSQLite,
	}.List()
	require.NoError(t, err)
	require.Equal(t, 3, items.Len())
	versions := make(map[string]struct{}, items.Len())
	databases := make(map[migrate.DialectName]int, items.Len())
	for i := 0; i < items.Len(); i++ {
		item, ok := items.Get(i)
		require.True(t, ok)
		versions[item.Version] = struct{}{}
		databases[item.Database]++
	}
	var hasV1, hasV2, hasV3 bool
	_, hasV1 = versions["1"]
	_, hasV2 = versions["2"]
	_, hasV3 = versions["3"]
	require.True(t, hasV1)
	require.True(t, hasV2)
	require.True(t, hasV3)
	require.Equal(t, 1, databases[migrate.DialectAny])
	require.Equal(t, 2, databases[migrate.DialectSQLite])

	items, err = migrate.FileSource{
		FS:       baseSource.FS,
		Dir:      baseSource.Dir,
		Database: migrate.DialectMySQL,
	}.List()
	require.NoError(t, err)
	require.Equal(t, 2, items.Len())
	versions = make(map[string]struct{}, items.Len())
	for i := 0; i < items.Len(); i++ {
		item, ok := items.Get(i)
		require.True(t, ok)
		versions[item.Version] = struct{}{}
	}
	_, hasV1 = versions["1"]
	_, hasV2 = versions["2"]
	require.True(t, hasV1)
	require.True(t, hasV2)
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
