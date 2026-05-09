package migrate

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"strings"
	"time"

	collectionx "github.com/arcgolabs/collectionx/list"
	mappingx "github.com/arcgolabs/collectionx/mapping"
	"github.com/arcgolabs/dbx/dialect"
)

// DialectName is a migration dialect selector.
type DialectName uint8

const (
	// DialectAny is the wildcard selector used when no dialect is specified.
	DialectAny DialectName = iota
	// DialectMySQL is the canonical MySQL migration selector.
	DialectMySQL
	// DialectPostgres is the canonical PostgreSQL migration selector.
	DialectPostgres
	// DialectSQLite is the canonical SQLite migration selector.
	DialectSQLite
	// DialectSQLServer is provided for compatibility, though it is not built in.
	DialectSQLServer
)

var (
	errDialectAliases = map[string]DialectName{
		"":           DialectAny,
		"mysql":      DialectMySQL,
		"postgres":   DialectPostgres,
		"postgresql": DialectPostgres,
		"sqlite":     DialectSQLite,
		"sqlite3":    DialectSQLite,
		"sqlserver":  DialectSQLServer,
		"mssql":      DialectSQLServer,
	}

	errInvalidDialect = errors.New("dbx/migrate: unknown migration dialect")
)

// String returns the canonical string representation for a migration dialect name.
func (d DialectName) String() string {
	switch d {
	case DialectMySQL:
		return "mysql"
	case DialectPostgres:
		return "postgres"
	case DialectSQLite:
		return "sqlite"
	case DialectSQLServer:
		return "sqlserver"
	default:
		return ""
	}
}

// IsKnown reports whether the dialect is an explicit supported database name.
func (d DialectName) IsKnown() bool {
	switch d {
	case DialectMySQL, DialectPostgres, DialectSQLite, DialectSQLServer:
		return true
	default:
		return false
	}
}

// IsValid reports whether the value is either a known dialect or the wildcard.
func (d DialectName) IsValid() bool {
	return d.IsKnown() || d.IsAny()
}

// IsAny reports whether the selector should match all migration dialects.
func (d DialectName) IsAny() bool {
	return d == DialectAny
}

// ParseDialectName parses a canonical or aliased dialect name.
func ParseDialectName(raw string) (DialectName, error) {
	norm := strings.ToLower(strings.TrimSpace(raw))
	dialectName, ok := errDialectAliases[norm]
	if !ok {
		return DialectAny, fmt.Errorf("%w: %q", errInvalidDialect, raw)
	}
	return dialectName, nil
}

// MustDialectName panics when ParseDialectName fails.
func MustDialectName(raw string) DialectName {
	dialectName, err := ParseDialectName(raw)
	if err != nil {
		panic(err)
	}
	return dialectName
}

// DialectFromDialect converts a query dialect to a migration selector.
func DialectFromDialect(d dialect.Contract) (DialectName, error) {
	if d == nil {
		return DialectAny, fmt.Errorf("%w: <nil>", errInvalidDialect)
	}
	return ParseDialectName(d.Name())
}

// Kind identifies the source type of a recorded migration.
type Kind string

// Direction identifies whether a migration file applies or rolls back changes.
type Direction string

const (
	// KindGo records a Go migration.
	KindGo Kind = "go"
	// KindSQL records a versioned SQL migration.
	KindSQL Kind = "sql"
	// KindRepeatable records a repeatable SQL migration.
	KindRepeatable Kind = "repeatable"
)

const (
	// DirectionUp applies a migration.
	DirectionUp Direction = "up"
	// DirectionDown rolls back a migration.
	DirectionDown Direction = "down"
)

// ErrInvalidVersionedFilename reports an invalid versioned migration filename.
var ErrInvalidVersionedFilename = errors.New("dbx/migrate: invalid versioned filename")

// Migration is the contract implemented by executable Go migrations.
type Migration interface {
	Version() string
	Description() string
	Up(ctx context.Context, tx *sql.Tx) error
	Down(ctx context.Context, tx *sql.Tx) error
}

// GoMigration is an in-memory migration implemented with Go functions.
type GoMigration struct {
	version     string
	description string
	up          func(context.Context, *sql.Tx) error
	down        func(context.Context, *sql.Tx) error
	databases   []DialectName
}

// NewGoMigration builds a Go migration from up/down callbacks.
// Optional database dialect names can be provided to run this migration only for
// matching runner dialects.
func NewGoMigration(
	version,
	description string,
	up,
	down func(context.Context, *sql.Tx) error,
	databases ...DialectName,
) GoMigration {
	return GoMigration{
		version:     version,
		description: description,
		up:          up,
		down:        down,
		databases:   normalizeMigrationDatabases(databases),
	}
}

// Version returns the migration version.
func (m GoMigration) Version() string { return m.version }

// Description returns the migration description.
func (m GoMigration) Description() string { return m.description }

// Up applies the migration within tx.
func (m GoMigration) Up(ctx context.Context, tx *sql.Tx) error {
	if m.up == nil {
		return nil
	}
	return m.up(ctx, tx)
}

// Down rolls back the migration within tx.
func (m GoMigration) Down(ctx context.Context, tx *sql.Tx) error {
	if m.down == nil {
		return nil
	}
	return m.down(ctx, tx)
}

func (m GoMigration) Databases() []DialectName {
	if len(m.databases) == 0 {
		return nil
	}
	databases := make([]DialectName, len(m.databases))
	copy(databases, m.databases)
	return databases
}

// VersionedFile describes a parsed migration filename.
type VersionedFile struct {
	Version     string
	Description string
	Kind        Kind
	Direction   Direction
	Database    DialectName
	Path        string
	Filename    string
}

// SQLMigration describes a versioned or repeatable SQL migration pair.
type SQLMigration struct {
	Version     string
	Description string
	Database    DialectName
	UpPath      string
	DownPath    string
	Repeatable  bool
}

// FileSource lists SQL migration files from a filesystem directory.
type FileSource struct {
	FS       fs.FS
	Dir      string
	Database DialectName
}

// RunnerOptions configures migration history tracking and ordering behavior.
type RunnerOptions struct {
	HistoryTable    string
	AllowOutOfOrder bool
	ValidateHash    bool
}

// Runner applies migrations and queries migration history.
type Runner struct {
	db      *sql.DB
	dialect dialect.Dialect
	options RunnerOptions
}

// AppliedRecord records a migration execution in the history table.
type AppliedRecord struct {
	Version     string
	Description string
	Kind        Kind
	AppliedAt   time.Time
	Checksum    string
	Success     bool
}

// NewRunner creates a migration runner for db and d.
func NewRunner(db *sql.DB, d dialect.Dialect, opts RunnerOptions) *Runner {
	if opts.HistoryTable == "" {
		opts.HistoryTable = "schema_history"
	}
	return &Runner{db: db, dialect: d, options: opts}
}

// DB returns the underlying database handle.
func (r *Runner) DB() *sql.DB {
	return r.db
}

// Dialect returns the SQL dialect used by the runner.
func (r *Runner) Dialect() dialect.Dialect {
	return r.dialect
}

// Options returns the runner options.
func (r *Runner) Options() RunnerOptions {
	return r.options
}

const versionedMigrationSuffix = ".sql"

// ParseVersionedFilename parses a migration filename into a structured record.
func ParseVersionedFilename(name string) (VersionedFile, error) {
	base := filepath.Base(name)
	if len(base) < len("V__a.sql") {
		return VersionedFile{}, ErrInvalidVersionedFilename
	}
	if !strings.HasSuffix(base, versionedMigrationSuffix) {
		return VersionedFile{}, ErrInvalidVersionedFilename
	}
	prefix := base[0]
	header := base[1 : len(base)-len(versionedMigrationSuffix)]
	if prefix != 'V' && prefix != 'U' && prefix != 'R' {
		return VersionedFile{}, ErrInvalidVersionedFilename
	}

	separator := strings.Index(header, "__")
	if separator < 0 {
		return VersionedFile{}, ErrInvalidVersionedFilename
	}

	file := VersionedFile{
		Filename: base,
		Path:     name,
	}

	switch string(prefix) {
	case "V":
		file.Kind = KindSQL
		file.Direction = DirectionUp
		if header[:separator] == "" {
			return VersionedFile{}, ErrInvalidVersionedFilename
		}
	case "U":
		file.Kind = KindSQL
		file.Direction = DirectionDown
		if header[:separator] == "" {
			return VersionedFile{}, ErrInvalidVersionedFilename
		}
	case "R":
		file.Kind = KindRepeatable
		file.Direction = DirectionUp
		if header[:separator] != "" {
			return VersionedFile{}, ErrInvalidVersionedFilename
		}
	}

	description, database := splitVersionedDescription(header[separator+2:])
	file.Version = header[:separator]
	if !isValidVersion(file.Version) && file.Kind != KindRepeatable {
		return VersionedFile{}, ErrInvalidVersionedFilename
	}
	if description == "" {
		return VersionedFile{}, ErrInvalidVersionedFilename
	}
	file.Description = strings.ReplaceAll(description, "_", " ")
	file.Database = database
	return file, nil
}

func splitVersionedDescription(raw string) (string, DialectName) {
	last := strings.LastIndex(raw, "__")
	if last < 0 {
		return raw, DialectAny
	}

	database := strings.TrimSpace(raw[last+2:])
	if !isDialectName(database) {
		return raw, DialectAny
	}

	return raw[:last], normalizeMigrationDatabase(database)
}

func isDialectName(raw string) bool {
	if raw == "" {
		return false
	}
	_, ok := errDialectAliases[strings.ToLower(strings.TrimSpace(raw))]
	return ok
}

func isValidVersion(version string) bool {
	if version == "" {
		return false
	}
	for i := range version {
		ch := version[i]
		if (ch >= '0' && ch <= '9') ||
			(ch >= 'A' && ch <= 'Z') ||
			(ch >= 'a' && ch <= 'z') ||
			ch == '_' || ch == '.' || ch == '-' {
			continue
		}
		return false
	}
	return true
}

// List returns the SQL migrations discovered in s.
func (s FileSource) List() (*collectionx.List[SQLMigration], error) {
	entries, err := s.readEntries()
	if err != nil {
		return nil, err
	}

	items := mappingx.NewMapWithCapacity[string, *SQLMigration](entries.Len())
	_, err = collectionx.ReduceErrList[fs.DirEntry, struct{}](
		collectionx.FilterList[fs.DirEntry](entries, func(_ int, entry fs.DirEntry) bool {
			return !entry.IsDir()
		}),
		struct{}{},
		func(state struct{}, _ int, entry fs.DirEntry) (struct{}, error) {
			return state, s.addEntry(items, entry)
		},
	)
	if err != nil {
		return nil, fmt.Errorf("dbx/migrate: collect sql migration entries: %w", err)
	}

	return sortedSQLMigrations(items), nil
}

func (s FileSource) readEntries() (*collectionx.List[fs.DirEntry], error) {
	entries, err := fs.ReadDir(s.FS, s.Dir)
	if err != nil {
		return nil, fmt.Errorf("dbx/migrate: read migration dir %q: %w", s.Dir, err)
	}
	return collectionx.NewList[fs.DirEntry](entries...), nil
}

func (s FileSource) addEntry(items *mappingx.Map[string, *SQLMigration], entry fs.DirEntry) error {
	fullPath, err := safeJoinPath(s.Dir, entry.Name())
	if err != nil {
		return fmt.Errorf("dbx/migrate: resolve migration path %q: %w", entry.Name(), err)
	}

	parsed, err := ParseVersionedFilename(entry.Name())
	if err != nil {
		if errors.Is(err, ErrInvalidVersionedFilename) {
			return nil
		}
		return fmt.Errorf("dbx/migrate: parse migration filename %q: %w", entry.Name(), err)
	}
	if !selectsVersionedFile(parsed, s.Database) {
		return nil
	}

	key := sqlMigrationKey(parsed)
	migration, exists := items.Get(key)
	if !exists {
		migration = &SQLMigration{
			Version:     parsed.Version,
			Description: parsed.Description,
			Database:    parsed.Database,
			Repeatable:  parsed.Kind == KindRepeatable,
		}
		items.Set(key, migration)
	}

	setSQLMigrationPath(migration, parsed.Direction, filepath.ToSlash(fullPath))
	return nil
}

func sqlMigrationKey(file VersionedFile) string {
	return strings.Join([]string{file.Version, file.Description, file.Database.String()}, ":")
}

func normalizeMigrationDatabase(name string) DialectName {
	normalized, err := ParseDialectName(name)
	if err != nil {
		return DialectAny
	}
	return normalized
}

func normalizeMigrationDatabases(databases []DialectName) []DialectName {
	if len(databases) == 0 {
		return nil
	}

	normed := make([]DialectName, 0, len(databases))
	seen := make(map[DialectName]struct{}, len(databases))
	for _, database := range databases {
		if !database.IsKnown() {
			continue
		}
		if _, ok := seen[database]; ok {
			continue
		}
		seen[database] = struct{}{}
		normed = append(normed, database)
	}
	return normed
}

func selectsVersionedFile(file VersionedFile, selectedDatabase DialectName) bool {
	if selectedDatabase.IsAny() {
		return true
	}
	return file.Database.IsAny() || file.Database == selectedDatabase
}

func setSQLMigrationPath(migration *SQLMigration, direction Direction, fullPath string) {
	if direction == DirectionUp {
		migration.UpPath = fullPath
		return
	}
	migration.DownPath = fullPath
}

func sortedSQLMigrations(items *mappingx.Map[string, *SQLMigration]) *collectionx.List[SQLMigration] {
	sorted := collectionx.NewListWithCapacity[SQLMigration](items.Len())
	items.Range(func(_ string, migration *SQLMigration) bool {
		sorted.Add(*migration)
		return true
	})
	return sorted.Sort(func(left, right SQLMigration) int {
		switch {
		case left.Repeatable != right.Repeatable:
			if left.Repeatable {
				return 1
			}
			return -1
		case left.Version < right.Version:
			return -1
		case left.Version > right.Version:
			return 1
		case left.Description < right.Description:
			return -1
		case left.Description > right.Description:
			return 1
		default:
			return 0
		}
	})
}
