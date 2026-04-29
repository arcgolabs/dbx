package migrate

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	collectionx "github.com/arcgolabs/collectionx/list"
	mappingx "github.com/arcgolabs/collectionx/mapping"
	"github.com/arcgolabs/dbx/dialect"
)

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
}

// NewGoMigration builds a Go migration from up/down callbacks.
func NewGoMigration(version, description string, up, down func(context.Context, *sql.Tx) error) GoMigration {
	return GoMigration{version: version, description: description, up: up, down: down}
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

// VersionedFile describes a parsed migration filename.
type VersionedFile struct {
	Version     string
	Description string
	Kind        Kind
	Direction   Direction
	Path        string
	Filename    string
}

// SQLMigration describes a versioned or repeatable SQL migration pair.
type SQLMigration struct {
	Version     string
	Description string
	UpPath      string
	DownPath    string
	Repeatable  bool
}

// FileSource lists SQL migration files from a filesystem directory.
type FileSource struct {
	FS  fs.FS
	Dir string
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

var versionedFilePattern = regexp.MustCompile(`^(?P<prefix>V|U|R)(?P<version>[0-9A-Za-z_.-]*)__(?P<description>.+)\.sql$`)

// ParseVersionedFilename parses a migration filename into a structured record.
func ParseVersionedFilename(name string) (VersionedFile, error) {
	base := filepath.Base(name)
	match := versionedFilePattern.FindStringSubmatch(base)
	if match == nil {
		return VersionedFile{}, ErrInvalidVersionedFilename
	}

	file := VersionedFile{
		Filename: base,
		Path:     name,
	}

	switch match[1] {
	case "V":
		file.Kind = KindSQL
		file.Direction = DirectionUp
	case "U":
		file.Kind = KindSQL
		file.Direction = DirectionDown
	case "R":
		file.Kind = KindRepeatable
		file.Direction = DirectionUp
	}

	file.Version = match[2]
	file.Description = strings.ReplaceAll(match[3], "_", " ")
	return file, nil
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

	key := sqlMigrationKey(parsed)
	migration, exists := items.Get(key)
	if !exists {
		migration = &SQLMigration{
			Version:     parsed.Version,
			Description: parsed.Description,
			Repeatable:  parsed.Kind == KindRepeatable,
		}
		items.Set(key, migration)
	}

	setSQLMigrationPath(migration, parsed.Direction, filepath.ToSlash(fullPath))
	return nil
}

func sqlMigrationKey(file VersionedFile) string {
	return file.Version + ":" + file.Description
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

// safeJoinPath joins base and name, returning an error if the result escapes base (path traversal).
func safeJoinPath(base, name string) (string, error) {
	base = filepath.Clean(base)
	path := filepath.Clean(filepath.Join(base, name))
	rel, err := filepath.Rel(base, path)
	if err != nil {
		return "", fmt.Errorf("dbx/migrate: compute relative path for %q: %w", name, err)
	}
	if strings.HasPrefix(rel, "..") {
		return "", errors.New("path traversal not allowed: " + name)
	}
	return path, nil
}
