package migrate

import (
	"context"
	"database/sql"
	"io/fs"
	"time"

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

// ForDialect returns a copy of s filtered to database.
func (s FileSource) ForDialect(database DialectName) FileSource {
	s.Database = database
	return s
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
