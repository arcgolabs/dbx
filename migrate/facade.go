package migrate

import (
	"context"

	collectionx "github.com/arcgolabs/collectionx/list"
)

// GoRunner is a focused facade for Go migrations.
type GoRunner struct {
	runner   *Runner
	database DialectName
}

// Go returns a Go migration facade over runner.
func Go(runner *Runner) GoRunner {
	return GoRunner{runner: runner}
}

// ForDialect returns a copy that selects migrations for database at call time.
func (r GoRunner) ForDialect(database DialectName) GoRunner {
	r.database = database
	return r
}

// Up applies Go migrations.
func (r GoRunner) Up(ctx context.Context, migrations ...Migration) (RunReport, error) {
	return r.runner.UpGoFor(ctx, r.database, migrations...)
}

// UpTo applies Go migrations up to, and including, toVersion.
func (r GoRunner) UpTo(ctx context.Context, toVersion int64, migrations ...Migration) (RunReport, error) {
	return r.runner.UpGoToFor(ctx, toVersion, r.database, migrations...)
}

// DownTo rolls back Go migrations down to, but not including, toVersion.
func (r GoRunner) DownTo(ctx context.Context, toVersion int64, migrations ...Migration) (RunReport, error) {
	return r.runner.DownGoToFor(ctx, toVersion, r.database, migrations...)
}

// Pending returns pending Go migrations.
func (r GoRunner) Pending(ctx context.Context, migrations ...Migration) (*collectionx.List[Migration], error) {
	return r.runner.PendingGoFor(ctx, r.database, migrations...)
}

// Status returns Go migration statuses.
func (r GoRunner) Status(ctx context.Context, migrations ...Migration) (*collectionx.List[MigrationStatus], error) {
	return r.runner.StatusGoFor(ctx, r.database, migrations...)
}

// SQLRunner is a focused facade for SQL migrations.
type SQLRunner struct {
	runner   *Runner
	database DialectName
}

// SQL returns a SQL migration facade over runner.
func SQL(runner *Runner) SQLRunner {
	return SQLRunner{runner: runner}
}

// ForDialect returns a copy that selects SQL files for database at call time.
func (r SQLRunner) ForDialect(database DialectName) SQLRunner {
	r.database = database
	return r
}

// Up applies SQL migrations.
func (r SQLRunner) Up(ctx context.Context, source FileSource) (RunReport, error) {
	return r.runner.UpSQLFor(ctx, r.database, source)
}

// UpTo applies SQL migrations up to, and including, toVersion.
func (r SQLRunner) UpTo(ctx context.Context, toVersion int64, source FileSource) (RunReport, error) {
	return r.runner.UpSQLToFor(ctx, toVersion, r.database, source)
}

// DownTo rolls back SQL migrations down to, but not including, toVersion.
func (r SQLRunner) DownTo(ctx context.Context, toVersion int64, source FileSource) (RunReport, error) {
	return r.runner.DownSQLToFor(ctx, toVersion, r.database, source)
}

// Pending returns pending SQL migrations.
func (r SQLRunner) Pending(ctx context.Context, source FileSource) (*collectionx.List[SQLMigration], error) {
	return r.runner.PendingSQLFor(ctx, r.database, source)
}

// Status returns SQL migration statuses.
func (r SQLRunner) Status(ctx context.Context, source FileSource) (*collectionx.List[MigrationStatus], error) {
	return r.runner.StatusSQLFor(ctx, r.database, source)
}
