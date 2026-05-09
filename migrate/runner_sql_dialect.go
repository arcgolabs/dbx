package migrate

import (
	"context"

	collectionx "github.com/arcgolabs/collectionx/list"
)

// UpSQLFor applies SQL migrations from source using database as the migration dialect selector.
func (r *Runner) UpSQLFor(ctx context.Context, database DialectName, source FileSource) (RunReport, error) {
	return r.UpSQL(ctx, source.ForDialect(database))
}

// UpSQLToFor applies SQL migrations for database up to, and including, toVersion.
func (r *Runner) UpSQLToFor(ctx context.Context, toVersion int64, database DialectName, source FileSource) (RunReport, error) {
	return r.UpSQLTo(ctx, toVersion, source.ForDialect(database))
}

// DownSQLToFor rolls back SQL migrations for database down to, but not including, toVersion.
func (r *Runner) DownSQLToFor(ctx context.Context, toVersion int64, database DialectName, source FileSource) (RunReport, error) {
	return r.DownSQLTo(ctx, toVersion, source.ForDialect(database))
}

// PendingSQLFor returns pending SQL migrations using database as the migration dialect selector.
func (r *Runner) PendingSQLFor(ctx context.Context, database DialectName, source FileSource) (*collectionx.List[SQLMigration], error) {
	return r.PendingSQL(ctx, source.ForDialect(database))
}

// StatusSQLFor returns SQL migration statuses using database as the migration dialect selector.
func (r *Runner) StatusSQLFor(ctx context.Context, database DialectName, source FileSource) (*collectionx.List[MigrationStatus], error) {
	return r.StatusSQL(ctx, source.ForDialect(database))
}
