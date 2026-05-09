package migrate

import (
	"context"

	collectionx "github.com/arcgolabs/collectionx/list"
)

// MigrationStatusBundle contains migration status grouped by migration source.
type MigrationStatusBundle struct {
	Go  *collectionx.List[MigrationStatus]
	SQL *collectionx.List[MigrationStatus]
}

// MigrationPendingBundle contains migrations that are pending for each source.
type MigrationPendingBundle struct {
	Go  *collectionx.List[Migration]
	SQL *collectionx.List[SQLMigration]
}

// StatusAll returns migration status for both Go and SQL sources in a single call.
//
// If no SQL source is provided, SQL status is reported as an empty list.
func (r *Runner) StatusAll(ctx context.Context, goMigrations []Migration, source *FileSource) (*MigrationStatusBundle, error) {
	goStatus, err := r.StatusGo(ctx, goMigrations...)
	if err != nil {
		return nil, err
	}

	sqlStatus := collectionx.NewList[MigrationStatus]()
	if source != nil {
		sqlStatus, err = r.StatusSQL(ctx, *source)
		if err != nil {
			return nil, err
		}
	}

	return &MigrationStatusBundle{
		Go:  goStatus,
		SQL: sqlStatus,
	}, nil
}

// PendingAll returns pending migrations for both Go and SQL sources in a single call.
//
// If no SQL source is provided, SQL pending items are reported as an empty list.
func (r *Runner) PendingAll(ctx context.Context, goMigrations []Migration, source *FileSource) (*MigrationPendingBundle, error) {
	goPending, err := r.PendingGo(ctx, goMigrations...)
	if err != nil {
		return nil, err
	}

	sqlPending := collectionx.NewList[SQLMigration]()
	if source != nil {
		sqlPending, err = r.PendingSQL(ctx, *source)
		if err != nil {
			return nil, err
		}
	}

	return &MigrationPendingBundle{
		Go:  goPending,
		SQL: sqlPending,
	}, nil
}
