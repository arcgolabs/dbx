package migrate

import (
	"context"
	"fmt"

	collectionx "github.com/arcgolabs/collectionx/list"
)

// UpGoFor applies Go migrations using database as the migration dialect selector.
func (r *Runner) UpGoFor(ctx context.Context, database DialectName, migrations ...Migration) (RunReport, error) {
	return r.upGoForDialect(ctx, database, migrations...)
}

// UpGoToFor applies Go migrations for database up to, and including, toVersion.
func (r *Runner) UpGoToFor(ctx context.Context, toVersion int64, database DialectName, migrations ...Migration) (RunReport, error) {
	return r.upGoToForDialect(ctx, toVersion, database, migrations...)
}

// DownGoToFor rolls back Go migrations for database down to, but not including, toVersion.
func (r *Runner) DownGoToFor(ctx context.Context, toVersion int64, database DialectName, migrations ...Migration) (RunReport, error) {
	return r.downGoToForDialect(ctx, toVersion, database, migrations...)
}

// PendingGoFor returns pending Go migrations using database as the migration dialect selector.
func (r *Runner) PendingGoFor(ctx context.Context, database DialectName, migrations ...Migration) (*collectionx.List[Migration], error) {
	migrations = r.selectGoMigrationsForDialect(migrations, database)
	bundle, err := r.newRunnerEngineFromGoMigrations(migrations)
	if err != nil {
		return nil, err
	}
	if bundle.engine == nil {
		return collectionx.NewList[Migration](), nil
	}

	statuses, err := pendingStatuses(ctx, bundle.engine, "go")
	if err != nil {
		return nil, err
	}
	indexed, err := r.appliedIndex(ctx)
	if err != nil {
		return nil, err
	}
	byVersion, err := indexGoMigrationsByVersion(migrations)
	if err != nil {
		return nil, err
	}

	return collectPendingGoMigrations(statuses, bundle.metaByVersion, indexed, byVersion, r.options.ValidateHash)
}

// StatusGoFor returns Go migration statuses using database as the migration dialect selector.
func (r *Runner) StatusGoFor(ctx context.Context, database DialectName, migrations ...Migration) (*collectionx.List[MigrationStatus], error) {
	migrations = r.selectGoMigrationsForDialect(migrations, database)
	bundle, err := r.newRunnerEngineFromGoMigrations(migrations)
	if err != nil {
		return nil, err
	}
	if bundle == nil || bundle.engine == nil {
		return collectionx.NewList[MigrationStatus](), nil
	}

	indexed, err := r.appliedIndex(ctx)
	if err != nil {
		return nil, err
	}
	byVersion, err := indexGoMigrationsByVersion(migrations)
	if err != nil {
		return nil, err
	}

	statuses, err := pendingStatuses(ctx, bundle.engine, "go")
	if err != nil {
		return nil, err
	}
	return buildGoMigrationStatuses(statuses, bundle.metaByVersion, indexed, byVersion, r.options.ValidateHash), nil
}

func (r *Runner) upGoForDialect(ctx context.Context, database DialectName, migrations ...Migration) (RunReport, error) {
	bundle, err := r.newRunnerEngineForGoWithDialect(migrations, database)
	if err != nil {
		return RunReport{}, err
	}
	if bundle.engine == nil {
		return RunReport{Applied: collectionx.NewList[AppliedRecord]()}, nil
	}

	results, err := bundle.engine.Up(ctx)
	if err != nil {
		return RunReport{}, fmt.Errorf("dbx/migrate: apply go migrations: %w", err)
	}
	applied, err := r.Applied(ctx)
	if err != nil {
		return RunReport{}, err
	}
	return buildRunReport(applied, bundle.metaByVersion, results)
}

func (r *Runner) upGoToForDialect(ctx context.Context, toVersion int64, database DialectName, migrations ...Migration) (RunReport, error) {
	if err := validateMigrationTarget(toVersion, true); err != nil {
		return RunReport{}, err
	}

	bundle, err := r.newRunnerEngineForGoWithDialect(migrations, database)
	if err != nil {
		return RunReport{}, err
	}
	if bundle.engine == nil {
		return RunReport{Applied: collectionx.NewList[AppliedRecord]()}, nil
	}

	results, err := bundle.engine.UpTo(ctx, toVersion)
	if err != nil {
		return RunReport{}, fmt.Errorf("dbx/migrate: apply go migrations to version %d: %w", toVersion, err)
	}
	applied, err := r.Applied(ctx)
	if err != nil {
		return RunReport{}, err
	}
	return buildRunReport(applied, bundle.metaByVersion, results)
}

func (r *Runner) downGoToForDialect(ctx context.Context, toVersion int64, database DialectName, migrations ...Migration) (RunReport, error) {
	if err := validateMigrationTarget(toVersion, false); err != nil {
		return RunReport{}, err
	}
	bundle, err := r.newRunnerEngineForGoWithDialect(migrations, database)
	if err != nil {
		return RunReport{}, err
	}
	if bundle.engine == nil {
		return RunReport{Applied: collectionx.NewList[AppliedRecord]()}, nil
	}

	appliedBefore, err := r.Applied(ctx)
	if err != nil {
		return RunReport{}, err
	}

	results, err := bundle.engine.DownTo(ctx, toVersion)
	if err != nil {
		return RunReport{}, fmt.Errorf("dbx/migrate: rollback go migrations to version %d: %w", toVersion, err)
	}
	return buildRunReport(appliedBefore, bundle.metaByVersion, results)
}
