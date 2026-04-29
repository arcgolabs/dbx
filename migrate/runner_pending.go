package migrate

import (
	"context"
	"fmt"

	collectionx "github.com/arcgolabs/collectionx/list"
	mappingx "github.com/arcgolabs/collectionx/mapping"
	"github.com/pressly/goose/v3"
)

// PendingGo returns Go migrations that have not yet been applied.
func (r *Runner) PendingGo(ctx context.Context, migrations ...Migration) (*collectionx.List[Migration], error) {
	bundle, err := r.newRunnerEngineForGo(migrations)
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

// PendingSQL returns SQL migrations that should be applied next.
func (r *Runner) PendingSQL(ctx context.Context, source FileSource) (*collectionx.List[SQLMigration], error) {
	bundle, repeatables, err := r.newRunnerEngineForSQL(source)
	if err != nil {
		return nil, err
	}
	indexed, err := r.appliedIndex(ctx)
	if err != nil {
		return nil, err
	}

	pending := collectionx.NewList[SQLMigration]()
	if bundle != nil && bundle.engine != nil {
		versionedPending, pendingErr := r.pendingVersionedSQL(ctx, source, bundle, indexed)
		if pendingErr != nil {
			return nil, pendingErr
		}
		pending.Merge(versionedPending)
	}

	pending.Merge(pendingRepeatableMigrations(repeatables, indexed))
	return pending, nil
}

func pendingStatuses(ctx context.Context, engine *goose.Provider, kind string) ([]*goose.MigrationStatus, error) {
	if _, err := engine.HasPending(ctx); err != nil {
		return nil, fmt.Errorf("dbx/migrate: check %s migration pending state: %w", kind, err)
	}

	statuses, err := engine.Status(ctx)
	if err != nil {
		return nil, fmt.Errorf("dbx/migrate: load %s migration status: %w", kind, err)
	}
	return statuses, nil
}

func (r *Runner) appliedIndex(ctx context.Context) (map[string]AppliedRecord, error) {
	applied, err := r.Applied(ctx)
	if err != nil {
		return nil, err
	}
	return indexAppliedRecords(applied), nil
}

func indexGoMigrationsByVersion(migrations []Migration) (map[int64]Migration, error) {
	byVersion, err := collectionx.ReduceErrList[Migration, map[int64]Migration](
		collectionx.NewList[Migration](migrations...),
		make(map[int64]Migration, len(migrations)),
		func(result map[int64]Migration, _ int, migration Migration) (map[int64]Migration, error) {
			version, parseErr := parseNumericVersion(migration.Version())
			if parseErr != nil {
				return nil, fmt.Errorf("dbx/migrate: parse go migration version %q: %w", migration.Version(), parseErr)
			}
			result[version] = migration
			return result, nil
		},
	)
	if err != nil {
		return nil, fmt.Errorf("dbx/migrate: index sql migrations by version: %w", err)
	}
	return byVersion, nil
}

func collectPendingGoMigrations(
	statuses []*goose.MigrationStatus,
	metaByVersion *mappingx.Map[int64, AppliedRecord],
	indexed map[string]AppliedRecord,
	byVersion map[int64]Migration,
	validateHash bool,
) (*collectionx.List[Migration], error) {
	return collectPendingMigrations(statuses, metaByVersion, indexed, byVersion, validateHash, "go")
}

func (r *Runner) pendingVersionedSQL(
	ctx context.Context,
	source FileSource,
	bundle *runnerEngine,
	indexed map[string]AppliedRecord,
) (*collectionx.List[SQLMigration], error) {
	statuses, err := pendingStatuses(ctx, bundle.engine, "sql")
	if err != nil {
		return nil, err
	}

	versionedByVersion, err := indexVersionedSQLMigrations(source)
	if err != nil {
		return nil, err
	}
	return collectPendingSQLMigrations(statuses, bundle.metaByVersion, indexed, versionedByVersion, r.options.ValidateHash)
}

func indexVersionedSQLMigrations(source FileSource) (map[int64]SQLMigration, error) {
	loaded, err := loadSQLMigrations(source)
	if err != nil {
		return nil, err
	}

	byVersion, err := collectionx.ReduceErrList[loadedSQLMigration, map[int64]SQLMigration](loaded, make(map[int64]SQLMigration, loaded.Len()), func(result map[int64]SQLMigration, _ int, migration loadedSQLMigration) (map[int64]SQLMigration, error) {
		if migration.Repeatable {
			return result, nil
		}
		version, parseErr := parseNumericVersion(migration.Version)
		if parseErr != nil {
			return nil, fmt.Errorf("dbx/migrate: parse sql migration version %q: %w", migration.Version, parseErr)
		}
		result[version] = migration.SQLMigration
		return result, nil
	})
	if err != nil {
		return nil, fmt.Errorf("dbx/migrate: index sql migrations by version: %w", err)
	}
	return byVersion, nil
}

func collectPendingSQLMigrations(
	statuses []*goose.MigrationStatus,
	metaByVersion *mappingx.Map[int64, AppliedRecord],
	indexed map[string]AppliedRecord,
	byVersion map[int64]SQLMigration,
	validateHash bool,
) (*collectionx.List[SQLMigration], error) {
	return collectPendingMigrations(statuses, metaByVersion, indexed, byVersion, validateHash, "sql")
}

func collectPendingMigrations[T any](
	statuses []*goose.MigrationStatus,
	metaByVersion *mappingx.Map[int64, AppliedRecord],
	indexed map[string]AppliedRecord,
	byVersion map[int64]T,
	validateHash bool,
	kind string,
) (*collectionx.List[T], error) {
	pending, err := collectionx.ReduceErrList[*goose.MigrationStatus, *collectionx.List[T]](
		collectionx.NewList[*goose.MigrationStatus](statuses...),
		collectionx.NewListWithCapacity[T](len(statuses)),
		func(result *collectionx.List[T], _ int, status *goose.MigrationStatus) (*collectionx.List[T], error) {
			migration, ok := byVersion[status.Source.Version]
			if !ok {
				return result, nil
			}
			if err := validatePendingStatus(status, metaByVersion, indexed, validateHash); err != nil {
				return nil, err
			}
			if status.State != goose.StatePending {
				return result, nil
			}
			result.Add(migration)
			return result, nil
		},
	)
	if err != nil {
		return nil, fmt.Errorf("dbx/migrate: collect pending %s migrations: %w", kind, err)
	}
	return pending, nil
}

func validatePendingStatus(
	status *goose.MigrationStatus,
	metaByVersion *mappingx.Map[int64, AppliedRecord],
	indexed map[string]AppliedRecord,
	validateHash bool,
) error {
	if !validateHash || status.State == goose.StatePending {
		return nil
	}

	record, ok := metaByVersion.Get(status.Source.Version)
	if !ok {
		return nil
	}
	existing, exists := indexed[appliedRecordKey(record.Kind, record.Version, record.Description)]
	if exists && existing.Checksum != record.Checksum {
		return fmt.Errorf("dbx/migrate: migration checksum mismatch for version %s", record.Version)
	}
	return nil
}

func pendingRepeatableMigrations(repeatables *collectionx.List[loadedSQLMigration], indexed map[string]AppliedRecord) *collectionx.List[SQLMigration] {
	return collectionx.FilterMapList[loadedSQLMigration, SQLMigration](repeatables, func(_ int, migration loadedSQLMigration) (SQLMigration, bool) {
		key := appliedRecordKey(migration.kind, migration.Version, migration.Description)
		record, ok := indexed[key]
		if ok && record.Checksum == migration.checksum {
			return SQLMigration{}, false
		}
		return migration.SQLMigration, true
	})
}
