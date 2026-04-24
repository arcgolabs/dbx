package migrate

import (
	"context"
	"fmt"
	"time"

	"github.com/arcgolabs/collectionx"
	"github.com/pressly/goose/v3"
)

// UpGo applies the provided Go migrations.
func (r *Runner) UpGo(ctx context.Context, migrations ...Migration) (RunReport, error) {
	bundle, err := r.newRunnerEngineForGo(migrations)
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

// UpSQL applies versioned and repeatable SQL migrations from source.
func (r *Runner) UpSQL(ctx context.Context, source FileSource) (RunReport, error) {
	bundle, repeatables, err := r.newRunnerEngineForSQL(source)
	if err != nil {
		return RunReport{}, err
	}

	report := RunReport{Applied: collectionx.NewListWithCapacity[AppliedRecord](8)}
	applied, err := r.versionedSQLRunReport(ctx, bundle)
	if err != nil {
		return report, err
	}
	report.Applied.Merge(applied)

	indexed, err := r.appliedIndex(ctx)
	if err != nil {
		return report, err
	}
	repeatableRecords, err := r.applyPendingRepeatables(ctx, repeatables, indexed)
	if err != nil {
		return report, err
	}
	report.Applied.Merge(repeatableRecords)
	return report, nil
}

func (r *Runner) versionedSQLRunReport(ctx context.Context, bundle *runnerEngine) (collectionx.List[AppliedRecord], error) {
	if bundle == nil || bundle.engine == nil {
		return collectionx.NewList[AppliedRecord](), nil
	}

	results, err := bundle.engine.Up(ctx)
	if err != nil {
		return nil, fmt.Errorf("dbx/migrate: apply sql migrations: %w", err)
	}
	applied, err := r.Applied(ctx)
	if err != nil {
		return nil, err
	}
	report, err := buildRunReport(applied, bundle.metaByVersion, results)
	if err != nil {
		return nil, err
	}
	return report.Applied, nil
}

func buildRunReport(
	applied collectionx.List[AppliedRecord],
	metaByVersion collectionx.Map[int64, AppliedRecord],
	results []*goose.MigrationResult,
) (RunReport, error) {
	reportApplied, err := collectionx.ReduceErrList[*goose.MigrationResult, collectionx.List[AppliedRecord]](
		collectionx.NewList[*goose.MigrationResult](results...),
		collectionx.NewListWithCapacity[AppliedRecord](len(results)),
		func(items collectionx.List[AppliedRecord], _ int, result *goose.MigrationResult) (collectionx.List[AppliedRecord], error) {
			record, ok := metaByVersion.Get(result.Source.Version)
			if !ok {
				return items, nil
			}
			current, currentErr := appliedRecordForVersion(applied, record)
			if currentErr != nil {
				return nil, currentErr
			}
			items.Add(current)
			return items, nil
		},
	)
	if err != nil {
		return RunReport{}, fmt.Errorf("dbx/migrate: build run report: %w", err)
	}
	return RunReport{Applied: reportApplied}, nil
}

func (r *Runner) applyPendingRepeatables(
	ctx context.Context,
	repeatables collectionx.List[loadedSQLMigration],
	indexed map[string]AppliedRecord,
) (collectionx.List[AppliedRecord], error) {
	applied, err := collectionx.ReduceErrList[loadedSQLMigration, collectionx.List[AppliedRecord]](
		repeatables,
		collectionx.NewListWithCapacity[AppliedRecord](repeatables.Len()),
		func(items collectionx.List[AppliedRecord], _ int, migration loadedSQLMigration) (collectionx.List[AppliedRecord], error) {
			if !shouldApplyRepeatableMigration(migration, indexed) {
				return items, nil
			}
			record, recordErr := r.applySQLMigration(ctx, migration)
			if recordErr != nil {
				return nil, recordErr
			}
			items.Add(record)
			return items, nil
		},
	)
	if err != nil {
		return nil, fmt.Errorf("dbx/migrate: apply pending repeatables: %w", err)
	}
	return applied, nil
}

func shouldApplyRepeatableMigration(migration loadedSQLMigration, indexed map[string]AppliedRecord) bool {
	key := appliedRecordKey(migration.kind, migration.Version, migration.Description)
	record, ok := indexed[key]
	return !ok || record.Checksum != migration.checksum
}

func (r *Runner) applySQLMigration(ctx context.Context, migration loadedSQLMigration) (_ AppliedRecord, resultErr error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return AppliedRecord{}, fmt.Errorf("dbx/migrate: begin sql migration %s transaction: %w", migration.Version, err)
	}

	committed := false
	defer func() {
		if committed {
			return
		}
		if rollbackErr := tx.Rollback(); rollbackErr != nil && resultErr == nil {
			resultErr = fmt.Errorf("dbx/migrate: rollback sql migration %s: %w", migration.Version, rollbackErr)
		}
	}()

	if _, err := tx.ExecContext(ctx, migration.upSQL); err != nil {
		return AppliedRecord{}, fmt.Errorf("dbx/migrate: execute sql migration %s: %w", migration.Version, err)
	}

	record := AppliedRecord{
		Version:     migration.Version,
		Description: migration.Description,
		Kind:        migration.kind,
		AppliedAt:   time.Now().UTC(),
		Checksum:    migration.checksum,
		Success:     true,
	}
	if err := replaceAppliedRecord(ctx, tx, r.dialect, r.options.HistoryTable, record); err != nil {
		return AppliedRecord{}, err
	}
	if err := tx.Commit(); err != nil {
		return AppliedRecord{}, fmt.Errorf("dbx/migrate: commit sql migration %s: %w", migration.Version, err)
	}
	committed = true
	return record, nil
}
