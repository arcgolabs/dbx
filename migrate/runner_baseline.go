package migrate

import (
	"context"
	"database/sql"
	"time"

	collectionx "github.com/arcgolabs/collectionx/list"
)

// Baseline records migrations as applied without executing them.
func (r *Runner) Baseline(ctx context.Context, records ...AppliedRecord) (*collectionx.List[AppliedRecord], error) {
	if r == nil || r.db == nil {
		return nil, sql.ErrConnDone
	}
	if err := r.EnsureHistory(ctx); err != nil {
		return nil, err
	}
	return collectionx.ReduceErrList[AppliedRecord, *collectionx.List[AppliedRecord]](
		collectionx.NewList[AppliedRecord](records...),
		collectionx.NewListWithCapacity[AppliedRecord](len(records)),
		func(applied *collectionx.List[AppliedRecord], _ int, record AppliedRecord) (*collectionx.List[AppliedRecord], error) {
			record = normalizeBaselineRecord(record)
			if err := replaceAppliedRecordOnConn(ctx, r.db, r.dialect, r.options.HistoryTable, record); err != nil {
				return nil, err
			}
			applied.Add(record)
			return applied, nil
		},
	)
}

// BaselineGo records Go migrations as applied without executing them.
func (r *Runner) BaselineGo(ctx context.Context, migrations ...Migration) (*collectionx.List[AppliedRecord], error) {
	records := collectionx.MapList[Migration, AppliedRecord](collectionx.NewList[Migration](migrations...), func(_ int, migration Migration) AppliedRecord {
		return AppliedRecord{
			Version:     migration.Version(),
			Description: migration.Description(),
			Kind:        KindGo,
			Checksum:    checksumGoMigration(migration),
		}
	})
	return r.baselineRecordList(ctx, records)
}

// BaselineSQL records SQL migrations as applied without executing them.
func (r *Runner) BaselineSQL(ctx context.Context, source FileSource) (*collectionx.List[AppliedRecord], error) {
	loaded, err := loadSQLMigrations(r.sqlSourceForDialect(source))
	if err != nil {
		return nil, err
	}
	records := collectionx.MapList[loadedSQLMigration, AppliedRecord](loaded, func(_ int, migration loadedSQLMigration) AppliedRecord {
		return AppliedRecord{
			Version:     migration.Version,
			Description: migration.Description,
			Kind:        migration.kind,
			Checksum:    migration.checksum,
		}
	})
	return r.baselineRecordList(ctx, records)
}

func (r *Runner) baselineRecordList(ctx context.Context, records *collectionx.List[AppliedRecord]) (*collectionx.List[AppliedRecord], error) {
	var applied *collectionx.List[AppliedRecord]
	var err error
	records.ViewValues(func(values []AppliedRecord) {
		applied, err = r.Baseline(ctx, values...)
	})
	return applied, err
}

// RepairGo replaces Go migration history checksums with the current migration metadata.
func (r *Runner) RepairGo(ctx context.Context, migrations ...Migration) (*collectionx.List[AppliedRecord], error) {
	return r.BaselineGo(ctx, migrations...)
}

// RepairSQL replaces SQL migration history checksums with the current source metadata.
func (r *Runner) RepairSQL(ctx context.Context, source FileSource) (*collectionx.List[AppliedRecord], error) {
	return r.BaselineSQL(ctx, source)
}

func normalizeBaselineRecord(record AppliedRecord) AppliedRecord {
	if record.AppliedAt.IsZero() {
		record.AppliedAt = time.Now().UTC()
	}
	record.Success = true
	return record
}
