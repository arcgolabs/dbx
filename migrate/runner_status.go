package migrate

import (
	"context"
	"fmt"
	"time"

	collectionx "github.com/arcgolabs/collectionx/list"
	mappingx "github.com/arcgolabs/collectionx/mapping"
	"github.com/pressly/goose/v3"
)

// MigrationState describes a migration lifecycle state in runner status output.
type MigrationState string

const (
	// MigrationStateApplied means the migration is present in the history table and current checksum matches.
	MigrationStateApplied MigrationState = "applied"
	// MigrationStatePending means the migration exists in filesystem but has no matching history entry.
	MigrationStatePending MigrationState = "pending"
	// MigrationStateOutdated means the migration is in history but checksum differs from source.
	MigrationStateOutdated MigrationState = "outdated"
)

// MigrationStatus describes one migration and its current runtime status.
type MigrationStatus struct {
	Version     string
	Description string
	Kind        Kind
	State       MigrationState
	AppliedAt   *time.Time
}

// StatusGo returns the current status for provided Go migrations.
func (r *Runner) StatusGo(ctx context.Context, migrations ...Migration) (*collectionx.List[MigrationStatus], error) {
	migrations = r.filterGoMigrationsByDialect(migrations)
	bundle, err := r.newRunnerEngineForGo(migrations)
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

// StatusSQL returns versioned and repeatable SQL migration statuses from source files and history.
func (r *Runner) StatusSQL(ctx context.Context, source FileSource) (*collectionx.List[MigrationStatus], error) {
	bundle, repeatables, err := r.newRunnerEngineForSQL(source)
	if err != nil {
		return nil, err
	}
	if repeatables == nil {
		repeatables = collectionx.NewList[loadedSQLMigration]()
	}

	indexed, err := r.appliedIndex(ctx)
	if err != nil {
		return nil, err
	}

	versionedStatuses := collectionx.NewList[MigrationStatus]()
	if bundle != nil && bundle.engine != nil {
		indexedByVersion, err := indexVersionedSQLMigrations(source)
		if err != nil {
			return nil, err
		}
		statuses, err := pendingStatuses(ctx, bundle.engine, "sql")
		if err != nil {
			return nil, err
		}
		versionedStatuses = buildSQLVersionedStatuses(statuses, bundle.metaByVersion, indexedByVersion, indexed, r.options.ValidateHash)
	}

	repeatableStatuses := buildRepeatableStatuses(repeatables, indexed)
	return versionedStatuses.Merge(repeatableStatuses), nil
}

func buildGoMigrationStatuses(
	statuses []*goose.MigrationStatus,
	metaByVersion *mappingx.Map[int64, AppliedRecord],
	applied map[string]AppliedRecord,
	byVersion map[int64]Migration,
	validateHash bool,
) *collectionx.List[MigrationStatus] {
	out := collectionx.NewListWithCapacity[MigrationStatus](len(statuses))
	collectionx.NewList[*goose.MigrationStatus](statuses...).Range(func(_ int, status *goose.MigrationStatus) bool {
		_, ok := byVersion[status.Source.Version]
		if !ok {
			return true
		}

		record, ok := metaByVersion.Get(status.Source.Version)
		if !ok {
			return true
		}

		out.Add(buildMigrationStatusFromRecord(status.State, record, applied, validateHash))
		return true
	})
	return out
}

func buildSQLVersionedStatuses(
	statuses []*goose.MigrationStatus,
	metaByVersion *mappingx.Map[int64, AppliedRecord],
	versioned map[int64]SQLMigration,
	applied map[string]AppliedRecord,
	validateHash bool,
) *collectionx.List[MigrationStatus] {
	out := collectionx.NewListWithCapacity[MigrationStatus](len(statuses))
	collectionx.NewList[*goose.MigrationStatus](statuses...).Range(func(_ int, status *goose.MigrationStatus) bool {
		_, ok := versioned[status.Source.Version]
		if !ok {
			return true
		}

		record, ok := metaByVersion.Get(status.Source.Version)
		if !ok {
			return true
		}

		out.Add(buildMigrationStatusFromRecord(status.State, record, applied, validateHash))
		return true
	})
	return out
}

func buildRepeatableStatuses(repeatables *collectionx.List[loadedSQLMigration], applied map[string]AppliedRecord) *collectionx.List[MigrationStatus] {
	return collectionx.FilterMapList[loadedSQLMigration, MigrationStatus](repeatables, func(_ int, migration loadedSQLMigration) (MigrationStatus, bool) {
		record := AppliedRecord{
			Version:     migration.Version,
			Description: migration.Description,
			Kind:        migration.kind,
			Checksum:    migration.checksum,
		}
		key := appliedRecordKey(record.Kind, record.Version, record.Description)
		existing, ok := applied[key]
		if !ok {
			return MigrationStatus{
				Version:     record.Version,
				Description: record.Description,
				Kind:        record.Kind,
				State:       MigrationStatePending,
			}, true
		}

		status := MigrationStateApplied
		if existing.Checksum != record.Checksum {
			status = MigrationStateOutdated
		}

		stateAppliedAt := existing.AppliedAt
		return MigrationStatus{
			Version:     record.Version,
			Description: record.Description,
			Kind:        record.Kind,
			State:       status,
			AppliedAt:   &stateAppliedAt,
		}, true
	})
}

func buildMigrationStatusFromRecord(
	state goose.State,
	record AppliedRecord,
	applied map[string]AppliedRecord,
	validateHash bool,
) MigrationStatus {
	item := MigrationStatus{
		Version:     record.Version,
		Description: record.Description,
		Kind:        record.Kind,
	}

	if state != goose.StatePending {
		item.State = MigrationStateApplied
	} else {
		item.State = MigrationStatePending
		return item
	}

	key := appliedRecordKey(record.Kind, record.Version, record.Description)
	if existing, ok := applied[key]; ok {
		if validateHash && existing.Checksum != record.Checksum {
			item.State = MigrationStateOutdated
		}
		timestamp := existing.AppliedAt
		item.AppliedAt = &timestamp
	}
	return item
}

func (r *MigrationStatus) String() string {
	if r == nil {
		return ""
	}
	return fmt.Sprintf("%s %s %s %s", r.State, r.Kind, r.Version, r.Description)
}
