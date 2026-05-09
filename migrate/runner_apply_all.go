package migrate

import (
	"context"

	collectionx "github.com/arcgolabs/collectionx/list"
)

// MigrationTarget describes an optional target version for up/down operations.
type MigrationTarget struct {
	Version int64
}

// MigrationApplySpec configures a single high-level migration run.
// Low-level methods (`UpGo`, `DownSQLTo`, etc.) remain available and preferred
// when you need full manual control.
type MigrationApplySpec struct {
	Direction    Direction
	Target       *MigrationTarget
	GoMigrations []Migration
	SQLSource    *FileSource
}

// ApplyAll executes migration operations for both Go and SQL sources in one call.
// It delegates to existing low-level methods and merges their reports.
func (r *Runner) ApplyAll(ctx context.Context, spec MigrationApplySpec) (RunReport, error) {
	if spec.Direction != DirectionUp && spec.Direction != DirectionDown {
		spec.Direction = DirectionUp
	}

	total := RunReport{
		Applied: collectionx.NewList[AppliedRecord](),
	}

	var (
		report RunReport
		err    error
	)

	switch spec.Direction {
	case DirectionUp:
		report, err = r.applyAllGoUp(ctx, spec.GoMigrations, spec.Target)
		if err != nil {
			return RunReport{}, err
		}
		total.Applied.Merge(report.Applied)

		report, err = r.applyAllSQLUp(ctx, spec.SQLSource, spec.Target)
		if err != nil {
			return RunReport{}, err
		}
		total.Applied.Merge(report.Applied)
	case DirectionDown:
		report, err = r.applyAllGoDown(ctx, spec.GoMigrations, spec.Target)
		if err != nil {
			return RunReport{}, err
		}
		total.Applied.Merge(report.Applied)

		report, err = r.applyAllSQLDown(ctx, spec.SQLSource, spec.Target)
		if err != nil {
			return RunReport{}, err
		}
		total.Applied.Merge(report.Applied)
	}

	return total, nil
}

func (r *Runner) applyAllGoUp(ctx context.Context, migrations []Migration, target *MigrationTarget) (RunReport, error) {
	if len(migrations) == 0 {
		return RunReport{Applied: collectionx.NewList[AppliedRecord]()}, nil
	}

	if target == nil {
		return r.UpGo(ctx, migrations...)
	}

	return r.UpGoTo(ctx, target.Version, migrations...)
}

func (r *Runner) applyAllSQLUp(ctx context.Context, source *FileSource, target *MigrationTarget) (RunReport, error) {
	if source == nil {
		return RunReport{Applied: collectionx.NewList[AppliedRecord]()}, nil
	}

	if target == nil {
		return r.UpSQL(ctx, *source)
	}

	return r.UpSQLTo(ctx, target.Version, *source)
}

func (r *Runner) applyAllGoDown(ctx context.Context, migrations []Migration, target *MigrationTarget) (RunReport, error) {
	if len(migrations) == 0 {
		return RunReport{Applied: collectionx.NewList[AppliedRecord]()}, nil
	}

	if target == nil {
		target = &MigrationTarget{Version: 0}
	}

	return r.DownGoTo(ctx, target.Version, migrations...)
}

func (r *Runner) applyAllSQLDown(ctx context.Context, source *FileSource, target *MigrationTarget) (RunReport, error) {
	if source == nil {
		return RunReport{Applied: collectionx.NewList[AppliedRecord]()}, nil
	}

	if target == nil {
		target = &MigrationTarget{Version: 0}
	}

	return r.DownSQLTo(ctx, target.Version, *source)
}
