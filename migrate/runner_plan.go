package migrate

import (
	"context"

	collectionx "github.com/arcgolabs/collectionx/list"
)

// MigrationPlan describes migrations that would run without executing them.
type MigrationPlan struct {
	Go  *collectionx.List[Migration]
	SQL *collectionx.List[SQLMigration]
}

// IsEmpty reports whether the plan has no pending migrations.
func (p MigrationPlan) IsEmpty() bool {
	return (p.Go == nil || p.Go.IsEmpty()) && (p.SQL == nil || p.SQL.IsEmpty())
}

// PlanGo returns pending Go migrations without applying them.
func (r *Runner) PlanGo(ctx context.Context, migrations ...Migration) (*collectionx.List[Migration], error) {
	return r.PendingGo(ctx, migrations...)
}

// PlanSQL returns pending SQL migrations without applying them.
func (r *Runner) PlanSQL(ctx context.Context, source FileSource) (*collectionx.List[SQLMigration], error) {
	return r.PendingSQL(ctx, source)
}

// PlanAll returns pending Go and SQL migrations without applying them.
func (r *Runner) PlanAll(ctx context.Context, migrations []Migration, source *FileSource) (MigrationPlan, error) {
	goPlan, err := r.PlanGo(ctx, migrations...)
	if err != nil {
		return MigrationPlan{}, err
	}
	sqlPlan := collectionx.NewList[SQLMigration]()
	if source != nil {
		sqlPlan, err = r.PlanSQL(ctx, *source)
		if err != nil {
			return MigrationPlan{}, err
		}
	}
	return MigrationPlan{Go: goPlan, SQL: sqlPlan}, nil
}

// DryRunGo is an alias for PlanGo.
func (r *Runner) DryRunGo(ctx context.Context, migrations ...Migration) (*collectionx.List[Migration], error) {
	return r.PlanGo(ctx, migrations...)
}

// DryRunSQL is an alias for PlanSQL.
func (r *Runner) DryRunSQL(ctx context.Context, source FileSource) (*collectionx.List[SQLMigration], error) {
	return r.PlanSQL(ctx, source)
}

// DryRunAll is an alias for PlanAll.
func (r *Runner) DryRunAll(ctx context.Context, migrations []Migration, source *FileSource) (MigrationPlan, error) {
	return r.PlanAll(ctx, migrations, source)
}
