package schemamigrate

import (
	"context"
	"fmt"
	"strings"

	"github.com/DaiYuANg/arcgo/collectionx"
	"github.com/arcgolabs/dbx"
	schemax "github.com/arcgolabs/dbx/schema"
	"github.com/samber/lo"
)

func PlanSchemaChanges(ctx context.Context, session dbx.Session, schemas ...Resource) (schemax.MigrationPlan, error) {
	dbx.LogRuntimeNode(session, "schema.plan.start", "schemas", len(schemas))
	if plan, ok, err := planSchemaChangesWithAtlas(ctx, session, schemas...); ok || err != nil {
		logPlanSchemaChangesResult(session, "atlas", plan, err)
		return plan, err
	}

	plan, err := planSchemaChangesLegacy(ctx, session, schemas...)
	logPlanSchemaChangesResult(session, "legacy", plan, err)
	return plan, err
}

func logPlanSchemaChangesResult(session dbx.Session, backend string, plan schemax.MigrationPlan, err error) {
	if err != nil {
		dbx.LogRuntimeNode(session, "schema.plan.error", "backend", backend, "error", err)
		return
	}
	dbx.LogRuntimeNode(session, "schema.plan.done", "backend", backend, "actions", plan.Actions.Len(), "manual_actions", plan.HasManualActions())
}

func planSchemaChangesLegacy(ctx context.Context, session dbx.Session, schemas ...Resource) (schemax.MigrationPlan, error) {
	schemaDialect, err := requireSchemaDialect(session)
	if err != nil {
		return schemax.MigrationPlan{}, err
	}

	reportTables := collectionx.NewListWithCapacity[schemax.TableDiff](len(schemas))
	actions := collectionx.NewListWithCapacity[schemax.MigrationAction](len(schemas))
	for _, schema := range schemas {
		diff, err := planLegacySchemaDiff(ctx, schemaDialect, session, schema)
		if err != nil {
			return schemax.MigrationPlan{}, err
		}
		reportTables.Add(diff)
		actions.Merge(buildLegacyMigrationActions(schemaDialect, schema, diff))
	}

	return schemax.MigrationPlan{
		Actions: actions,
		Report: schemax.ValidationReport{
			Tables:   reportTables,
			Backend:  schemax.ValidationBackendLegacy,
			Complete: false,
			Warnings: collectionx.NewList[string]("dbx: schema validation is running in legacy mode; extra drift may not be reported"),
		},
	}, nil
}

func planLegacySchemaDiff(ctx context.Context, schemaDialect Dialect, session dbx.Session, schema Resource) (schemax.TableDiff, error) {
	table := schema.TableName()
	dbx.LogRuntimeNode(session, "schema.plan.legacy.diff", "table", table)
	diff, err := diffSchema(ctx, schemaDialect, session, schema)
	if err != nil {
		dbx.LogRuntimeNode(session, "schema.plan.legacy.error", "table", table, "error", err)
		return schemax.TableDiff{}, err
	}
	logLegacyDiffSummary(session, diff)
	return diff, nil
}

func logLegacyDiffSummary(session dbx.Session, diff schemax.TableDiff) {
	dbx.LogRuntimeNode(session,
		"schema.plan.legacy.diff_done",
		"table", diff.Table,
		"missing_table", diff.MissingTable,
		"missing_columns", diff.MissingColumns.Len(),
		"missing_indexes", diff.MissingIndexes.Len(),
		"missing_foreign_keys", diff.MissingForeignKeys.Len(),
		"missing_checks", diff.MissingChecks.Len(),
		"column_diffs", diff.ColumnDiffs.Len(),
	)
}

func buildLegacyMigrationActions(schemaDialect Dialect, schema Resource, diff schemax.TableDiff) collectionx.List[schemax.MigrationAction] {
	spec := schema.Spec()
	if diff.MissingTable {
		return buildMissingTableActions(schemaDialect, spec)
	}
	return buildExistingTableActions(schemaDialect, diff)
}

func buildMissingTableActions(schemaDialect Dialect, spec schemax.TableSpec) collectionx.List[schemax.MigrationAction] {
	actions := collectionx.NewList[schemax.MigrationAction](buildCreateTableAction(schemaDialect, spec))
	actions.Merge(mappedMigrationActions(spec.Indexes, func(index schemax.IndexMeta) schemax.MigrationAction {
		return buildCreateIndexAction(schemaDialect, index)
	}))
	return actions
}

func buildExistingTableActions(schemaDialect Dialect, diff schemax.TableDiff) collectionx.List[schemax.MigrationAction] {
	actions := collectionx.NewListWithCapacity[schemax.MigrationAction](
		diff.MissingColumns.Len() +
			diff.MissingIndexes.Len() +
			diff.MissingForeignKeys.Len() +
			diff.MissingChecks.Len() +
			diff.ColumnDiffs.Len() + 1,
	)
	actions.Merge(mappedMigrationActions(diff.MissingColumns, func(column schemax.ColumnMeta) schemax.MigrationAction {
		return buildAddColumnAction(schemaDialect, diff.Table, column)
	}))
	actions.Merge(mappedMigrationActions(diff.MissingIndexes, func(index schemax.IndexMeta) schemax.MigrationAction {
		return buildCreateIndexAction(schemaDialect, index)
	}))
	actions.Merge(mappedMigrationActions(diff.MissingForeignKeys, func(foreignKey schemax.ForeignKeyMeta) schemax.MigrationAction {
		return buildAddForeignKeyAction(schemaDialect, diff.Table, foreignKey)
	}))
	actions.Merge(mappedMigrationActions(diff.MissingChecks, func(check schemax.CheckMeta) schemax.MigrationAction {
		return buildAddCheckAction(schemaDialect, diff.Table, check)
	}))
	actions.Merge(primaryKeyManualActions(diff))
	actions.Merge(columnDiffManualActions(diff))
	return actions
}

func ValidateSchemas(ctx context.Context, session dbx.Session, schemas ...Resource) (schemax.ValidationReport, error) {
	report, err := dbx.ObserveOperation(ctx, session, dbx.HookEvent{
		Operation: dbx.OperationValidate,
		Table:     schemaNames(schemas),
	}, func(ctx context.Context) (schemax.ValidationReport, error) {
		return validateSchemas(ctx, session, schemas...)
	})
	return report, wrapMigrateError("observe validate schemas", err)
}

func validateSchemas(ctx context.Context, session dbx.Session, schemas ...Resource) (schemax.ValidationReport, error) {
	dbx.LogRuntimeNode(session, "schema.validate.start", "schemas", len(schemas))
	plan, err := PlanSchemaChanges(ctx, session, schemas...)
	if err != nil {
		dbx.LogRuntimeNode(session, "schema.validate.error", "error", err)
		return schemax.ValidationReport{}, err
	}
	dbx.LogRuntimeNode(session, "schema.validate.done", "valid", plan.Report.Valid(), "tables", plan.Report.Tables.Len())
	return plan.Report, nil
}

func requireSchemaDialect(session dbx.Session) (Dialect, error) {
	if session == nil {
		return nil, dbx.ErrNilDB
	}
	if session.Dialect() == nil {
		return nil, dbx.ErrNilDialect
	}
	schemaDialect, ok := session.Dialect().(Dialect)
	if !ok {
		return nil, fmt.Errorf("dbx/schemamigrate: dialect %T does not implement schema migration support", session.Dialect())
	}
	return schemaDialect, nil
}

func schemaNames(schemas []Resource) string {
	if len(schemas) == 0 {
		return ""
	}
	return strings.Join(lo.Map(schemas, func(schema Resource, _ int) string {
		return schema.TableName()
	}), ",")
}

func primaryKeyManualActions(diff schemax.TableDiff) collectionx.List[schemax.MigrationAction] {
	if diff.PrimaryKeyDiff == nil {
		return collectionx.NewList[schemax.MigrationAction]()
	}
	return collectionx.NewList[schemax.MigrationAction](schemax.MigrationAction{
		Kind:    schemax.MigrationActionManual,
		Table:   diff.Table,
		Summary: "manual primary key migration required",
	})
}
