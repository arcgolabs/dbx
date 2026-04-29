package schemamigrate

import (
	"context"
	"database/sql"

	collectionx "github.com/arcgolabs/collectionx/list"
	"github.com/arcgolabs/dbx"
	schemax "github.com/arcgolabs/dbx/schema"
)

func AutoMigrate(ctx context.Context, session dbx.Session, schemas ...Resource) (schemax.ValidationReport, error) {
	report, err := dbx.ObserveOperation(ctx, session, dbx.HookEvent{
		Operation: dbx.OperationAutoMigrate,
		Table:     schemaNames(schemas),
	}, func(ctx context.Context) (schemax.ValidationReport, error) {
		return autoMigrate(ctx, session, schemas...)
	})
	return report, wrapMigrateError("observe auto migrate", err)
}

func autoMigrate(ctx context.Context, session dbx.Session, schemas ...Resource) (schemax.ValidationReport, error) {
	dbx.LogRuntimeNode(session, "schema.auto_migrate.start", "schemas", len(schemas))
	plan, err := PlanSchemaChanges(ctx, session, schemas...)
	if err != nil {
		dbx.LogRuntimeNode(session, "schema.auto_migrate.error", "stage", "plan", "error", err)
		return schemax.ValidationReport{}, err
	}
	if plan.HasManualActions() {
		dbx.LogRuntimeNode(session, "schema.auto_migrate.manual_required", "actions", plan.Actions.Len())
		return plan.Report, schemax.SchemaDriftError{Report: plan.Report}
	}

	report, err := applyMigrationPlan(ctx, session, plan, schemas...)
	if err != nil {
		return schemax.ValidationReport{}, err
	}
	dbx.LogRuntimeNode(session, "schema.auto_migrate.done", "actions", plan.Actions.Len())
	return report, nil
}

func applyMigrationPlan(ctx context.Context, session dbx.Session, plan schemax.MigrationPlan, schemas ...Resource) (schemax.ValidationReport, error) {
	executableActions := plan.ExecutableActions()
	execSession, finalize, rollback, transactional, err := autoMigrateExecutionSession(ctx, session, executableActions.Len() > 0)
	if err != nil {
		dbx.LogRuntimeNode(session, "schema.auto_migrate.error", "stage", "begin_tx", "error", err)
		return schemax.ValidationReport{}, err
	}

	committed := false
	if rollback != nil {
		defer rollbackPendingMigration(session, rollback, &committed)
	}
	execErr := executeMigrationActions(ctx, session, execSession, plan.Actions)
	if execErr != nil {
		return schemax.ValidationReport{}, execErr
	}

	report, err := validateAppliedMigration(ctx, session, execSession, schemas...)
	if err != nil {
		return schemax.ValidationReport{}, err
	}
	report = appendNonTransactionalWarning(report, transactional, executableActions.Len())
	if err := ensureMigrationReportValid(session, report); err != nil {
		return report, err
	}
	if err := finalizeMigration(finalize, &committed, session); err != nil {
		return schemax.ValidationReport{}, err
	}
	return report, nil
}

func rollbackPendingMigration(session dbx.Session, rollback func() error, committed *bool) {
	if *committed {
		return
	}
	if rollbackErr := rollback(); rollbackErr != nil {
		dbx.LogRuntimeNode(session, "schema.auto_migrate.error", "stage", "rollback", "error", rollbackErr)
	}
}

func executeMigrationActions(ctx context.Context, session, execSession dbx.Session, actions *collectionx.List[schemax.MigrationAction]) error {
	var execErr error
	actions.Range(func(_ int, action schemax.MigrationAction) bool {
		if !action.Executable {
			return true
		}
		dbx.LogRuntimeNode(execSession, "schema.auto_migrate.exec_action", "kind", action.Kind, "table", action.Table, "summary", action.Summary)
		if _, err := execSession.ExecBoundContext(ctx, action.Statement); err != nil {
			dbx.LogRuntimeNode(session, "schema.auto_migrate.error", "stage", "exec", "kind", action.Kind, "table", action.Table, "error", err)
			execErr = wrapMigrateError("apply schema migration action", err)
			return false
		}
		return true
	})
	return execErr
}

func validateAppliedMigration(ctx context.Context, session, execSession dbx.Session, schemas ...Resource) (schemax.ValidationReport, error) {
	report, err := validateSchemas(ctx, execSession, schemas...)
	if err != nil {
		dbx.LogRuntimeNode(session, "schema.auto_migrate.error", "stage", "validate", "error", err)
		return schemax.ValidationReport{}, err
	}
	return report, nil
}

func appendNonTransactionalWarning(report schemax.ValidationReport, transactional bool, actionCount int) schemax.ValidationReport {
	if transactional || actionCount <= 1 {
		return report
	}
	return report.WithWarning("dbx: auto migrate executed without transaction; partial application is possible on failure")
}

func ensureMigrationReportValid(session dbx.Session, report schemax.ValidationReport) error {
	if report.Valid() {
		return nil
	}
	dbx.LogRuntimeNode(session, "schema.auto_migrate.invalid_after_apply", "tables", report.Tables.Len())
	return schemax.SchemaDriftError{Report: report}
}

func finalizeMigration(finalize func() error, committed *bool, session dbx.Session) error {
	if finalize == nil {
		return nil
	}
	if err := finalize(); err != nil {
		dbx.LogRuntimeNode(session, "schema.auto_migrate.error", "stage", "commit", "error", err)
		return err
	}
	*committed = true
	return nil
}

type txStarter interface {
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*dbx.Tx, error)
}

func autoMigrateExecutionSession(ctx context.Context, session dbx.Session, needExec bool) (dbx.Session, func() error, func() error, bool, error) {
	if !needExec {
		dbx.LogRuntimeNode(session, "schema.auto_migrate.execution_session", "need_exec", false, "transactional", false)
		return session, nil, nil, false, nil
	}

	starter, ok := session.(txStarter)
	if !ok {
		dbx.LogRuntimeNode(session, "schema.auto_migrate.execution_session", "need_exec", true, "transactional", false, "reason", "session_has_no_begin_tx")
		return session, nil, nil, false, nil
	}

	tx, err := starter.BeginTx(ctx, nil)
	if err != nil {
		dbx.LogRuntimeNode(session, "schema.auto_migrate.execution_session.error", "error", err)
		return nil, nil, nil, false, wrapMigrateError("begin schema migration transaction", err)
	}
	dbx.LogRuntimeNode(session, "schema.auto_migrate.execution_session", "need_exec", true, "transactional", true)
	return tx, tx.Commit, tx.Rollback, true, nil
}
