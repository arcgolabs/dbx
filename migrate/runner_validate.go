package migrate

import (
	"context"
	"fmt"

	collectionx "github.com/arcgolabs/collectionx/list"
)

// MigrationValidationIssue describes one migration validation problem.
type MigrationValidationIssue struct {
	Version     string
	Description string
	Kind        Kind
	State       MigrationState
	Message     string
}

// MigrationValidationReport describes validation results for one migration source.
type MigrationValidationReport struct {
	Issues *collectionx.List[MigrationValidationIssue]
}

// Valid reports whether no validation issues were found.
func (r MigrationValidationReport) Valid() bool {
	return r.Issues == nil || r.Issues.IsEmpty()
}

// ValidateGo checks Go migration history against the provided migrations.
func (r *Runner) ValidateGo(ctx context.Context, migrations ...Migration) (MigrationValidationReport, error) {
	statuses, err := r.validateHashRunner().StatusGo(ctx, migrations...)
	if err != nil {
		return MigrationValidationReport{}, err
	}
	return validateMigrationStatuses(statuses), nil
}

// ValidateSQL checks SQL migration history against source.
func (r *Runner) ValidateSQL(ctx context.Context, source FileSource) (MigrationValidationReport, error) {
	statuses, err := r.validateHashRunner().StatusSQL(ctx, source)
	if err != nil {
		return MigrationValidationReport{}, err
	}
	return validateMigrationStatuses(statuses), nil
}

// MigrationValidationBundle contains validation reports grouped by source.
type MigrationValidationBundle struct {
	Go  MigrationValidationReport
	SQL MigrationValidationReport
}

// Valid reports whether both Go and SQL validation reports have no issues.
func (b MigrationValidationBundle) Valid() bool {
	return b.Go.Valid() && b.SQL.Valid()
}

// ValidateAll checks Go and SQL migration history in one call.
func (r *Runner) ValidateAll(ctx context.Context, migrations []Migration, source *FileSource) (MigrationValidationBundle, error) {
	goReport, err := r.ValidateGo(ctx, migrations...)
	if err != nil {
		return MigrationValidationBundle{}, err
	}
	sqlReport := MigrationValidationReport{Issues: collectionx.NewList[MigrationValidationIssue]()}
	if source != nil {
		sqlReport, err = r.ValidateSQL(ctx, *source)
		if err != nil {
			return MigrationValidationBundle{}, err
		}
	}
	return MigrationValidationBundle{Go: goReport, SQL: sqlReport}, nil
}

func (r *Runner) validateHashRunner() *Runner {
	if r == nil {
		return r
	}
	cloned := *r
	cloned.options.ValidateHash = true
	return &cloned
}

func validateMigrationStatuses(statuses *collectionx.List[MigrationStatus]) MigrationValidationReport {
	if statuses == nil {
		return MigrationValidationReport{Issues: collectionx.NewList[MigrationValidationIssue]()}
	}
	issues := collectionx.FilterMapList[MigrationStatus, MigrationValidationIssue](statuses, func(_ int, status MigrationStatus) (MigrationValidationIssue, bool) {
		if status.State != MigrationStateOutdated {
			return MigrationValidationIssue{}, false
		}
		return MigrationValidationIssue{
			Version:     status.Version,
			Description: status.Description,
			Kind:        status.Kind,
			State:       status.State,
			Message:     fmt.Sprintf("%s migration %s checksum differs from history", status.Kind, status.Version),
		}, true
	})
	return MigrationValidationReport{Issues: issues}
}
