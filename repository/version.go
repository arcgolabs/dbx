package repository

import (
	"context"
	"database/sql"

	collectionx "github.com/arcgolabs/collectionx/list"
	"github.com/arcgolabs/dbx"
	"github.com/arcgolabs/dbx/querydsl"
)

// VersionColumn is the typed int64 version-column behavior needed by optimistic locking.
type VersionColumn interface {
	querydsl.ColumnAccessor
	Eq(int64) querydsl.Predicate
	Set(int64) querydsl.Assignment
}

// UpdateByVersionSet performs an optimistic-lock update against a typed key set and version column.
func (r *Base[E, S]) UpdateByVersionSet(ctx context.Context, key TypedKeySet, version VersionColumn, currentVersion int64, assignments ...querydsl.Assignment) (sql.Result, error) {
	if r == nil || r.session == nil {
		return nil, dbx.ErrNilDB
	}
	if version == nil {
		return nil, &ValidationError{Message: "version column is nil"}
	}
	if len(assignments) == 0 {
		return nil, ErrNilMutation
	}
	keyPredicate, err := key.Predicate()
	if err != nil {
		return nil, err
	}
	nextVersion := currentVersion + 1
	nextAssignments := collectionx.NewList[querydsl.Assignment](assignments...)
	nextAssignments.Add(version.Set(nextVersion))

	result, err := r.Update(ctx, querydsl.Update(r.schema).
		SetList(nextAssignments).
		Where(querydsl.And(keyPredicate, version.Eq(currentVersion))))
	if err != nil {
		dbx.LogRuntimeNode(r.session, "repository.update_by_version_set.error", "table", r.schema.TableName(), "error", err)
		return nil, err
	}
	if !hasAffectedRows(result) {
		dbx.LogRuntimeNode(r.session, "repository.update_by_version_set.conflict", "table", r.schema.TableName(), "version", currentVersion)
		return nil, &VersionConflictError{Err: ErrVersionConflict}
	}
	if err := r.auditUpdatedByKeySet(ctx, result, key); err != nil {
		return result, err
	}
	dbx.LogRuntimeNode(r.session, "repository.update_by_version_set.done", "table", r.schema.TableName(), "version", nextVersion)
	return result, nil
}
