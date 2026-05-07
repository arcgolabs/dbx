package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	collectionx "github.com/arcgolabs/collectionx/list"
	"github.com/arcgolabs/dbx"
	columnx "github.com/arcgolabs/dbx/column"
	"github.com/arcgolabs/dbx/querydsl"
)

// UpdateByVersionReturning performs an optimistic-lock update and scans the updated row.
func (r *Base[E, S]) UpdateByVersionReturning(ctx context.Context, key Key, currentVersion int64, assignments ...querydsl.Assignment) (E, error) {
	var zero E
	if r == nil || r.session == nil {
		return zero, dbx.ErrNilDB
	}
	if len(key) == 0 {
		return zero, &ValidationError{Message: "key is empty"}
	}
	if len(assignments) == 0 {
		return zero, ErrNilMutation
	}
	nextVersion := currentVersion + 1
	nextAssignments := collectionx.NewList[querydsl.Assignment](assignments...)
	nextAssignments.Add(columnx.Named[any](r.schema, "version").Set(nextVersion))
	predicate := querydsl.And(
		keyPredicate(r.schema, key),
		columnx.Named[any](r.schema, "version").Eq(currentVersion),
	)
	return r.updateByVersionReturning(ctx, predicate, nextAssignments, currentVersion, nextVersion, "update by version returning")
}

// UpdateByVersionSetReturning performs an optimistic-lock update against typed key and version columns, then scans the updated row.
func (r *Base[E, S]) UpdateByVersionSetReturning(ctx context.Context, key TypedKeySet, version VersionColumn, currentVersion int64, assignments ...querydsl.Assignment) (E, error) {
	var zero E
	if r == nil || r.session == nil {
		return zero, dbx.ErrNilDB
	}
	if version == nil {
		return zero, &ValidationError{Message: "version column is nil"}
	}
	if len(assignments) == 0 {
		return zero, ErrNilMutation
	}
	keyPredicate, err := key.Predicate()
	if err != nil {
		return zero, err
	}
	nextVersion := currentVersion + 1
	nextAssignments := collectionx.NewList[querydsl.Assignment](assignments...)
	nextAssignments.Add(version.Set(nextVersion))
	predicate := querydsl.And(keyPredicate, version.Eq(currentVersion))
	return r.updateByVersionReturning(ctx, predicate, nextAssignments, currentVersion, nextVersion, "update by version set returning")
}

func (r *Base[E, S]) updateByVersionReturning(
	ctx context.Context,
	predicate querydsl.Predicate,
	assignments *collectionx.List[querydsl.Assignment],
	currentVersion int64,
	nextVersion int64,
	op string,
) (E, error) {
	var zero E
	query := querydsl.Update(r.schema).
		SetList(assignments).
		Where(predicate).
		ReturningList(returningItems(r))
	item, err := dbx.QueryOne[E](ctx, r.session, query, r.mapper)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			dbx.LogRuntimeNode(r.session, "repository.update_by_version_returning.conflict", "table", r.schema.TableName(), "version", currentVersion)
			return zero, &VersionConflictError{Err: ErrVersionConflict}
		}
		wrapped := wrapMutationError(fmt.Errorf("%s: %w", op, err))
		dbx.LogRuntimeNode(r.session, "repository.update_by_version_returning.error", "table", r.schema.TableName(), "error", wrapped)
		return zero, wrapped
	}
	dbx.LogRuntimeNode(r.session, "repository.update_by_version_returning.done", "table", r.schema.TableName(), "version", nextVersion)
	return item, nil
}
