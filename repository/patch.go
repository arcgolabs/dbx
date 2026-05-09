package repository

import (
	"context"
	"database/sql"

	collectionx "github.com/arcgolabs/collectionx/list"
	"github.com/arcgolabs/dbx"
	"github.com/arcgolabs/dbx/querydsl"
)

// PatchBuilder accumulates partial update assignments for one keyed row.
type PatchBuilder[E any, S EntitySchema[E]] struct {
	repo        *Base[E, S]
	key         Key
	keySet      TypedKeySet
	useKeySet   bool
	assignments []querydsl.Assignment
	version     VersionColumn
	current     int64
	hasVersion  bool
}

// Patch starts a partial update for a dynamic key.
func Patch[E any, S EntitySchema[E]](repo *Base[E, S], key Key) PatchBuilder[E, S] {
	return PatchBuilder[E, S]{repo: repo, key: key}
}

// PatchSet starts a partial update for a typed key set.
func PatchSet[E any, S EntitySchema[E]](repo *Base[E, S], key TypedKeySet) PatchBuilder[E, S] {
	return PatchBuilder[E, S]{repo: repo, keySet: key, useKeySet: true}
}

// Set appends typed column assignments to this patch.
func (p PatchBuilder[E, S]) Set(assignments ...querydsl.Assignment) PatchBuilder[E, S] {
	p.assignments = append(p.assignments, compactAssignments(assignments...)...)
	return p
}

func compactAssignments(assignments ...querydsl.Assignment) []querydsl.Assignment {
	return collectionx.FilterList[querydsl.Assignment](collectionx.NewList[querydsl.Assignment](assignments...), func(_ int, assignment querydsl.Assignment) bool {
		return assignment != nil
	}).Values()
}

// Version enables optimistic locking with the provided version column and current value.
func (p PatchBuilder[E, S]) Version(column VersionColumn, current int64) PatchBuilder[E, S] {
	p.version = column
	p.current = current
	p.hasVersion = true
	return p
}

// Apply executes the patch update.
func (p PatchBuilder[E, S]) Apply(ctx context.Context) (sql.Result, error) {
	if p.repo == nil || p.repo.session == nil {
		return nil, dbx.ErrNilDB
	}
	if len(p.assignments) == 0 {
		return nil, ErrNilMutation
	}
	if p.useKeySet {
		if p.hasVersion {
			return p.repo.UpdateByVersionSet(ctx, p.keySet, p.version, p.current, p.assignments...)
		}
		return p.repo.UpdateByKeySet(ctx, p.keySet, p.assignments...)
	}
	if p.hasVersion {
		return p.repo.UpdateByVersion(ctx, p.key, p.current, p.assignments...)
	}
	return p.repo.UpdateByKey(ctx, p.key, p.assignments...)
}

func (r *Base[E, S]) softDeleteAssignment() (querydsl.Assignment, error) {
	if r == nil || r.session == nil {
		return nil, dbx.ErrNilDB
	}
	if r.softDeleteAssign == nil {
		return nil, &ValidationError{Message: "soft delete is not configured"}
	}
	assignment := r.softDeleteAssign()
	if assignment == nil {
		return nil, ErrNilMutation
	}
	return assignment, nil
}

// SoftDeleteByKey updates the configured soft-delete column for key.
func (r *Base[E, S]) SoftDeleteByKey(ctx context.Context, key Key) (sql.Result, error) {
	assignment, err := r.softDeleteAssignment()
	if err != nil {
		return nil, err
	}
	return r.UpdateByKey(ctx, key, assignment)
}

// SoftDeleteByKeySet updates the configured soft-delete column for key.
func (r *Base[E, S]) SoftDeleteByKeySet(ctx context.Context, key TypedKeySet) (sql.Result, error) {
	assignment, err := r.softDeleteAssignment()
	if err != nil {
		return nil, err
	}
	return r.UpdateByKeySet(ctx, key, assignment)
}

// SoftDeletePatch starts a soft-delete patch for a dynamic key.
func SoftDeletePatch[E any, S EntitySchema[E]](repo *Base[E, S], key Key) (PatchBuilder[E, S], error) {
	assignment, err := repo.softDeleteAssignment()
	if err != nil {
		return PatchBuilder[E, S]{}, err
	}
	return Patch(repo, key).Set(assignment), nil
}

// SoftDeletePatchSet starts a soft-delete patch for a typed key set.
func SoftDeletePatchSet[E any, S EntitySchema[E]](repo *Base[E, S], key TypedKeySet) (PatchBuilder[E, S], error) {
	assignment, err := repo.softDeleteAssignment()
	if err != nil {
		return PatchBuilder[E, S]{}, err
	}
	return PatchSet(repo, key).Set(assignment), nil
}
