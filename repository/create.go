package repository

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/arcgolabs/collectionx"
	"github.com/arcgolabs/dbx"
	columnx "github.com/arcgolabs/dbx/column"
	"github.com/arcgolabs/dbx/idgen"
	mapperx "github.com/arcgolabs/dbx/mapper"
	"github.com/arcgolabs/dbx/querydsl"
)

// Create inserts a single entity.
func (r *Base[E, S]) Create(ctx context.Context, entity *E) error {
	if r == nil || r.session == nil {
		return dbx.ErrNilDB
	}
	if entity == nil {
		return &ValidationError{Message: "entity is nil"}
	}
	dbx.LogRuntimeNode(r.session, "repository.create.start", "table", r.schema.TableName())
	assignments, err := r.insertAssignments(ctx, entity)
	if err != nil {
		dbx.LogRuntimeNode(r.session, "repository.create.error", "table", r.schema.TableName(), "stage", "assignments", "error", err)
		return err
	}
	_, err = dbx.Exec(ctx, r.session, querydsl.InsertInto(r.schema).ValuesList(assignments))
	if err != nil {
		wrapped := wrapMutationError(err)
		dbx.LogRuntimeNode(r.session, "repository.create.error", "table", r.schema.TableName(), "stage", "exec", "error", wrapped)
		return wrapped
	}
	dbx.LogRuntimeNode(r.session, "repository.create.done", "table", r.schema.TableName())
	return nil
}

// CreateMany inserts multiple entities in one statement.
func (r *Base[E, S]) CreateMany(ctx context.Context, entities ...*E) error {
	if r == nil || r.session == nil {
		return dbx.ErrNilDB
	}
	if len(entities) == 0 {
		return nil
	}
	dbx.LogRuntimeNode(r.session, "repository.create_many.start", "table", r.schema.TableName(), "entities", len(entities))
	query := querydsl.InsertInto(r.schema)
	var buildErr error
	collectionx.NewList[*E](entities...).Range(func(index int, entity *E) bool {
		if entity == nil {
			buildErr = &ValidationError{Message: fmt.Sprintf("entity at index %d is nil", index)}
			return false
		}
		assignments, err := r.insertAssignments(ctx, entity)
		if err != nil {
			dbx.LogRuntimeNode(r.session, "repository.create_many.error", "table", r.schema.TableName(), "stage", "assignments", "index", index, "error", err)
			buildErr = err
			return false
		}
		query.ValuesList(assignments)
		return true
	})
	if buildErr != nil {
		return buildErr
	}
	_, err := dbx.Exec(ctx, r.session, query)
	if err != nil {
		wrapped := wrapMutationError(err)
		dbx.LogRuntimeNode(r.session, "repository.create_many.error", "table", r.schema.TableName(), "stage", "exec", "error", wrapped)
		return wrapped
	}
	dbx.LogRuntimeNode(r.session, "repository.create_many.done", "table", r.schema.TableName(), "entities", len(entities))
	return nil
}

// Upsert inserts an entity or updates the conflicting row.
func (r *Base[E, S]) Upsert(ctx context.Context, entity *E, conflictColumns ...string) error {
	if r == nil || r.session == nil {
		return dbx.ErrNilDB
	}
	if entity == nil {
		return &ValidationError{Message: "entity is nil"}
	}
	dbx.LogRuntimeNode(r.session, "repository.upsert.start", "table", r.schema.TableName(), "conflict_columns", conflictColumns)
	assignments, err := r.insertAssignments(ctx, entity)
	if err != nil {
		dbx.LogRuntimeNode(r.session, "repository.upsert.error", "table", r.schema.TableName(), "stage", "assignments", "error", err)
		return err
	}
	query := querydsl.InsertInto(r.schema).ValuesList(assignments)
	targetColumns := normalizeConflictColumns(collectionx.NewList[string](conflictColumns...), r.primaryKeyColumns())
	if targetColumns.Len() == 0 {
		return &ValidationError{Message: "upsert requires conflict columns"}
	}
	targetExpressions := collectionx.MapList[string, querydsl.Expression](targetColumns, func(_ int, column string) querydsl.Expression {
		return columnx.Named[any](r.schema, column)
	})
	updateAssignments := upsertUpdateAssignments(r.schema, r.mapper.Fields(), targetColumns)
	if updateAssignments.Len() == 0 {
		query.OnConflictList(targetExpressions).DoNothing()
	} else {
		query.OnConflictList(targetExpressions).DoUpdateSetList(updateAssignments)
	}
	_, err = dbx.Exec(ctx, r.session, query)
	if err != nil {
		wrapped := wrapMutationError(err)
		dbx.LogRuntimeNode(r.session, "repository.upsert.error", "table", r.schema.TableName(), "stage", "exec", "error", wrapped)
		return wrapped
	}
	dbx.LogRuntimeNode(r.session, "repository.upsert.done", "table", r.schema.TableName(), "conflict_columns", targetColumns)
	return nil
}

func (r *Base[E, S]) insertAssignments(ctx context.Context, entity *E) (collectionx.List[querydsl.Assignment], error) {
	type idGeneratorCarrier interface {
		IDGenerator() idgen.Generator
	}
	carrier, ok := any(r.session).(idGeneratorCarrier)
	if !ok {
		return nil, fmt.Errorf("build insert assignments: %w", errors.New("session does not expose id generator"))
	}
	assignments, err := r.mapper.InsertAssignmentsWithID(ctx, r.schema, entity, carrier.IDGenerator())
	if err != nil {
		return nil, fmt.Errorf("build insert assignments: %w", err)
	}

	return assignments, nil
}

func normalizeConflictColumns(columns, fallback collectionx.List[string]) collectionx.List[string] {
	items := columns
	if items == nil || items.Len() == 0 {
		items = fallback
	}
	ordered := collectionx.NewOrderedSet[string]()
	items.Range(func(_ int, column string) bool {
		if name := strings.TrimSpace(column); name != "" {
			ordered.Add(name)
		}
		return true
	})
	result := collectionx.NewListWithCapacity[string](ordered.Len())
	ordered.Range(func(item string) bool {
		result.Add(item)
		return true
	})
	return result
}

func upsertUpdateAssignments[S querydsl.TableSource](schema S, fields collectionx.List[mapperx.MappedField], conflictColumns collectionx.List[string]) collectionx.List[querydsl.Assignment] {
	conflictSet := collectionx.NewSetWithCapacity[string](conflictColumns.Len())
	conflictColumns.Range(func(_ int, column string) bool {
		conflictSet.Add(column)
		return true
	})
	return collectionx.FilterMapList[mapperx.MappedField, querydsl.Assignment](fields, func(_ int, field mapperx.MappedField) (querydsl.Assignment, bool) {
		if conflictSet.Contains(field.Column) {
			return nil, false
		}
		return columnx.Named[any](schema, field.Column).SetExcluded(), true
	})
}
