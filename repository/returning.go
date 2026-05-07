package repository

import (
	"context"
	"fmt"

	collectionx "github.com/arcgolabs/collectionx/list"
	"github.com/arcgolabs/dbx"
	mapperx "github.com/arcgolabs/dbx/mapper"
	"github.com/arcgolabs/dbx/querydsl"
)

// CreateReturning inserts entity and scans the RETURNING row into the repository entity type.
func (r *Base[E, S]) CreateReturning(ctx context.Context, entity *E) (E, error) {
	return CreateReturning(ctx, r, entity)
}

// CreateReturning inserts entity and scans the RETURNING row into the repository entity type.
func CreateReturning[E any, S EntitySchema[E]](ctx context.Context, repo *Base[E, S], entity *E, items ...querydsl.SelectItem) (E, error) {
	if repo == nil {
		var zero E
		return zero, dbx.ErrNilDB
	}
	return createReturningWithMapper(ctx, repo, entity, repo.mapper, items...)
}

// CreateReturningInto inserts entity and scans the RETURNING row into R.
func CreateReturningInto[R any, E any, S EntitySchema[E]](ctx context.Context, repo *Base[E, S], entity *E, items ...querydsl.SelectItem) (R, error) {
	return createReturningWithMapper(ctx, repo, entity, mapperx.MustStructMapper[R](), items...)
}

func createReturningWithMapper[R any, E any, S EntitySchema[E]](
	ctx context.Context,
	repo *Base[E, S],
	entity *E,
	scanner mapperx.RowsScanner[R],
	items ...querydsl.SelectItem,
) (R, error) {
	var zero R
	if repo == nil || repo.session == nil {
		return zero, dbx.ErrNilDB
	}
	if entity == nil {
		return zero, &ValidationError{Message: "entity is nil"}
	}
	assignments, err := repo.insertAssignments(ctx, entity)
	if err != nil {
		return zero, err
	}
	query := querydsl.InsertInto(repo.schema).
		ValuesList(assignments).
		ReturningList(returningItems(repo, items...))
	item, err := dbx.QueryOne[R](ctx, repo.session, query, scanner)
	if err != nil {
		return zero, wrapMutationError(fmt.Errorf("create returning: %w", err))
	}
	return item, nil
}

// UpsertReturning inserts or updates entity and scans the RETURNING row into the repository entity type.
func (r *Base[E, S]) UpsertReturning(ctx context.Context, entity *E, conflictColumns ...string) (E, error) {
	return UpsertReturning(ctx, r, entity, conflictColumns...)
}

// UpsertReturning inserts or updates entity and scans the RETURNING row into the repository entity type.
func UpsertReturning[E any, S EntitySchema[E]](ctx context.Context, repo *Base[E, S], entity *E, conflictColumns ...string) (E, error) {
	var zero E
	if repo == nil || repo.session == nil {
		return zero, dbx.ErrNilDB
	}
	if entity == nil {
		return zero, &ValidationError{Message: "entity is nil"}
	}
	assignments, err := repo.insertAssignments(ctx, entity)
	if err != nil {
		return zero, err
	}
	query, _, err := repo.upsertQuery(assignments, conflictColumns...)
	if err != nil {
		return zero, err
	}
	item, err := dbx.QueryOne[E](ctx, repo.session, query.ReturningList(returningItems(repo)), repo.mapper)
	if err != nil {
		return zero, wrapMutationError(fmt.Errorf("upsert returning: %w", err))
	}
	return item, nil
}

// UpdateReturning executes an UPDATE ... RETURNING query and scans rows into the repository entity type.
func (r *Base[E, S]) UpdateReturning(ctx context.Context, query *querydsl.UpdateQuery) (*collectionx.List[E], error) {
	return UpdateReturning(ctx, r, query)
}

// UpdateReturning executes an UPDATE ... RETURNING query and scans rows into the repository entity type.
func UpdateReturning[E any, S EntitySchema[E]](ctx context.Context, repo *Base[E, S], query *querydsl.UpdateQuery, items ...querydsl.SelectItem) (*collectionx.List[E], error) {
	if repo == nil {
		return nil, dbx.ErrNilDB
	}
	return updateReturningWithMapper(ctx, repo, query, repo.mapper, items...)
}

// UpdateReturningInto executes an UPDATE ... RETURNING query and scans rows into R.
func UpdateReturningInto[R any, E any, S EntitySchema[E]](ctx context.Context, repo *Base[E, S], query *querydsl.UpdateQuery, items ...querydsl.SelectItem) (*collectionx.List[R], error) {
	return updateReturningWithMapper(ctx, repo, query, mapperx.MustStructMapper[R](), items...)
}

func updateReturningWithMapper[R any, E any, S EntitySchema[E]](
	ctx context.Context,
	repo *Base[E, S],
	query *querydsl.UpdateQuery,
	scanner mapperx.RowsScanner[R],
	items ...querydsl.SelectItem,
) (*collectionx.List[R], error) {
	if repo == nil || repo.session == nil {
		return nil, dbx.ErrNilDB
	}
	if query == nil {
		return nil, ErrNilMutation
	}
	returningQuery := query.Clone()
	ensureUpdateReturning(repo, returningQuery, items...)
	rows, err := dbx.QueryAll[R](ctx, repo.session, returningQuery, scanner)
	if err != nil {
		return nil, wrapMutationError(fmt.Errorf("update returning: %w", err))
	}
	return rows, nil
}

// DeleteReturning executes a DELETE ... RETURNING query and scans rows into the repository entity type.
func (r *Base[E, S]) DeleteReturning(ctx context.Context, query *querydsl.DeleteQuery) (*collectionx.List[E], error) {
	return DeleteReturning(ctx, r, query)
}

// DeleteReturning executes a DELETE ... RETURNING query and scans rows into the repository entity type.
func DeleteReturning[E any, S EntitySchema[E]](ctx context.Context, repo *Base[E, S], query *querydsl.DeleteQuery, items ...querydsl.SelectItem) (*collectionx.List[E], error) {
	if repo == nil {
		return nil, dbx.ErrNilDB
	}
	return deleteReturningWithMapper(ctx, repo, query, repo.mapper, items...)
}

// DeleteReturningInto executes a DELETE ... RETURNING query and scans rows into R.
func DeleteReturningInto[R any, E any, S EntitySchema[E]](ctx context.Context, repo *Base[E, S], query *querydsl.DeleteQuery, items ...querydsl.SelectItem) (*collectionx.List[R], error) {
	return deleteReturningWithMapper(ctx, repo, query, mapperx.MustStructMapper[R](), items...)
}

func deleteReturningWithMapper[R any, E any, S EntitySchema[E]](
	ctx context.Context,
	repo *Base[E, S],
	query *querydsl.DeleteQuery,
	scanner mapperx.RowsScanner[R],
	items ...querydsl.SelectItem,
) (*collectionx.List[R], error) {
	if repo == nil || repo.session == nil {
		return nil, dbx.ErrNilDB
	}
	if query == nil {
		return nil, ErrNilMutation
	}
	returningQuery := query.Clone()
	ensureDeleteReturning(repo, returningQuery, items...)
	rows, err := dbx.QueryAll[R](ctx, repo.session, returningQuery, scanner)
	if err != nil {
		return nil, wrapMutationError(fmt.Errorf("delete returning: %w", err))
	}
	return rows, nil
}

func returningItems[E any, S EntitySchema[E]](repo *Base[E, S], items ...querydsl.SelectItem) *collectionx.List[querydsl.SelectItem] {
	if len(items) > 0 {
		return querydsl.CompactSelectItems(items)
	}
	return querydsl.AllColumns(repo.schema)
}

func ensureUpdateReturning[E any, S EntitySchema[E]](repo *Base[E, S], query *querydsl.UpdateQuery, items ...querydsl.SelectItem) {
	if len(items) > 0 {
		query.ReturningItems = returningItems(repo, items...)
		return
	}
	if query.ReturningItems.Len() == 0 {
		query.ReturningItems = returningItems(repo)
	}
}

func ensureDeleteReturning[E any, S EntitySchema[E]](repo *Base[E, S], query *querydsl.DeleteQuery, items ...querydsl.SelectItem) {
	if len(items) > 0 {
		query.ReturningItems = returningItems(repo, items...)
		return
	}
	if query.ReturningItems.Len() == 0 {
		query.ReturningItems = returningItems(repo)
	}
}
