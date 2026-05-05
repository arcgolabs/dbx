package repository

import (
	"context"

	collectionx "github.com/arcgolabs/collectionx/list"
	"github.com/arcgolabs/dbx/paging"
	"github.com/arcgolabs/dbx/querydsl"
)

// QueryBuilder accumulates repository specs and executes them against one repo.
type QueryBuilder[E any, S EntitySchema[E]] struct {
	repo  *Base[E, S]
	specs []Spec
}

// Query starts a fluent repository query bound to repo.
func Query[E any, S EntitySchema[E]](repo *Base[E, S]) QueryBuilder[E, S] {
	return QueryBuilder[E, S]{repo: repo}
}

// Where appends a predicate spec.
func (q QueryBuilder[E, S]) Where(predicate querydsl.Predicate) QueryBuilder[E, S] {
	return q.Spec(Where(predicate))
}

// OrderBy appends order specs.
func (q QueryBuilder[E, S]) OrderBy(orders ...querydsl.Order) QueryBuilder[E, S] {
	return q.Spec(OrderBy(orders...))
}

// Limit appends a row limit spec.
func (q QueryBuilder[E, S]) Limit(limit int) QueryBuilder[E, S] {
	return q.Spec(Limit(limit))
}

// Offset appends a row offset spec.
func (q QueryBuilder[E, S]) Offset(offset int) QueryBuilder[E, S] {
	return q.Spec(Offset(offset))
}

// Page appends a normalized page request spec.
func (q QueryBuilder[E, S]) Page(page, pageSize int) QueryBuilder[E, S] {
	return q.PageRequest(paging.NewRequest(page, pageSize))
}

// PageRequest appends a paging spec.
func (q QueryBuilder[E, S]) PageRequest(request paging.Request) QueryBuilder[E, S] {
	return q.Spec(PageByRequest(request))
}

// Spec appends repository specs.
func (q QueryBuilder[E, S]) Spec(specs ...Spec) QueryBuilder[E, S] {
	for _, spec := range specs {
		if spec != nil {
			q.specs = append(q.specs, spec)
		}
	}
	return q
}

// Select builds the underlying SelectQuery for inspection or composition.
func (q QueryBuilder[E, S]) Select() *querydsl.SelectQuery {
	if q.repo == nil {
		return nil
	}
	return q.repo.applySpecs(q.specs...)
}

// List executes the accumulated query and returns every matched entity.
func (q QueryBuilder[E, S]) List(ctx context.Context) (*collectionx.List[E], error) {
	return q.repo.List(ctx, q.Select())
}

// First executes the accumulated query and returns the first matched entity.
func (q QueryBuilder[E, S]) First(ctx context.Context) (E, error) {
	return q.repo.First(ctx, q.Select())
}

// Count executes the accumulated query as a count.
func (q QueryBuilder[E, S]) Count(ctx context.Context) (int64, error) {
	return q.repo.Count(ctx, q.Select())
}

// Exists reports whether the accumulated query matches at least one row.
func (q QueryBuilder[E, S]) Exists(ctx context.Context) (bool, error) {
	return q.repo.Exists(ctx, q.Select())
}

// ListPage executes the accumulated query and returns a page result.
func (q QueryBuilder[E, S]) ListPage(ctx context.Context, request paging.Request) (paging.Result[E], error) {
	return q.repo.ListPageRequest(ctx, q.Select(), request)
}
