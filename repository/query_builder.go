package repository

import (
	"context"

	collectionx "github.com/arcgolabs/collectionx/list"
	"github.com/arcgolabs/dbx"
	"github.com/arcgolabs/dbx/paging"
	"github.com/arcgolabs/dbx/querydsl"
	"github.com/samber/mo"
)

// QueryBuilder accumulates repository specs and executes them against one repo.
type QueryBuilder[E any, S EntitySchema[E]] struct {
	repo             *Base[E, S]
	specs            []Spec
	includes         []Include[E]
	skipDefaultSpecs bool
	buildErr         error
}

// Query starts a fluent repository query bound to repo.
func Query[E any, S EntitySchema[E]](repo *Base[E, S]) QueryBuilder[E, S] {
	return QueryBuilder[E, S]{repo: repo}
}

func (q QueryBuilder[E, S]) boundRepo() (*Base[E, S], error) {
	if q.buildErr != nil {
		return nil, q.buildErr
	}
	if q.repo == nil || q.repo.session == nil {
		return nil, dbx.ErrNilDB
	}
	return q.repo, nil
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
	q.specs = append(q.specs, compactSpecs(specs...)...)
	return q
}

// Include appends relation or custom include loaders to List, First, and ListPage.
func (q QueryBuilder[E, S]) Include(includes ...Include[E]) QueryBuilder[E, S] {
	q.includes = append(q.includes, compactIncludes(includes...)...)
	return q
}

func compactIncludes[E any](includes ...Include[E]) []Include[E] {
	return collectionx.FilterList[Include[E]](collectionx.NewList[Include[E]](includes...), func(_ int, include Include[E]) bool {
		return include != nil
	}).Values()
}

// WithDeleted bypasses repository default specs, including soft-delete filters.
func (q QueryBuilder[E, S]) WithDeleted() QueryBuilder[E, S] {
	q.skipDefaultSpecs = true
	return q
}

// WithoutDefaultSpecs bypasses repository default specs.
func (q QueryBuilder[E, S]) WithoutDefaultSpecs() QueryBuilder[E, S] {
	return q.WithDeleted()
}

// OnlyDeleted bypasses default specs and applies the configured soft-delete deleted filter.
func (q QueryBuilder[E, S]) OnlyDeleted() QueryBuilder[E, S] {
	q.skipDefaultSpecs = true
	if q.repo == nil || q.repo.softDeleteSpec == nil {
		q.buildErr = &ValidationError{Message: "soft delete is not configured"}
		return q
	}
	return q.Spec(q.repo.softDeleteSpec)
}

// Select builds the underlying SelectQuery for inspection or composition.
func (q QueryBuilder[E, S]) Select() *querydsl.SelectQuery {
	if q.repo == nil {
		return nil
	}
	return q.repo.applySpecsWithDefaults(!q.skipDefaultSpecs, q.specs...)
}

// List executes the accumulated query and returns every matched entity.
func (q QueryBuilder[E, S]) List(ctx context.Context) (*collectionx.List[E], error) {
	repo, err := q.boundRepo()
	if err != nil {
		return nil, err
	}
	items, err := repo.list(ctx, q.Select(), false)
	if err != nil {
		return nil, err
	}
	if err := LoadIncludes(ctx, items, q.includes...); err != nil {
		return nil, err
	}
	return items, nil
}

// First executes the accumulated query and returns the first matched entity.
func (q QueryBuilder[E, S]) First(ctx context.Context) (E, error) {
	repo, err := q.boundRepo()
	if err != nil {
		var zero E
		return zero, err
	}
	item, err := repo.first(ctx, q.Select(), false)
	if err != nil {
		var zero E
		return zero, err
	}
	if len(q.includes) == 0 {
		return item, nil
	}
	items := collectionx.NewList[E](item)
	if err := LoadIncludes(ctx, items, q.includes...); err != nil {
		var zero E
		return zero, err
	}
	loaded, _ := items.GetFirst()
	return loaded, nil
}

// Find returns the first matched entity as an option.
func (q QueryBuilder[E, S]) Find(ctx context.Context) (mo.Option[E], error) {
	return optionFromResult(q.First(ctx))
}

// FirstOption returns the first matched entity as an option.
func (q QueryBuilder[E, S]) FirstOption(ctx context.Context) (mo.Option[E], error) {
	return q.Find(ctx)
}

// Count executes the accumulated query as a count.
func (q QueryBuilder[E, S]) Count(ctx context.Context) (int64, error) {
	repo, err := q.boundRepo()
	if err != nil {
		return 0, err
	}
	return repo.count(ctx, q.Select(), false)
}

// Exists reports whether the accumulated query matches at least one row.
func (q QueryBuilder[E, S]) Exists(ctx context.Context) (bool, error) {
	repo, err := q.boundRepo()
	if err != nil {
		return false, err
	}
	return repo.exists(ctx, q.Select(), false)
}

// ListPage executes the accumulated query and returns a page result.
func (q QueryBuilder[E, S]) ListPage(ctx context.Context, request paging.Request) (paging.Result[E], error) {
	repo, err := q.boundRepo()
	if err != nil {
		return paging.Result[E]{}, err
	}
	page, err := repo.listPageRequest(ctx, q.Select(), request, false)
	if err != nil {
		return paging.Result[E]{}, err
	}
	if err := LoadIncludes(ctx, page.Items, q.includes...); err != nil {
		return paging.Result[E]{}, err
	}
	return page, nil
}

// Cursor opens a streaming cursor for the accumulated query.
func (q QueryBuilder[E, S]) Cursor(ctx context.Context) (dbx.Cursor[E], error) {
	repo, err := q.boundRepo()
	if err != nil {
		return nil, err
	}
	return repo.cursor(ctx, q.Select(), false)
}

// Each returns an iterator over entities matched by the accumulated query.
func (q QueryBuilder[E, S]) Each(ctx context.Context) func(func(E, error) bool) {
	return func(yield func(E, error) bool) {
		cursor, err := q.Cursor(ctx)
		if err != nil {
			var zero E
			yield(zero, err)
			return
		}
		defer yieldRepositoryCursorCloseError(cursor, yield)
		for cursor.Next() {
			item, itemErr := cursor.Get()
			if !yield(item, itemErr) || itemErr != nil {
				return
			}
		}
		if err := cursor.Err(); err != nil {
			var zero E
			yield(zero, err)
		}
	}
}

// Batch reads entities in batches and loads includes for each batch before handle.
func (q QueryBuilder[E, S]) Batch(ctx context.Context, size int, handle func(*collectionx.List[E]) error) error {
	repo, err := q.boundRepo()
	if err != nil {
		return err
	}
	return repo.batch(ctx, q.Select(), false, size, handle, q.includes...)
}
