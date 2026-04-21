package repository

import (
	"context"
	"github.com/arcgolabs/dbx/querydsl"

	"github.com/DaiYuANg/arcgo/collectionx"
	"github.com/arcgolabs/dbx"
	mapperx "github.com/arcgolabs/dbx/mapper"
	"github.com/arcgolabs/dbx/paging"
)

// List returns every entity matched by the query.
func (r *Base[E, S]) List(ctx context.Context, query *querydsl.SelectQuery) (collectionx.List[E], error) {
	if r == nil || r.session == nil {
		return nil, dbx.ErrNilDB
	}
	listQuery := cloneOrDefault(r, query)
	dbx.LogRuntimeNode(r.session, "repository.list.start", "table", r.schema.TableName(), "has_query", query != nil)
	rows, err := dbx.QueryAll[E](ctx, r.session, listQuery, r.mapper)
	if err != nil {
		dbx.LogRuntimeNode(r.session, "repository.list.error", "table", r.schema.TableName(), "error", err)
		return nil, err
	}
	dbx.LogRuntimeNode(r.session, "repository.list.done", "table", r.schema.TableName(), "items", rows.Len())
	return rows, nil
}

// ListSpec returns every entity matched by the provided specs.
func (r *Base[E, S]) ListSpec(ctx context.Context, specs ...Spec) (collectionx.List[E], error) {
	return r.List(ctx, r.applySpecs(specs...))
}

// First returns the first entity matched by the query.
func (r *Base[E, S]) First(ctx context.Context, query *querydsl.SelectQuery) (E, error) {
	var zero E
	if r == nil || r.session == nil {
		return zero, dbx.ErrNilDB
	}
	firstQuery := cloneOrDefault(r, query)
	dbx.LogRuntimeNode(r.session, "repository.first.start", "table", r.schema.TableName(), "has_query", query != nil)
	items, err := dbx.QueryAll[E](ctx, r.session, firstQuery.Limit(1), r.mapper)
	if err != nil {
		dbx.LogRuntimeNode(r.session, "repository.first.error", "table", r.schema.TableName(), "error", err)
		return zero, err
	}
	if items.IsEmpty() {
		dbx.LogRuntimeNode(r.session, "repository.first.not_found", "table", r.schema.TableName())
		return zero, ErrNotFound
	}
	dbx.LogRuntimeNode(r.session, "repository.first.done", "table", r.schema.TableName())
	item, _ := items.GetFirst()
	return item, nil
}

// FirstSpec returns the first entity matched by the provided specs.
func (r *Base[E, S]) FirstSpec(ctx context.Context, specs ...Spec) (E, error) {
	return r.First(ctx, r.applySpecs(specs...))
}

// Count returns the number of rows matched by the query.
func (r *Base[E, S]) Count(ctx context.Context, query *querydsl.SelectQuery) (int64, error) {
	if r == nil || r.session == nil {
		return 0, dbx.ErrNilDB
	}
	dbx.LogRuntimeNode(r.session, "repository.count.start", "table", r.schema.TableName(), "has_query", query != nil)
	if countQueryRequiresWrap(query) {
		total, err := r.countWrapped(ctx, query)
		if err != nil {
			dbx.LogRuntimeNode(r.session, "repository.count.error", "table", r.schema.TableName(), "error", err)
			return 0, err
		}
		dbx.LogRuntimeNode(r.session, "repository.count.done", "table", r.schema.TableName(), "count", total)
		return total, nil
	}
	countQuery := r.defaultSelect()
	if query != nil {
		countQuery = cloneForCount(query)
	}
	countQuery.Items = collectionx.NewList[querydsl.SelectItem](querydsl.CountAll().As("count"))
	rows, err := dbx.QueryAll[countRow](ctx, r.session, countQuery, mapperx.MustStructMapper[countRow]())
	if err != nil {
		dbx.LogRuntimeNode(r.session, "repository.count.error", "table", r.schema.TableName(), "error", err)
		return 0, err
	}
	total := firstCount(rows)
	dbx.LogRuntimeNode(r.session, "repository.count.done", "table", r.schema.TableName(), "count", total)
	return total, nil
}

func (r *Base[E, S]) countWrapped(ctx context.Context, query *querydsl.SelectQuery) (int64, error) {
	bound, err := wrappedCountBound(r.session, query)
	if err != nil {
		return 0, err
	}
	rows, err := dbx.QueryAllBound[countRow](ctx, r.session, bound, mapperx.MustStructMapper[countRow]())
	if err != nil {
		return 0, err
	}
	return firstCount(rows), nil
}

// CountSpec returns the number of rows matched by the provided specs.
func (r *Base[E, S]) CountSpec(ctx context.Context, specs ...Spec) (int64, error) {
	return r.Count(ctx, r.applySpecs(specs...))
}

// Exists reports whether the query matches at least one row.
func (r *Base[E, S]) Exists(ctx context.Context, query *querydsl.SelectQuery) (bool, error) {
	if r == nil || r.session == nil {
		return false, dbx.ErrNilDB
	}
	dbx.LogRuntimeNode(r.session, "repository.exists.start", "table", r.schema.TableName(), "has_query", query != nil)
	bound, err := r.existsBound(query)
	if err != nil {
		dbx.LogRuntimeNode(r.session, "repository.exists.error", "table", r.schema.TableName(), "stage", "build_bound", "error", err)
		return false, err
	}
	exists, err := queryExistsBound(ctx, r.session, bound)
	if err != nil {
		dbx.LogRuntimeNode(r.session, "repository.exists.error", "table", r.schema.TableName(), "stage", "query_rows", "error", err)
		return false, err
	}
	dbx.LogRuntimeNode(r.session, "repository.exists.done", "table", r.schema.TableName(), "exists", exists)
	return exists, nil
}

// ExistsSpec reports whether the provided specs match at least one row.
func (r *Base[E, S]) ExistsSpec(ctx context.Context, specs ...Spec) (bool, error) {
	return r.Exists(ctx, r.applySpecs(specs...))
}

// ListPage returns one page of results together with the total row count.
func (r *Base[E, S]) ListPage(ctx context.Context, query *querydsl.SelectQuery, page, pageSize int) (paging.Result[E], error) {
	return r.ListPageRequest(ctx, query, paging.NewRequest(page, pageSize))
}

// ListPageRequest returns one page of results using a shared page request.
func (r *Base[E, S]) ListPageRequest(ctx context.Context, query *querydsl.SelectQuery, request paging.Request) (paging.Result[E], error) {
	if r == nil || r.session == nil {
		return paging.Result[E]{}, dbx.ErrNilDB
	}
	request = request.Normalize()
	dbx.LogRuntimeNode(r.session, "repository.list_page.start", "table", r.schema.TableName(), "page", request.Page, "page_size", request.PageSize)
	total, err := r.Count(ctx, query)
	if err != nil {
		dbx.LogRuntimeNode(r.session, "repository.list_page.error", "table", r.schema.TableName(), "stage", "count", "error", err)
		return paging.Result[E]{}, err
	}
	pagedQuery := cloneOrDefault(r, query)
	items, err := r.List(ctx, pagedQuery.Page(request))
	if err != nil {
		dbx.LogRuntimeNode(r.session, "repository.list_page.error", "table", r.schema.TableName(), "stage", "list", "error", err)
		return paging.Result[E]{}, err
	}
	dbx.LogRuntimeNode(r.session, "repository.list_page.done", "table", r.schema.TableName(), "items", items.Len(), "total", total)
	return paging.NewResult[E](items, total, request), nil
}

// ListPageSpec returns one page of results for the provided specs.
func (r *Base[E, S]) ListPageSpec(ctx context.Context, page, pageSize int, specs ...Spec) (paging.Result[E], error) {
	return r.ListPageSpecRequest(ctx, paging.NewRequest(page, pageSize), specs...)
}

// ListPageSpecRequest returns one page of results for the provided specs and page request.
func (r *Base[E, S]) ListPageSpecRequest(ctx context.Context, request paging.Request, specs ...Spec) (paging.Result[E], error) {
	if r == nil || r.session == nil {
		return paging.Result[E]{}, dbx.ErrNilDB
	}
	return r.ListPageRequest(ctx, r.applySpecs(specs...), request)
}
