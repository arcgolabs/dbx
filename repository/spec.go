package repository

import (
	"github.com/arcgolabs/dbx/paging"
	"github.com/arcgolabs/dbx/querydsl"
)

// Spec mutates a select query before repository execution.
type Spec interface {
	Apply(query *querydsl.SelectQuery) *querydsl.SelectQuery
}

// SpecFunc adapts a function into a Spec.
type SpecFunc func(query *querydsl.SelectQuery) *querydsl.SelectQuery

// Apply applies the wrapped query mutation.
func (f SpecFunc) Apply(query *querydsl.SelectQuery) *querydsl.SelectQuery { return f(query) }

// Where appends a predicate to the query.
func Where(predicate querydsl.Predicate) Spec {
	return SpecFunc(func(query *querydsl.SelectQuery) *querydsl.SelectQuery { return query.Where(predicate) })
}

// OrderBy appends one or more order clauses to the query.
func OrderBy(orders ...querydsl.Order) Spec {
	return SpecFunc(func(query *querydsl.SelectQuery) *querydsl.SelectQuery { return query.OrderBy(orders...) })
}

// Page applies a normalized page request to the query.
func Page(page, pageSize int) Spec {
	return PageByRequest(paging.NewRequest(page, pageSize))
}

// PageByRequest applies an existing page request to the query.
func PageByRequest(request paging.Request) Spec {
	return SpecFunc(func(query *querydsl.SelectQuery) *querydsl.SelectQuery { return query.Page(request) })
}

// Limit applies a row limit to the query.
func Limit(limit int) Spec {
	return SpecFunc(func(query *querydsl.SelectQuery) *querydsl.SelectQuery { return query.Limit(limit) })
}

// Offset applies a row offset to the query.
func Offset(offset int) Spec {
	return SpecFunc(func(query *querydsl.SelectQuery) *querydsl.SelectQuery { return query.Offset(offset) })
}
