package querydsl

import (
	"errors"

	collectionx "github.com/arcgolabs/collectionx/list"
	"github.com/arcgolabs/dbx/dialect"
	"github.com/arcgolabs/dbx/paging"
	"github.com/arcgolabs/dbx/sqlstmt"
)

// SelectResult is a typed SELECT builder whose row shape is R.
//
// The result type is intentionally carried only at the public API boundary.
// Rendering still delegates to SelectQuery, so existing querydsl internals stay
// non-generic and reusable.
type SelectResult[R any] struct {
	query *SelectQuery
}

// SelectInto starts a SELECT query whose result rows scan into R.
func SelectInto[R any](items ...SelectItem) SelectResult[R] {
	return SelectResult[R]{query: Select(items...)}
}

// SelectValue starts a typed scalar SELECT from one typed select item.
func SelectValue[T any](item TypedSelectItem[T]) SelectResult[T] {
	return SelectInto[T](item)
}

// SelectFromInto starts a SELECT query from source whose result rows scan into R.
func SelectFromInto[R any](source TableSource, items ...SelectItem) SelectResult[R] {
	return SelectInto[R](items...).From(source)
}

// TypedSelect wraps an existing SelectQuery with a result row type.
func TypedSelect[R any](query *SelectQuery) SelectResult[R] {
	return SelectResult[R]{query: query}
}

// Query returns the underlying mutable SelectQuery.
func (q SelectResult[R]) Query() *SelectQuery {
	return q.query
}

// Subquery returns the typed query as a scalar subquery operand.
func (q SelectResult[R]) Subquery() SubqueryOperand {
	return Subquery(q.query)
}

// Clone returns a typed wrapper around a cloned SelectQuery.
func (q SelectResult[R]) Clone() SelectResult[R] {
	if q.query == nil {
		return SelectResult[R]{}
	}
	return SelectResult[R]{query: q.query.Clone()}
}

// Build renders the underlying SELECT query.
func (q SelectResult[R]) Build(d dialect.Dialect) (sqlstmt.Bound, error) {
	if q.query == nil {
		return sqlstmt.Bound{}, errors.New("dbx/querydsl: typed select query is nil")
	}
	return q.query.Build(d)
}

// WithDistinct marks the query as DISTINCT.
func (q SelectResult[R]) WithDistinct() SelectResult[R] {
	q.ensure().WithDistinct()
	return q
}

// DistinctOn marks the query as DISTINCT.
func (q SelectResult[R]) DistinctOn() SelectResult[R] {
	q.ensure().DistinctOn()
	return q
}

// With adds a CTE to the typed query.
func (q SelectResult[R]) With(name string, query *SelectQuery) SelectResult[R] {
	q.ensure().With(name, query)
	return q
}

// Select replaces the typed query select list.
func (q SelectResult[R]) Select(items ...SelectItem) SelectResult[R] {
	q.ensure().Select(items...)
	return q
}

// SelectList replaces the typed query select list from a collectionx.List.
func (q SelectResult[R]) SelectList(items *collectionx.List[SelectItem]) SelectResult[R] {
	q.ensure().SelectList(items)
	return q
}

// From sets the typed query source.
func (q SelectResult[R]) From(source TableSource) SelectResult[R] {
	q.ensure().From(source)
	return q
}

// Where sets the typed query predicate.
func (q SelectResult[R]) Where(predicate Predicate) SelectResult[R] {
	q.ensure().Where(predicate)
	return q
}

// GroupBy appends group expressions.
func (q SelectResult[R]) GroupBy(expressions ...Expression) SelectResult[R] {
	q.ensure().GroupBy(expressions...)
	return q
}

// GroupByList appends group expressions from a collectionx.List.
func (q SelectResult[R]) GroupByList(expressions *collectionx.List[Expression]) SelectResult[R] {
	q.ensure().GroupByList(expressions)
	return q
}

// Having sets the typed query HAVING predicate.
func (q SelectResult[R]) Having(predicate Predicate) SelectResult[R] {
	q.ensure().Having(predicate)
	return q
}

// OrderBy appends order clauses.
func (q SelectResult[R]) OrderBy(orders ...Order) SelectResult[R] {
	q.ensure().OrderBy(orders...)
	return q
}

// OrderByList appends order clauses from a collectionx.List.
func (q SelectResult[R]) OrderByList(orders *collectionx.List[Order]) SelectResult[R] {
	q.ensure().OrderByList(orders)
	return q
}

// Limit sets the typed query row limit.
func (q SelectResult[R]) Limit(limit int) SelectResult[R] {
	q.ensure().Limit(limit)
	return q
}

// Offset sets the typed query row offset.
func (q SelectResult[R]) Offset(offset int) SelectResult[R] {
	q.ensure().Offset(offset)
	return q
}

// Page applies a normalized page request to the typed query.
func (q SelectResult[R]) Page(request paging.Request) SelectResult[R] {
	q.ensure().Page(request)
	return q
}

// PageBy applies page and page size values to the typed query.
func (q SelectResult[R]) PageBy(page, pageSize int) SelectResult[R] {
	q.ensure().PageBy(page, pageSize)
	return q
}

// Union appends a UNION query.
func (q SelectResult[R]) Union(query *SelectQuery) SelectResult[R] {
	q.ensure().Union(query)
	return q
}

// UnionAll appends a UNION ALL query.
func (q SelectResult[R]) UnionAll(query *SelectQuery) SelectResult[R] {
	q.ensure().UnionAll(query)
	return q
}

// Join starts a typed inner join builder.
func (q SelectResult[R]) Join(source TableSource) TypedJoinBuilder[R] {
	return TypedJoinBuilder[R]{query: q, builder: q.ensure().Join(source)}
}

// LeftJoin starts a typed left join builder.
func (q SelectResult[R]) LeftJoin(source TableSource) TypedJoinBuilder[R] {
	return TypedJoinBuilder[R]{query: q, builder: q.ensure().LeftJoin(source)}
}

// RightJoin starts a typed right join builder.
func (q SelectResult[R]) RightJoin(source TableSource) TypedJoinBuilder[R] {
	return TypedJoinBuilder[R]{query: q, builder: q.ensure().RightJoin(source)}
}

func (q *SelectResult[R]) ensure() *SelectQuery {
	if q.query == nil {
		q.query = Select()
	}
	return q.query
}

// TypedJoinBuilder preserves SelectResult's row type across join construction.
type TypedJoinBuilder[R any] struct {
	query   SelectResult[R]
	builder *JoinBuilder
}

// On completes the join and returns the typed SELECT builder.
func (b TypedJoinBuilder[R]) On(predicate Predicate) SelectResult[R] {
	if b.builder != nil {
		b.builder.On(predicate)
	}
	return b.query
}
