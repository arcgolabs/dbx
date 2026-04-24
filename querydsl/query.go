package querydsl

import (
	"github.com/arcgolabs/collectionx"
	"github.com/arcgolabs/dbx/paging"
)

type Join struct {
	Type      JoinType
	Table     Table
	Predicate Predicate
}

type CTE struct {
	Name  string
	Query *SelectQuery
}

type UnionClause struct {
	All   bool
	Query *SelectQuery
}

type SelectQuery struct {
	Items     collectionx.List[SelectItem]
	FromItem  Table
	Joins     collectionx.List[Join]
	WhereExp  Predicate
	Groups    collectionx.List[Expression]
	HavingExp Predicate
	Orders    collectionx.List[Order]
	LimitN    *int
	OffsetN   *int
	Distinct  bool
	CTEs      collectionx.List[CTE]
	Unions    collectionx.List[UnionClause]
}

type JoinBuilder struct {
	query *SelectQuery
	index int
}

func Select(items ...SelectItem) *SelectQuery {
	return &SelectQuery{Items: CompactSelectItems(items)}
}

func SelectList(items collectionx.List[SelectItem]) *SelectQuery {
	return &SelectQuery{Items: CompactSelectItemsList(items)}
}

func (q *SelectQuery) Clone() *SelectQuery {
	if q == nil {
		return nil
	}
	cloned := *q
	cloned.Items = q.Items.Clone()
	cloned.Joins = q.Joins.Clone()
	cloned.Groups = q.Groups.Clone()
	cloned.Orders = q.Orders.Clone()
	cloned.CTEs = cloneCTEs(q.CTEs)
	cloned.Unions = cloneUnionClauses(q.Unions)
	cloned.LimitN = cloneInt(q.LimitN)
	cloned.OffsetN = cloneInt(q.OffsetN)
	return &cloned
}

func (q *SelectQuery) WithDistinct() *SelectQuery {
	q.Distinct = true
	return q
}

func (q *SelectQuery) DistinctOn() *SelectQuery {
	q.Distinct = true
	return q
}

func (q *SelectQuery) With(name string, query *SelectQuery) *SelectQuery {
	q.CTEs = appendListItem(q.CTEs, CTE{Name: name, Query: query})
	return q
}

func (q *SelectQuery) From(source TableSource) *SelectQuery {
	q.FromItem = TableRef(source)
	return q
}

func (q *SelectQuery) Where(predicate Predicate) *SelectQuery {
	q.WhereExp = predicate
	return q
}

func (q *SelectQuery) GroupBy(expressions ...Expression) *SelectQuery {
	return q.GroupByList(CompactExpressions(expressions))
}

func (q *SelectQuery) GroupByList(expressions collectionx.List[Expression]) *SelectQuery {
	q.Groups = mergeList(q.Groups, CompactExpressionsList(expressions))
	return q
}

func (q *SelectQuery) Having(predicate Predicate) *SelectQuery {
	q.HavingExp = predicate
	return q
}

func (q *SelectQuery) OrderBy(orders ...Order) *SelectQuery {
	return q.OrderByList(CompactOrders(orders))
}

func (q *SelectQuery) OrderByList(orders collectionx.List[Order]) *SelectQuery {
	q.Orders = mergeList(q.Orders, CompactOrdersList(orders))
	return q
}

func (q *SelectQuery) Limit(limit int) *SelectQuery {
	q.LimitN = &limit
	return q
}

func (q *SelectQuery) Offset(offset int) *SelectQuery {
	q.OffsetN = &offset
	return q
}

// Page applies a normalized page request to the query.
func (q *SelectQuery) Page(request paging.Request) *SelectQuery {
	if q == nil {
		return nil
	}
	request = request.Normalize()
	return q.Limit(request.Limit()).Offset(request.Offset())
}

// PageBy applies page and page size values to the query.
func (q *SelectQuery) PageBy(page, pageSize int) *SelectQuery {
	return q.Page(paging.NewRequest(page, pageSize))
}

func (q *SelectQuery) Union(query *SelectQuery) *SelectQuery {
	q.Unions = appendListItem(q.Unions, UnionClause{Query: query})
	return q
}

func (q *SelectQuery) UnionAll(query *SelectQuery) *SelectQuery {
	q.Unions = appendListItem(q.Unions, UnionClause{All: true, Query: query})
	return q
}

func (q *SelectQuery) Join(source TableSource) *JoinBuilder {
	q.Joins = appendListItem(q.Joins, Join{Type: InnerJoin, Table: TableRef(source)})
	return &JoinBuilder{query: q, index: q.Joins.Len() - 1}
}

func (q *SelectQuery) LeftJoin(source TableSource) *JoinBuilder {
	q.Joins = appendListItem(q.Joins, Join{Type: LeftJoin, Table: TableRef(source)})
	return &JoinBuilder{query: q, index: q.Joins.Len() - 1}
}

func (q *SelectQuery) RightJoin(source TableSource) *JoinBuilder {
	q.Joins = appendListItem(q.Joins, Join{Type: RightJoin, Table: TableRef(source)})
	return &JoinBuilder{query: q, index: q.Joins.Len() - 1}
}

func (b *JoinBuilder) On(predicate Predicate) *SelectQuery {
	join, ok := b.query.Joins.Get(b.index)
	if ok {
		join.Predicate = predicate
		b.query.Joins.Set(b.index, join)
	}
	return b.query
}

func cloneCTEs(items collectionx.List[CTE]) collectionx.List[CTE] {
	return collectionx.MapList[CTE, CTE](items, func(_ int, item CTE) CTE {
		return CTE{Name: item.Name, Query: item.Query.Clone()}
	})
}

func cloneUnionClauses(items collectionx.List[UnionClause]) collectionx.List[UnionClause] {
	return collectionx.MapList[UnionClause, UnionClause](items, func(_ int, item UnionClause) UnionClause {
		return UnionClause{All: item.All, Query: item.Query.Clone()}
	})
}

func cloneInt(value *int) *int {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

func mergeList[T any](current, next collectionx.List[T]) collectionx.List[T] {
	if next == nil {
		if current == nil {
			return collectionx.NewList[T]()
		}
		return current
	}
	if current == nil {
		return next.Clone()
	}
	current.Merge(next)
	return current
}

func appendListItem[T any](current collectionx.List[T], item T) collectionx.List[T] {
	if current == nil {
		return collectionx.NewList[T](item)
	}
	current.Add(item)
	return current
}
