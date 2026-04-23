package querydsl

import "github.com/arcgolabs/collectionx"

type InsertQuery struct {
	Into           Table
	TargetColumns  collectionx.List[Expression]
	Assignments    collectionx.List[Assignment]
	Rows           collectionx.Grid[Assignment]
	Source         *SelectQuery
	Upsert         *UpsertClause
	ReturningItems collectionx.List[SelectItem]
}

type UpdateQuery struct {
	Table          Table
	Assignments    collectionx.List[Assignment]
	WhereExp       Predicate
	ReturningItems collectionx.List[SelectItem]
}

type DeleteQuery struct {
	From           Table
	WhereExp       Predicate
	ReturningItems collectionx.List[SelectItem]
}

type ConflictBuilder struct {
	query *InsertQuery
}

type UpsertClause struct {
	Targets     collectionx.List[Expression]
	DoNothing   bool
	Assignments collectionx.List[Assignment]
}

func InsertInto(source TableSource) *InsertQuery {
	return &InsertQuery{Into: TableRef(source)}
}

func (q *InsertQuery) Columns(columns ...Expression) *InsertQuery {
	return q.ColumnsList(CompactExpressions(columns))
}

func (q *InsertQuery) ColumnsList(columns collectionx.List[Expression]) *InsertQuery {
	q.TargetColumns = mergeList(q.TargetColumns, columns)
	return q
}

func (q *InsertQuery) Values(assignments ...Assignment) *InsertQuery {
	return q.ValuesList(CompactAssignments(assignments))
}

func (q *InsertQuery) ValuesList(assignments collectionx.List[Assignment]) *InsertQuery {
	grid := collectionx.NewGridWithCapacity[Assignment](1)
	grid.AddRowList(assignments)
	return q.ValuesGrid(grid)
}

func (q *InsertQuery) ValuesRowsList(rows collectionx.List[collectionx.List[Assignment]]) *InsertQuery {
	if rows == nil || rows.Len() == 0 {
		return q
	}
	grid := collectionx.NewGridWithCapacity[Assignment](rows.Len())
	rows.Each(func(_ int, row collectionx.List[Assignment]) {
		grid.AddRowList(row)
	})
	return q.ValuesGrid(grid)
}

func (q *InsertQuery) ValuesGrid(rows collectionx.Grid[Assignment]) *InsertQuery {
	q.Rows = mergeGrid(q.Rows, rows)
	if q.Rows.RowCount() == 1 {
		row, _ := q.Rows.GetRowList(0)
		q.Assignments = row
	} else {
		q.Assignments = nil
	}
	return q
}

func (q *InsertQuery) FromSelect(query *SelectQuery) *InsertQuery {
	q.Source = query
	return q
}

func (q *InsertQuery) Returning(items ...SelectItem) *InsertQuery {
	return q.ReturningList(CompactSelectItems(items))
}

func (q *InsertQuery) ReturningList(items collectionx.List[SelectItem]) *InsertQuery {
	q.ReturningItems = mergeList(q.ReturningItems, items)
	return q
}

func (q *InsertQuery) OnConflict(targets ...Expression) *ConflictBuilder {
	return q.OnConflictList(CompactExpressions(targets))
}

func (q *InsertQuery) OnConflictList(targets collectionx.List[Expression]) *ConflictBuilder {
	q.Upsert = &UpsertClause{Targets: targets.Clone()}
	return &ConflictBuilder{query: q}
}

func (b *ConflictBuilder) DoNothing() *InsertQuery {
	b.query.Upsert = &UpsertClause{
		Targets:   b.query.Upsert.Targets.Clone(),
		DoNothing: true,
	}
	return b.query
}

func (b *ConflictBuilder) DoUpdateSet(assignments ...Assignment) *InsertQuery {
	return b.DoUpdateSetList(CompactAssignments(assignments))
}

func (b *ConflictBuilder) DoUpdateSetList(assignments collectionx.List[Assignment]) *InsertQuery {
	b.query.Upsert = &UpsertClause{
		Targets:     b.query.Upsert.Targets.Clone(),
		Assignments: assignments.Clone(),
	}
	return b.query
}

func Update(source TableSource) *UpdateQuery {
	return &UpdateQuery{Table: TableRef(source)}
}

func (q *UpdateQuery) Set(assignments ...Assignment) *UpdateQuery {
	return q.SetList(CompactAssignments(assignments))
}

func (q *UpdateQuery) SetList(assignments collectionx.List[Assignment]) *UpdateQuery {
	q.Assignments = mergeList(q.Assignments, assignments)
	return q
}

func (q *UpdateQuery) Where(predicate Predicate) *UpdateQuery {
	q.WhereExp = predicate
	return q
}

func (q *UpdateQuery) Returning(items ...SelectItem) *UpdateQuery {
	return q.ReturningList(CompactSelectItems(items))
}

func (q *UpdateQuery) ReturningList(items collectionx.List[SelectItem]) *UpdateQuery {
	q.ReturningItems = mergeList(q.ReturningItems, items)
	return q
}

func DeleteFrom(source TableSource) *DeleteQuery {
	return &DeleteQuery{From: TableRef(source)}
}

func (q *DeleteQuery) Where(predicate Predicate) *DeleteQuery {
	q.WhereExp = predicate
	return q
}

func (q *DeleteQuery) Returning(items ...SelectItem) *DeleteQuery {
	return q.ReturningList(CompactSelectItems(items))
}

func (q *DeleteQuery) ReturningList(items collectionx.List[SelectItem]) *DeleteQuery {
	q.ReturningItems = mergeList(q.ReturningItems, items)
	return q
}

func mergeGrid[T any](current, next collectionx.Grid[T]) collectionx.Grid[T] {
	if current == nil {
		return next.Clone()
	}
	current.Merge(next)
	return current
}
