package querydsl

import collectionx "github.com/arcgolabs/collectionx/list"

// Clone returns a mutable copy of the insert query.
func (q *InsertQuery) Clone() *InsertQuery {
	if q == nil {
		return nil
	}
	return &InsertQuery{
		Into:           q.Into,
		TargetColumns:  cloneList(q.TargetColumns),
		Assignments:    cloneList(q.Assignments),
		Rows:           cloneGrid(q.Rows),
		Source:         q.Source.Clone(),
		Upsert:         cloneUpsert(q.Upsert),
		ReturningItems: cloneList(q.ReturningItems),
	}
}

// Clone returns a mutable copy of the update query.
func (q *UpdateQuery) Clone() *UpdateQuery {
	if q == nil {
		return nil
	}
	return &UpdateQuery{
		Table:          q.Table,
		Assignments:    cloneList(q.Assignments),
		WhereExp:       q.WhereExp,
		ReturningItems: cloneList(q.ReturningItems),
	}
}

// Clone returns a mutable copy of the delete query.
func (q *DeleteQuery) Clone() *DeleteQuery {
	if q == nil {
		return nil
	}
	return &DeleteQuery{
		From:           q.From,
		WhereExp:       q.WhereExp,
		ReturningItems: cloneList(q.ReturningItems),
	}
}

func cloneList[T any](items *collectionx.List[T]) *collectionx.List[T] {
	if items == nil {
		return nil
	}
	return items.Clone()
}

func cloneGrid[T any](items *collectionx.Grid[T]) *collectionx.Grid[T] {
	if items == nil {
		return nil
	}
	return items.Clone()
}

func cloneUpsert(clause *UpsertClause) *UpsertClause {
	if clause == nil {
		return nil
	}
	return &UpsertClause{
		Targets:     cloneList(clause.Targets),
		DoNothing:   clause.DoNothing,
		Assignments: cloneList(clause.Assignments),
	}
}
