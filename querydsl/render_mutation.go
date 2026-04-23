//revive:disable:file-length-limit Mutation rendering helpers are kept together to preserve related SQL behavior.

package querydsl

import (
	"errors"
	"fmt"
	schemax "github.com/arcgolabs/dbx/schema"

	"github.com/arcgolabs/collectionx"
	"github.com/arcgolabs/dbx/dialect"
	"github.com/arcgolabs/dbx/sqlstmt"
)

func (q *InsertQuery) Build(d dialect.Dialect) (sqlstmt.Bound, error) {
	if q == nil {
		return sqlstmt.Bound{}, errors.New("dbx/querydsl: insert query is nil")
	}
	rows := normalizedInsertRows(q)
	if err := validateInsertQuery(q, rows); err != nil {
		return sqlstmt.Bound{}, err
	}

	state := NewState(d, rows.RowCount()*4)
	state.WriteString(insertStatementPrefix(d, q))
	if err := renderInsertBody(state, q, rows); err != nil {
		return sqlstmt.Bound{}, err
	}
	if err := renderUpsert(state, q); err != nil {
		return sqlstmt.Bound{}, err
	}
	if err := renderReturning(state, q.ReturningItems); err != nil {
		return sqlstmt.Bound{}, err
	}
	if err := state.Err(); err != nil {
		return sqlstmt.Bound{}, err
	}
	return state.Bound(), nil
}

func validateInsertQuery(q *InsertQuery, rows collectionx.Grid[Assignment]) error {
	switch {
	case q.Into.Name() == "":
		return errors.New("dbx/querydsl: insert query requires target table")
	case rows.RowCount() == 0 && q.Source == nil:
		return errors.New("dbx/querydsl: insert query requires values or source query")
	case rows.RowCount() > 0 && q.Source != nil:
		return errors.New("dbx/querydsl: insert query cannot combine values and source query")
	case q.Source != nil && q.TargetColumns.Len() == 0:
		return errors.New("dbx/querydsl: insert-select requires target columns")
	default:
		return nil
	}
}

func insertStatementPrefix(d dialect.Dialect, q *InsertQuery) string {
	features := DialectFeatures(d)
	if features.InsertIgnoreForUpsertNothing && q.Upsert != nil && q.Upsert.DoNothing {
		return "INSERT IGNORE INTO "
	}
	return "INSERT INTO "
}

func renderInsertBody(state *State, q *InsertQuery, rows collectionx.Grid[Assignment]) error {
	state.RenderTable(q.Into)
	columns, err := resolveInsertColumns(q, rows)
	if err != nil {
		return err
	}
	if err := renderInsertColumns(state, columns); err != nil {
		return err
	}
	return renderInsertSourceOrValues(state, q, columns, rows)
}

func renderInsertColumns(state *State, columns collectionx.List[schemax.ColumnMeta]) error {
	if columns.Len() == 0 {
		return nil
	}
	state.WriteString(" (")
	columns.Range(func(index int, column schemax.ColumnMeta) bool {
		if index > 0 {
			state.WriteString(", ")
		}
		state.WriteQuotedIdent(column.Name)
		return true
	})
	state.WriteRawByte(')')
	return nil
}

func renderInsertSourceOrValues(state *State, q *InsertQuery, columns collectionx.List[schemax.ColumnMeta], rows collectionx.Grid[Assignment]) error {
	if q.Source != nil {
		state.WriteRawByte(' ')
		return renderSelectQuery(state, q.Source)
	}
	return renderInsertValues(state, columns, rows)
}

func renderInsertValues(state *State, columns collectionx.List[schemax.ColumnMeta], rows collectionx.Grid[Assignment]) error {
	orderedRows, err := orderInsertRows(columns, rows)
	if err != nil {
		return err
	}
	state.WriteString(" VALUES ")
	var renderErr error
	orderedRows.Range(func(rowIndex int, row []Assignment) bool {
		if rowIndex > 0 {
			state.WriteString(", ")
		}
		if err := renderInsertValueRow(state, row); err != nil {
			renderErr = err
			return false
		}
		return true
	})
	return renderErr
}

func renderInsertValueRow(state *State, row []Assignment) error {
	state.WriteRawByte('(')
	for colIndex, assignment := range row {
		renderer, ok := assignment.(InsertAssignment)
		if !ok {
			return fmt.Errorf("dbx/querydsl: unsupported insert assignment %T", assignment)
		}
		if colIndex > 0 {
			state.WriteString(", ")
		}
		if err := renderer.RenderAssignmentValue(state); err != nil {
			return wrapRenderError("render insert assignment value", err)
		}
	}
	state.WriteRawByte(')')
	return nil
}

func (q *UpdateQuery) Build(d dialect.Dialect) (sqlstmt.Bound, error) {
	if q == nil {
		return sqlstmt.Bound{}, errors.New("dbx/querydsl: update query is nil")
	}
	if err := validateUpdateQuery(q); err != nil {
		return sqlstmt.Bound{}, err
	}

	state := NewState(d, q.Assignments.Len())
	state.WriteString("UPDATE ")
	state.RenderTable(q.Table)
	state.WriteString(" SET ")
	if err := renderUpdateAssignments(state, q.Assignments); err != nil {
		return sqlstmt.Bound{}, err
	}
	if err := renderOptionalWhere(state, q.WhereExp); err != nil {
		return sqlstmt.Bound{}, err
	}
	if err := renderReturning(state, q.ReturningItems); err != nil {
		return sqlstmt.Bound{}, err
	}
	if err := state.Err(); err != nil {
		return sqlstmt.Bound{}, err
	}
	return state.Bound(), nil
}

func validateUpdateQuery(q *UpdateQuery) error {
	switch {
	case q.Table.Name() == "":
		return errors.New("dbx/querydsl: update query requires target table")
	case q.Assignments.Len() == 0:
		return errors.New("dbx/querydsl: update query requires assignments")
	default:
		return nil
	}
}

func renderUpdateAssignments(state *State, assignments collectionx.List[Assignment]) error {
	var renderErr error
	assignments.Range(func(index int, assignment Assignment) bool {
		if index > 0 {
			state.WriteString(", ")
		}
		if err := RenderAssignment(state, assignment); err != nil {
			renderErr = err
			return false
		}
		return true
	})
	return renderErr
}

func renderOptionalWhere(state *State, predicate Predicate) error {
	if predicate == nil {
		return nil
	}
	state.WriteString(" WHERE ")
	return RenderPredicate(state, predicate)
}

func (q *DeleteQuery) Build(d dialect.Dialect) (sqlstmt.Bound, error) {
	if q == nil {
		return sqlstmt.Bound{}, errors.New("dbx/querydsl: delete query is nil")
	}
	if q.From.Name() == "" {
		return sqlstmt.Bound{}, errors.New("dbx/querydsl: delete query requires target table")
	}

	state := NewState(d, 4)
	state.WriteString("DELETE FROM ")
	state.RenderTable(q.From)
	if err := renderOptionalWhere(state, q.WhereExp); err != nil {
		return sqlstmt.Bound{}, err
	}
	if err := renderReturning(state, q.ReturningItems); err != nil {
		return sqlstmt.Bound{}, err
	}
	if err := state.Err(); err != nil {
		return sqlstmt.Bound{}, err
	}
	return state.Bound(), nil
}

func normalizedInsertRows(q *InsertQuery) collectionx.Grid[Assignment] {
	if q.Rows.RowCount() > 0 {
		return q.Rows
	}
	if q.Assignments.Len() > 0 {
		rows := collectionx.NewGridWithCapacity[Assignment](1)
		rows.AddRowList(q.Assignments)
		return rows
	}
	return nil
}

func resolveInsertColumns(q *InsertQuery, rows collectionx.Grid[Assignment]) (collectionx.List[schemax.ColumnMeta], error) {
	if q.TargetColumns.Len() > 0 {
		return resolveTargetColumns(q.TargetColumns)
	}
	row, ok := rows.FirstRowWhere(func(_ int, _ []Assignment) bool { return true }).Get()
	if !ok {
		return collectionx.NewList[schemax.ColumnMeta](), nil
	}
	return assignmentColumns(row)
}

func assignmentColumns(assignments []Assignment) (collectionx.List[schemax.ColumnMeta], error) {
	columns := collectionx.NewListWithCapacity[schemax.ColumnMeta](len(assignments))
	for _, assignment := range assignments {
		renderer, ok := assignment.(InsertAssignment)
		if !ok {
			return nil, fmt.Errorf("dbx/querydsl: unsupported insert assignment %T", assignment)
		}
		columns.Add(renderer.AssignmentColumn())
	}
	return columns, nil
}

func resolveTargetColumns(expressions collectionx.List[Expression]) (collectionx.List[schemax.ColumnMeta], error) {
	columns := collectionx.NewListWithCapacity[schemax.ColumnMeta](expressions.Len())
	var resolveErr error
	expressions.Range(func(_ int, expression Expression) bool {
		column, ok := expression.(ColumnAccessor)
		if !ok {
			resolveErr = fmt.Errorf("dbx/querydsl: unsupported target column expression %T", expression)
			return false
		}
		columns.Add(column.ColumnRef())
		return true
	})
	if resolveErr != nil {
		return nil, resolveErr
	}
	return columns, nil
}

func orderInsertRows(columns collectionx.List[schemax.ColumnMeta], rows collectionx.Grid[Assignment]) (collectionx.Grid[Assignment], error) {
	orderedRows := collectionx.NewGridWithCapacity[Assignment](rows.RowCount())
	var orderErr error
	rows.Range(func(_ int, row []Assignment) bool {
		orderedRow, err := orderInsertRow(columns, row)
		if err != nil {
			orderErr = err
			return false
		}
		orderedRows.AddRowList(orderedRow)
		return true
	})
	if orderErr != nil {
		return nil, orderErr
	}
	return orderedRows, nil
}

func orderInsertRow(columns collectionx.List[schemax.ColumnMeta], row []Assignment) (collectionx.List[Assignment], error) {
	assignmentsByColumn := collectionx.NewMapWithCapacity[string, Assignment](len(row))
	for _, assignment := range row {
		renderer, ok := assignment.(InsertAssignment)
		if !ok {
			return nil, fmt.Errorf("dbx/querydsl: unsupported insert assignment %T", assignment)
		}
		assignmentsByColumn.Set(renderer.AssignmentColumn().Name, assignment)
	}

	orderedRow := collectionx.NewListWithCapacity[Assignment](columns.Len())
	var orderErr error
	columns.Range(func(_ int, column schemax.ColumnMeta) bool {
		assignment, ok := assignmentsByColumn.Get(column.Name)
		if !ok {
			orderErr = fmt.Errorf("dbx/querydsl: missing value for insert column %s", column.Name)
			return false
		}
		orderedRow.Add(assignment)
		return true
	})
	if orderErr != nil {
		return nil, orderErr
	}
	return orderedRow, nil
}
