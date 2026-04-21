package querydsl

import (
	"errors"
	"fmt"

	"github.com/DaiYuANg/arcgo/collectionx"
)

func renderUpsert(state *State, q *InsertQuery) error {
	if q.Upsert == nil {
		return nil
	}
	switch DialectFeatures(state.Dialect()).UpsertVariant {
	case "on_conflict":
		return renderUpsertOnConflict(state, q)
	case "on_duplicate_key":
		return renderUpsertOnDuplicateKey(state, q)
	default:
		return fmt.Errorf("dbx/querydsl: upsert is not supported for dialect %s", state.Dialect().Name())
	}
}

func renderUpsertOnConflict(state *State, q *InsertQuery) error {
	state.WriteString(" ON CONFLICT")
	if err := renderUpsertTargets(state, q.Upsert.Targets); err != nil {
		return err
	}
	if q.Upsert.DoNothing {
		state.WriteString(" DO NOTHING")
		return nil
	}
	if err := validateUpsertAssignments(q.Upsert); err != nil {
		return err
	}
	state.WriteString(" DO UPDATE SET ")
	return renderUpsertAssignments(state, q.Upsert.Assignments)
}

func renderUpsertTargets(state *State, targets collectionx.List[Expression]) error {
	if targets.Len() == 0 {
		return nil
	}
	state.WriteString(" (")
	var renderErr error
	targets.Range(func(index int, target Expression) bool {
		if index > 0 {
			state.WriteString(", ")
		}
		if column, ok := target.(ColumnAccessor); ok {
			state.WriteQuotedIdent(column.ColumnRef().Name)
			return true
		}
		operand, err := RenderOperandValue(state, target)
		if err != nil {
			renderErr = err
			return false
		}
		state.WriteString(operand)
		return true
	})
	if renderErr != nil {
		return renderErr
	}
	state.WriteRawByte(')')
	return nil
}

func validateUpsertAssignments(upsert *UpsertClause) error {
	switch {
	case upsert.Assignments.Len() == 0:
		return errors.New("dbx/querydsl: upsert update requires assignments")
	case upsert.Targets.Len() == 0:
		return errors.New("dbx/querydsl: upsert update requires conflict targets")
	default:
		return nil
	}
}

func renderUpsertOnDuplicateKey(state *State, q *InsertQuery) error {
	if q.Upsert.DoNothing {
		return nil
	}
	if q.Upsert.Assignments.Len() == 0 {
		return errors.New("dbx/querydsl: upsert update requires assignments")
	}
	state.WriteString(" ON DUPLICATE KEY UPDATE ")
	return renderUpsertAssignments(state, q.Upsert.Assignments)
}

func renderUpsertAssignments(state *State, assignments collectionx.List[Assignment]) error {
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

func renderReturning(state *State, items collectionx.List[SelectItem]) error {
	if items.Len() == 0 {
		return nil
	}
	if !DialectFeatures(state.Dialect()).SupportsReturning {
		return fmt.Errorf("dbx/querydsl: RETURNING is not supported for dialect %s", state.Dialect().Name())
	}
	state.WriteString(" RETURNING ")
	var renderErr error
	items.Range(func(index int, item SelectItem) bool {
		if index > 0 {
			state.WriteString(", ")
		}
		if err := RenderSelectItem(state, item); err != nil {
			renderErr = err
			return false
		}
		return true
	})
	return renderErr
}
