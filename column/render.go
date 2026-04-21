package column

import (
	"fmt"

	"github.com/arcgolabs/dbx/querydsl"
	schemax "github.com/arcgolabs/dbx/schema"
)

type columnOperand[T any] struct {
	Column Typed[T]
}

type excludedColumnOperand[T any] struct {
	Column schemax.ColumnMeta
}

type columnAssignment[E any, T any] struct {
	Column Column[E, T]
	Value  any
}

type columnOrder[E any, T any] struct {
	Column     Column[E, T]
	Descending bool
}

func (c Column[E, T]) RenderOperand(state *querydsl.State) (string, error) {
	return renderColumnOperand(state, c.columnRef())
}

func (o columnOperand[T]) QueryExpression() {}

func (o columnOperand[T]) RenderOperand(state *querydsl.State) (string, error) {
	return renderColumnOperand(state, o.Column.ColumnRef())
}

func renderColumnOperand(state *querydsl.State, meta schemax.ColumnMeta) (string, error) {
	var builder querydsl.Buffer
	table := meta.Table
	if meta.Alias != "" {
		table = meta.Alias
	}
	builder.WriteString(state.Dialect().QuoteIdent(table))
	builder.WriteRawByte('.')
	builder.WriteString(state.Dialect().QuoteIdent(meta.Name))
	return builder.String(), builder.Err("render column operand")
}

func wrapRenderError(op string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("dbx/column: %s: %w", op, err)
}

func (a columnAssignment[E, T]) QueryAssignment() {}

func (a columnAssignment[E, T]) AssignmentColumn() schemax.ColumnMeta {
	return a.Column.columnRef()
}

func (a columnAssignment[E, T]) RenderAssignment(state *querydsl.State) error {
	state.WriteQuotedIdent(a.Column.Name())
	state.WriteString(" = ")
	operand, err := querydsl.RenderOperandValue(state, a.Value)
	if err != nil {
		return wrapRenderError("render assignment operand", err)
	}
	state.WriteString(operand)
	return nil
}

func (a columnAssignment[E, T]) RenderAssignmentValue(state *querydsl.State) error {
	operand, err := querydsl.RenderOperandValue(state, a.Value)
	if err != nil {
		return wrapRenderError("render assignment value", err)
	}
	state.WriteString(operand)
	return nil
}

func (o columnOrder[E, T]) QueryOrder() {}

func (o columnOrder[E, T]) RenderOrder(state *querydsl.State) error {
	state.RenderColumn(o.Column.columnRef())
	if o.Descending {
		state.WriteString(" DESC")
		return nil
	}
	state.WriteString(" ASC")
	return nil
}

func (o excludedColumnOperand[T]) QueryExpression() {}

func (o excludedColumnOperand[T]) RenderOperand(state *querydsl.State) (string, error) {
	f := querydsl.DialectFeatures(state.Dialect())
	quoted := state.Dialect().QuoteIdent(o.Column.Name)
	switch f.ExcludedRefStyle {
	case "excluded":
		return "EXCLUDED." + quoted, nil
	case "values":
		return "VALUES(" + quoted + ")", nil
	default:
		return "", fmt.Errorf("dbx/column: excluded assignment is not supported for dialect %s", state.Dialect().Name())
	}
}
