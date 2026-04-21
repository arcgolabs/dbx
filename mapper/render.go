package mapper

import (
	"fmt"

	"github.com/arcgolabs/dbx/querydsl"
	schemax "github.com/arcgolabs/dbx/schema"
)

type metadataAssignment struct {
	meta  schemax.ColumnMeta
	value any
}

type metadataComparisonPredicate struct {
	left  schemax.ColumnMeta
	op    querydsl.ComparisonOperator
	right any
}

func (metadataAssignment) QueryAssignment() {}

func (metadataComparisonPredicate) QueryExpression() {}
func (metadataComparisonPredicate) QueryPredicate()  {}

func (a metadataAssignment) AssignmentColumn() schemax.ColumnMeta {
	return a.meta
}

func (a metadataAssignment) RenderAssignment(state *querydsl.State) error {
	state.WriteQuotedIdent(a.meta.Name)
	state.WriteString(" = ")
	operand, err := querydsl.RenderOperandValue(state, a.value)
	if err != nil {
		return fmt.Errorf("dbx/mapper: render metadata assignment operand: %w", err)
	}
	state.WriteString(operand)
	return nil
}

func (a metadataAssignment) RenderAssignmentValue(state *querydsl.State) error {
	operand, err := querydsl.RenderOperandValue(state, a.value)
	if err != nil {
		return fmt.Errorf("dbx/mapper: render metadata assignment value: %w", err)
	}
	state.WriteString(operand)
	return nil
}

func (p metadataComparisonPredicate) RenderPredicate(state *querydsl.State) error {
	state.RenderColumn(p.left)
	if p.op == querydsl.OpIs || p.op == querydsl.OpIsNot {
		state.WriteRawByte(' ')
		state.WriteString(string(p.op))
		state.WriteString(" NULL")
		return nil
	}

	operand, err := querydsl.RenderOperandValue(state, p.right)
	if err != nil {
		return fmt.Errorf("dbx/mapper: render metadata predicate operand: %w", err)
	}
	state.WriteRawByte(' ')
	state.WriteString(string(p.op))
	state.WriteRawByte(' ')
	state.WriteString(operand)
	return nil
}
