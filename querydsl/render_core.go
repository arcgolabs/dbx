package querydsl

import (
	"errors"
	"fmt"
	schemax "github.com/arcgolabs/dbx/schema"
	"strings"

	collectionx "github.com/arcgolabs/collectionx/list"
	"github.com/arcgolabs/dbx/dialect"
	"github.com/arcgolabs/dbx/sqlstmt"
)

type Builder interface {
	Build(d dialect.Dialect) (sqlstmt.Bound, error)
}

type State struct {
	dialect  dialect.Dialect
	buf      strings.Builder
	args     *collectionx.List[any]
	writeErr error
}

type Buffer struct {
	buf strings.Builder
	err error
}

func NewState(d dialect.Dialect, capacity int) *State {
	return &State{dialect: d, args: collectionx.NewListWithCapacity[any](capacity)}
}

func (s *State) Dialect() dialect.Dialect {
	return s.dialect
}

func (s *State) WriteString(text string) {
	if s.writeErr != nil {
		return
	}
	_, s.writeErr = s.buf.WriteString(text)
}

func (s *State) WriteRawByte(value byte) {
	if s.writeErr != nil {
		return
	}
	s.writeErr = s.buf.WriteByte(value)
}

func (s *State) Err() error {
	if s.writeErr == nil {
		return nil
	}
	return fmt.Errorf("dbx/querydsl: write rendered SQL: %w", s.writeErr)
}

func (b *Buffer) WriteString(text string) {
	if b.err != nil {
		return
	}
	_, b.err = b.buf.WriteString(text)
}

func (b *Buffer) WriteRawByte(value byte) {
	if b.err != nil {
		return
	}
	b.err = b.buf.WriteByte(value)
}

func (b *Buffer) String() string {
	return b.buf.String()
}

func (b *Buffer) Err(op string) error {
	if b.err == nil {
		return nil
	}
	return fmt.Errorf("dbx/querydsl: %s: %w", op, b.err)
}

func wrapRenderError(op string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("dbx/querydsl: %s: %w", op, err)
}

func (s *State) Bind(value any) string {
	s.args.Add(value)
	return s.dialect.BindVar(s.args.Len())
}

func (s *State) WriteQuotedIdent(name string) {
	s.WriteString(s.dialect.QuoteIdent(name))
}

func (s *State) WriteQualifiedIdent(table, column string) {
	if table != "" {
		s.WriteQuotedIdent(table)
		s.WriteRawByte('.')
	}
	s.WriteQuotedIdent(column)
}

func (s *State) RenderColumn(meta schemax.ColumnMeta) {
	table := meta.Table
	if meta.Alias != "" {
		table = meta.Alias
	}
	s.WriteQualifiedIdent(table, meta.Name)
}

func (s *State) RenderTable(table Table) {
	s.WriteQuotedIdent(table.Name())
	if alias := table.Alias(); alias != "" && alias != table.Name() {
		s.WriteString(" AS ")
		s.WriteQuotedIdent(alias)
	}
}

func (s *State) Bound() sqlstmt.Bound {
	return sqlstmt.Bound{SQL: s.buf.String(), Args: s.args.Clone()}
}

func RenderSelectItem(state *State, item SelectItem) error {
	if renderer, ok := item.(SelectItemRenderer); ok {
		return wrapRenderError("render select item", renderer.RenderSelectItem(state))
	}
	if operand, ok := item.(Operand); ok {
		value, err := operand.RenderOperand(state)
		if err != nil {
			return wrapRenderError("render select item operand", err)
		}
		state.WriteString(value)
		return nil
	}
	return fmt.Errorf("dbx/querydsl: unsupported select item %T", item)
}

func RenderPredicate(state *State, predicate Predicate) error {
	if predicate == nil {
		return errors.New("dbx/querydsl: predicate is nil")
	}
	return wrapRenderError("render predicate", predicate.RenderPredicate(state))
}

func RenderAssignment(state *State, assignment Assignment) error {
	if assignment == nil {
		return errors.New("dbx/querydsl: assignment is nil")
	}
	return wrapRenderError("render assignment", assignment.RenderAssignment(state))
}

func RenderOrder(state *State, order Order) error {
	if order == nil {
		return errors.New("dbx/querydsl: order is nil")
	}
	return wrapRenderError("render order", order.RenderOrder(state))
}

func RenderPredicateValue(state *State, predicate Predicate) (string, error) {
	original := state.buf
	var builder strings.Builder
	state.buf = builder
	if err := RenderPredicate(state, predicate); err != nil {
		state.buf = original
		return "", err
	}
	rendered := state.buf.String()
	state.buf = original
	return rendered, nil
}

func RenderOperandValue(state *State, value any) (string, error) {
	if renderer, ok := value.(Operand); ok {
		operand, err := renderer.RenderOperand(state)
		if err != nil {
			return "", wrapRenderError("render operand", err)
		}
		return operand, nil
	}
	if values, ok := value.(*collectionx.List[any]); ok {
		return renderListOperand(state, values)
	}
	if values, ok := value.([]any); ok {
		return renderListOperand(state, collectionx.NewList[any](values...))
	}
	return state.Bind(value), nil
}

func renderListOperand(state *State, values *collectionx.List[any]) (string, error) {
	if values == nil || values.Len() == 0 {
		return "", errors.New("dbx/querydsl: IN operand cannot be empty")
	}
	var builder Buffer
	builder.WriteRawByte('(')
	values.Range(func(index int, value any) bool {
		if index > 0 {
			builder.WriteString(", ")
		}
		builder.WriteString(state.Bind(value))
		return true
	})
	builder.WriteRawByte(')')
	return builder.String(), builder.Err("render list operand")
}

func DialectFeatures(d dialect.Dialect) dialect.QueryFeatures {
	if p, ok := d.(dialect.QueryFeaturesProvider); ok {
		return p.QueryFeatures()
	}
	return dialect.DefaultQueryFeatures(d.Name())
}
