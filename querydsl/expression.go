package querydsl

import (
	"errors"

	collectionx "github.com/arcgolabs/collectionx/list"
	schemax "github.com/arcgolabs/dbx/schema"
)

type Expression interface {
	QueryExpression()
}

type Operand interface {
	Expression
	RenderOperand(*State) (string, error)
}

type ColumnAccessor interface {
	ColumnRef() schemax.ColumnMeta
}

type TypedColumn[T any] interface {
	Operand
	ColumnAccessor
	ColumnType(T)
}

type Predicate interface {
	Expression
	QueryPredicate()
	RenderPredicate(*State) error
}

type Condition = Predicate

type SelectItem interface {
	QuerySelectItem()
}

type SelectItemRenderer interface {
	RenderSelectItem(*State) error
}

type Assignment interface {
	QueryAssignment()
	RenderAssignment(*State) error
}

type InsertAssignment interface {
	Assignment
	RenderAssignmentValue(*State) error
	AssignmentColumn() schemax.ColumnMeta
}

type Order interface {
	QueryOrder()
	RenderOrder(*State) error
}

type ValueOperand[T any] struct {
	Value T
}

type comparisonPredicate struct {
	Left  Operand
	Op    ComparisonOperator
	Right any
}

type logicalPredicate struct {
	Op         LogicalOperator
	Predicates *collectionx.List[Predicate]
}

type notPredicate struct {
	Predicate Predicate
}

type existsPredicate struct {
	Query *SelectQuery
}

func (ValueOperand[T]) QueryExpression()     {}
func (comparisonPredicate) QueryExpression() {}
func (comparisonPredicate) QueryPredicate()  {}
func (logicalPredicate) QueryExpression()    {}
func (logicalPredicate) QueryPredicate()     {}
func (notPredicate) QueryExpression()        {}
func (notPredicate) QueryPredicate()         {}
func (existsPredicate) QueryExpression()     {}
func (existsPredicate) QueryPredicate()      {}
func (v ValueOperand[T]) RenderOperand(s *State) (string, error) {
	return s.Bind(any(v.Value)), nil
}

func Value[T any](value T) ValueOperand[T] {
	return ValueOperand[T]{Value: value}
}

func Compare(left Operand, op ComparisonOperator, right any) Predicate {
	return comparisonPredicate{Left: left, Op: op, Right: right}
}

func And(predicates ...Predicate) Predicate {
	return AndList(CompactPredicates(predicates))
}

func Or(predicates ...Predicate) Predicate {
	return OrList(CompactPredicates(predicates))
}

func AndList(predicates *collectionx.List[Predicate]) Predicate {
	items := CompactPredicatesList(predicates)
	if items.Len() == 1 {
		predicate, _ := items.GetFirst()
		return predicate
	}
	return logicalPredicate{Op: LogicalAnd, Predicates: items}
}

func OrList(predicates *collectionx.List[Predicate]) Predicate {
	items := CompactPredicatesList(predicates)
	if items.Len() == 1 {
		predicate, _ := items.GetFirst()
		return predicate
	}
	return logicalPredicate{Op: LogicalOr, Predicates: items}
}

func Not(predicate Predicate) Predicate {
	return notPredicate{Predicate: predicate}
}

func Like(left TypedColumn[string], pattern string) Predicate {
	return Compare(left, OpLike, Value(pattern))
}

func Exists(query *SelectQuery) Predicate {
	return existsPredicate{Query: query}
}

func CompactExpressions(expressions []Expression) *collectionx.List[Expression] {
	return CompactExpressionsList(collectionx.NewList[Expression](expressions...))
}

func CompactExpressionsList(expressions *collectionx.List[Expression]) *collectionx.List[Expression] {
	return collectionx.FilterList[Expression](expressions, func(_ int, expression Expression) bool {
		return expression != nil
	})
}

func CompactPredicates(predicates []Predicate) *collectionx.List[Predicate] {
	return CompactPredicatesList(collectionx.NewList[Predicate](predicates...))
}

func CompactPredicatesList(predicates *collectionx.List[Predicate]) *collectionx.List[Predicate] {
	return collectionx.FilterList[Predicate](predicates, func(_ int, predicate Predicate) bool {
		return predicate != nil
	})
}

func CompactAssignments(assignments []Assignment) *collectionx.List[Assignment] {
	return CompactAssignmentsList(collectionx.NewList[Assignment](assignments...))
}

func CompactAssignmentsList(assignments *collectionx.List[Assignment]) *collectionx.List[Assignment] {
	return collectionx.FilterList[Assignment](assignments, func(_ int, assignment Assignment) bool {
		return assignment != nil
	})
}

func CompactSelectItems(items []SelectItem) *collectionx.List[SelectItem] {
	return CompactSelectItemsList(collectionx.NewList[SelectItem](items...))
}

func CompactSelectItemsList(items *collectionx.List[SelectItem]) *collectionx.List[SelectItem] {
	return collectionx.FilterList[SelectItem](items, func(_ int, item SelectItem) bool {
		return item != nil
	})
}

func CompactOrders(orders []Order) *collectionx.List[Order] {
	return CompactOrdersList(collectionx.NewList[Order](orders...))
}

func CompactOrdersList(orders *collectionx.List[Order]) *collectionx.List[Order] {
	return collectionx.FilterList[Order](orders, func(_ int, order Order) bool {
		return order != nil
	})
}

func (p comparisonPredicate) RenderPredicate(state *State) error {
	left, err := p.Left.RenderOperand(state)
	if err != nil {
		return wrapRenderError("render comparison left operand", err)
	}
	state.WriteString(left)
	if p.Op == OpIs || p.Op == OpIsNot {
		state.WriteRawByte(' ')
		state.WriteString(string(p.Op))
		state.WriteString(" NULL")
		return nil
	}
	operand, err := RenderOperandValue(state, p.Right)
	if err != nil {
		return err
	}
	state.WriteRawByte(' ')
	state.WriteString(string(p.Op))
	state.WriteRawByte(' ')
	state.WriteString(operand)
	return nil
}

func (p logicalPredicate) RenderPredicate(state *State) error {
	if p.Predicates.Len() == 0 {
		return errors.New("dbx/querydsl: logical predicate requires nested predicates")
	}
	state.WriteRawByte('(')
	var renderErr error
	p.Predicates.Range(func(index int, predicate Predicate) bool {
		if index > 0 {
			state.WriteRawByte(' ')
			state.WriteString(string(p.Op))
			state.WriteRawByte(' ')
		}
		if err := RenderPredicate(state, predicate); err != nil {
			renderErr = err
			return false
		}
		return true
	})
	if renderErr != nil {
		return renderErr
	}
	state.WriteRawByte(')')
	return nil
}

func (p notPredicate) RenderPredicate(state *State) error {
	if p.Predicate == nil {
		return errors.New("dbx/querydsl: NOT predicate requires nested predicate")
	}
	state.WriteString("NOT (")
	if err := RenderPredicate(state, p.Predicate); err != nil {
		return err
	}
	state.WriteRawByte(')')
	return nil
}

func (p existsPredicate) RenderPredicate(state *State) error {
	if p.Query == nil {
		return errors.New("dbx/querydsl: EXISTS predicate requires subquery")
	}
	state.WriteString("EXISTS (")
	if err := renderSelectStatement(state, p.Query); err != nil {
		return err
	}
	state.WriteRawByte(')')
	return nil
}
