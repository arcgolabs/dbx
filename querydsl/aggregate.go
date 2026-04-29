package querydsl

import (
	"errors"
	"fmt"
	"strings"

	collectionx "github.com/arcgolabs/collectionx/list"
)

type Aggregate[T any] struct {
	Function AggregateFunction
	Expr     Operand
	Distinct bool
	star     bool
}

type CaseBuilder[T any] struct {
	branches *collectionx.List[caseWhenBranch]
}

type CaseExpression[T any] struct {
	Branches *collectionx.List[caseWhenBranch]
	Else     any
}

type caseWhenBranch struct {
	Predicate Predicate
	Value     any
}

type aliasedSelectItem struct {
	Item  SelectItem
	Alias string
}

type expressionOrder struct {
	Expr       Operand
	Descending bool
}

func CaseWhen[T any](predicate Predicate, value T) *CaseBuilder[T] {
	return (&CaseBuilder[T]{}).When(predicate, value)
}

func CountAll() Aggregate[int64] {
	return Aggregate[int64]{Function: AggCount, star: true}
}

func Count(expr Operand) Aggregate[int64] {
	return Aggregate[int64]{Function: AggCount, Expr: expr}
}

func CountDistinct(expr Operand) Aggregate[int64] {
	return Aggregate[int64]{Function: AggCount, Expr: expr, Distinct: true}
}

func Sum[T any](expr TypedColumn[T]) Aggregate[T] {
	return Aggregate[T]{Function: AggSum, Expr: expr}
}

func Avg[T any](expr TypedColumn[T]) Aggregate[float64] {
	return Aggregate[float64]{Function: AggAvg, Expr: expr}
}

func Min[T any](expr TypedColumn[T]) Aggregate[T] {
	return Aggregate[T]{Function: AggMin, Expr: expr}
}

func Max[T any](expr TypedColumn[T]) Aggregate[T] {
	return Aggregate[T]{Function: AggMax, Expr: expr}
}

func Alias(item SelectItem, alias string) SelectItem {
	return aliasedSelectItem{Item: item, Alias: alias}
}

func (b *CaseBuilder[T]) When(predicate Predicate, value T) *CaseBuilder[T] {
	if b == nil {
		b = &CaseBuilder[T]{}
	}
	b.branches = mergeList(b.branches, collectionx.NewList[caseWhenBranch](caseWhenBranch{Predicate: predicate, Value: value}))
	return b
}

func (b *CaseBuilder[T]) Else(value T) CaseExpression[T] {
	if b == nil {
		return CaseExpression[T]{Else: value}
	}
	return CaseExpression[T]{
		Branches: b.branches.Clone(),
		Else:     value,
	}
}

func (b *CaseBuilder[T]) End() CaseExpression[T] {
	if b == nil {
		return CaseExpression[T]{}
	}
	return CaseExpression[T]{
		Branches: b.branches.Clone(),
	}
}

func (a Aggregate[T]) As(alias string) SelectItem {
	return Alias(a, alias)
}

func (c CaseExpression[T]) As(alias string) SelectItem {
	return Alias(c, alias)
}

func (a Aggregate[T]) Eq(value T) Predicate { return Compare(a, OpEq, Value(value)) }
func (a Aggregate[T]) Ne(value T) Predicate { return Compare(a, OpNe, Value(value)) }
func (a Aggregate[T]) Gt(value T) Predicate { return Compare(a, OpGt, Value(value)) }
func (a Aggregate[T]) Ge(value T) Predicate { return Compare(a, OpGe, Value(value)) }
func (a Aggregate[T]) Lt(value T) Predicate { return Compare(a, OpLt, Value(value)) }
func (a Aggregate[T]) Le(value T) Predicate { return Compare(a, OpLe, Value(value)) }

func (a Aggregate[T]) Asc() Order {
	return expressionOrder{Expr: a}
}

func (a Aggregate[T]) Desc() Order {
	return expressionOrder{Expr: a, Descending: true}
}

func (c CaseExpression[T]) Eq(value T) Predicate { return Compare(c, OpEq, Value(value)) }
func (c CaseExpression[T]) Ne(value T) Predicate { return Compare(c, OpNe, Value(value)) }
func (c CaseExpression[T]) Gt(value T) Predicate { return Compare(c, OpGt, Value(value)) }
func (c CaseExpression[T]) Ge(value T) Predicate { return Compare(c, OpGe, Value(value)) }
func (c CaseExpression[T]) Lt(value T) Predicate { return Compare(c, OpLt, Value(value)) }
func (c CaseExpression[T]) Le(value T) Predicate { return Compare(c, OpLe, Value(value)) }

func (c CaseExpression[T]) Asc() Order {
	return expressionOrder{Expr: c}
}

func (c CaseExpression[T]) Desc() Order {
	return expressionOrder{Expr: c, Descending: true}
}

func (a Aggregate[T]) QueryExpression() {}
func (a Aggregate[T]) QuerySelectItem() {}

func (a Aggregate[T]) RenderOperand(state *State) (string, error) {
	var builder Buffer
	builder.WriteString(string(a.Function))
	builder.WriteRawByte('(')
	if a.Distinct {
		builder.WriteString("DISTINCT ")
	}
	if a.star {
		builder.WriteRawByte('*')
	} else {
		if a.Expr == nil {
			return "", fmt.Errorf("dbx/querydsl: aggregate %s requires expression", a.Function)
		}
		operand, err := a.Expr.RenderOperand(state)
		if err != nil {
			return "", wrapRenderError("render aggregate expression", err)
		}
		builder.WriteString(operand)
	}
	builder.WriteRawByte(')')
	return builder.String(), builder.Err("render aggregate operand")
}

func (a Aggregate[T]) RenderSelectItem(state *State) error {
	operand, err := a.RenderOperand(state)
	if err != nil {
		return err
	}
	state.WriteString(operand)
	return nil
}

func (c CaseExpression[T]) QueryExpression() {}
func (c CaseExpression[T]) QuerySelectItem() {}

func (c CaseExpression[T]) RenderOperand(state *State) (string, error) {
	if c.Branches.Len() == 0 {
		return "", errors.New("dbx/querydsl: CASE expression requires at least one WHEN branch")
	}

	var builder Buffer
	builder.WriteString("CASE")
	var renderErr error
	c.Branches.Range(func(_ int, branch caseWhenBranch) bool {
		if err := renderCaseBranch(&builder, state, branch); err != nil {
			renderErr = err
			return false
		}
		return true
	})
	if renderErr != nil {
		return "", renderErr
	}
	if err := renderCaseElse(&builder, state, c.Else); err != nil {
		return "", err
	}
	builder.WriteString(" END")
	return builder.String(), builder.Err("render case operand")
}

func renderCaseBranch(builder *Buffer, state *State, branch caseWhenBranch) error {
	if branch.Predicate == nil {
		return errors.New("dbx/querydsl: CASE branch requires predicate")
	}
	builder.WriteString(" WHEN ")
	predicateSQL, err := RenderPredicateValue(state, branch.Predicate)
	if err != nil {
		return wrapRenderError("render case predicate", err)
	}
	builder.WriteString(predicateSQL)
	builder.WriteString(" THEN ")
	valueSQL, err := RenderOperandValue(state, branch.Value)
	if err != nil {
		return wrapRenderError("render case value", err)
	}
	builder.WriteString(valueSQL)
	return nil
}

func renderCaseElse(builder *Buffer, state *State, value any) error {
	if value == nil {
		return nil
	}
	builder.WriteString(" ELSE ")
	elseSQL, err := RenderOperandValue(state, value)
	if err != nil {
		return wrapRenderError("render case else", err)
	}
	builder.WriteString(elseSQL)
	return nil
}

func (c CaseExpression[T]) RenderSelectItem(state *State) error {
	operand, err := c.RenderOperand(state)
	if err != nil {
		return err
	}
	state.WriteString(operand)
	return nil
}

func (o expressionOrder) QueryOrder() {}

func (o expressionOrder) RenderOrder(state *State) error {
	operand, err := o.Expr.RenderOperand(state)
	if err != nil {
		return wrapRenderError("render order expression", err)
	}
	state.WriteString(operand)
	if o.Descending {
		state.WriteString(" DESC")
		return nil
	}
	state.WriteString(" ASC")
	return nil
}

func (a aliasedSelectItem) QuerySelectItem() {}

func (a aliasedSelectItem) RenderSelectItem(state *State) error {
	if a.Item == nil {
		return errors.New("dbx/querydsl: aliased select item requires value")
	}
	if err := renderAliasedItemValue(state, a.Item); err != nil {
		return err
	}
	if strings.TrimSpace(a.Alias) == "" {
		return nil
	}
	state.WriteString(" AS ")
	state.WriteQuotedIdent(strings.TrimSpace(a.Alias))
	return nil
}

func renderAliasedItemValue(state *State, item any) error {
	switch renderer := item.(type) {
	case SelectItemRenderer:
		return wrapRenderError("render aliased select item", renderer.RenderSelectItem(state))
	case Operand:
		operand, err := renderer.RenderOperand(state)
		if err != nil {
			return wrapRenderError("render aliased operand", err)
		}
		state.WriteString(operand)
		return nil
	default:
		return fmt.Errorf("dbx/querydsl: unsupported aliased select item %T", item)
	}
}
