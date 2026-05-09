package querydsl

import (
	"errors"

	collectionx "github.com/arcgolabs/collectionx/list"
)

type betweenPredicate struct {
	Left    Operand
	Lower   any
	Upper   any
	Negated bool
}

func (betweenPredicate) QueryExpression() {}
func (betweenPredicate) QueryPredicate()  {}

func In[T any](left TypedOperand[T], values ...T) Predicate {
	return InList(left, collectionx.NewList[T](values...))
}

func InList[T any](left TypedOperand[T], values *collectionx.List[T]) Predicate {
	return Compare(left, OpIn, typedValueList(values))
}

func NotIn[T any](left TypedOperand[T], values ...T) Predicate {
	return NotInList(left, collectionx.NewList[T](values...))
}

func NotInList[T any](left TypedOperand[T], values *collectionx.List[T]) Predicate {
	return Compare(left, OpNotIn, typedValueList(values))
}

func InQuery[T any](left TypedOperand[T], query SelectResult[T]) Predicate {
	return Compare(left, OpIn, query.Subquery())
}

func NotInQuery[T any](left TypedOperand[T], query SelectResult[T]) Predicate {
	return Compare(left, OpNotIn, query.Subquery())
}

func Like(left TypedOperand[string], pattern string) Predicate {
	return Compare(left, OpLike, Value(pattern))
}

func NotLike(left TypedOperand[string], pattern string) Predicate {
	return Compare(left, OpNotLike, Value(pattern))
}

func Between[T any](left TypedOperand[T], lower, upper T) Predicate {
	return BetweenOperands(left, Value(lower), Value(upper))
}

func NotBetween[T any](left TypedOperand[T], lower, upper T) Predicate {
	return NotBetweenOperands(left, Value(lower), Value(upper))
}

func BetweenOperands[T any](left, lower, upper TypedOperand[T]) Predicate {
	return betweenPredicate{Left: left, Lower: lower, Upper: upper}
}

func NotBetweenOperands[T any](left, lower, upper TypedOperand[T]) Predicate {
	return betweenPredicate{Left: left, Lower: lower, Upper: upper, Negated: true}
}

func typedValueList[T any](values *collectionx.List[T]) *collectionx.List[any] {
	if values == nil {
		return nil
	}
	return collectionx.MapList[T, any](values, func(_ int, value T) any {
		return value
	})
}

func (p betweenPredicate) RenderPredicate(state *State) error {
	if p.Left == nil {
		return errors.New("dbx/querydsl: BETWEEN predicate requires left operand")
	}
	left, err := p.Left.RenderOperand(state)
	if err != nil {
		return wrapRenderError("render between left operand", err)
	}
	lower, err := RenderOperandValue(state, p.Lower)
	if err != nil {
		return wrapRenderError("render between lower operand", err)
	}
	upper, err := RenderOperandValue(state, p.Upper)
	if err != nil {
		return wrapRenderError("render between upper operand", err)
	}
	state.WriteString(left)
	if p.Negated {
		state.WriteString(" NOT")
	}
	state.WriteString(" BETWEEN ")
	state.WriteString(lower)
	state.WriteString(" AND ")
	state.WriteString(upper)
	return nil
}
