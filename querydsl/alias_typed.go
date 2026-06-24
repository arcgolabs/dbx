package querydsl

import (
	"errors"
	"strings"
)

type typedAliasedSelectItem[T any] struct {
	Item  TypedSelectItem[T]
	Alias string
}

// TypedAlias aliases a typed select item without dropping its result type.
func TypedAlias[T any](item TypedSelectItem[T], alias string) TypedSelectItem[T] {
	return typedAliasedSelectItem[T]{Item: item, Alias: alias}
}

func (a typedAliasedSelectItem[T]) QueryExpression() {}
func (a typedAliasedSelectItem[T]) QuerySelectItem() {}
func (a typedAliasedSelectItem[T]) ColumnType(T)     {}

func (a typedAliasedSelectItem[T]) RenderOperand(state *State) (string, error) {
	if a.Item == nil {
		return "", errors.New("dbx/querydsl: typed aliased select item requires value")
	}
	operand, err := a.Item.RenderOperand(state)
	if err != nil {
		return "", wrapRenderError("render typed aliased operand", err)
	}
	return operand, nil
}

func (a typedAliasedSelectItem[T]) RenderSelectItem(state *State) error {
	if a.Item == nil {
		return errors.New("dbx/querydsl: typed aliased select item requires value")
	}
	operand, err := a.RenderOperand(state)
	if err != nil {
		return wrapRenderError("render typed aliased select item", err)
	}
	state.WriteString(operand)
	if strings.TrimSpace(a.Alias) == "" {
		return nil
	}
	state.WriteString(" AS ")
	state.WriteQuotedIdent(strings.TrimSpace(a.Alias))
	return nil
}
