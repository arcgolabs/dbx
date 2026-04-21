package querydsl

import (
	"errors"
	"strings"
)

type SubqueryOperand struct {
	Query *SelectQuery
}

func (SubqueryOperand) QueryExpression() {}

func Subquery(query *SelectQuery) SubqueryOperand {
	return SubqueryOperand{Query: query}
}

func (s SubqueryOperand) RenderOperand(state *State) (string, error) {
	if s.Query == nil {
		return "", errors.New("dbx/querydsl: subquery is nil")
	}
	original := state.buf
	var builder strings.Builder
	state.buf = builder
	if err := renderSelectStatement(state, s.Query); err != nil {
		state.buf = original
		return "", err
	}
	rendered := state.buf.String()
	state.buf = original
	return "(" + rendered + ")", nil
}
