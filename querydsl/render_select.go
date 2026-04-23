package querydsl

import (
	"errors"
	"fmt"
	"strings"

	"github.com/arcgolabs/collectionx"
	"github.com/arcgolabs/dbx/dialect"
	"github.com/arcgolabs/dbx/sqlstmt"
)

func (q *SelectQuery) Build(d dialect.Dialect) (sqlstmt.Bound, error) {
	if q == nil {
		return sqlstmt.Bound{}, errors.New("dbx/querydsl: select query is nil")
	}
	if q.FromItem.Name() == "" {
		return sqlstmt.Bound{}, errors.New("dbx/querydsl: select query requires FROM")
	}
	if q.Items.Len() == 0 {
		return sqlstmt.Bound{}, errors.New("dbx/querydsl: select query requires at least one item")
	}

	state := NewState(d, 8)
	if err := renderSelectStatement(state, q); err != nil {
		return sqlstmt.Bound{}, err
	}
	if err := state.Err(); err != nil {
		return sqlstmt.Bound{}, err
	}
	bound := state.Bound()
	if q.LimitN != nil && *q.LimitN > 0 {
		bound.CapacityHint = *q.LimitN
	}
	return bound, nil
}

func renderSelectStatement(state *State, q *SelectQuery) error {
	if err := renderCTEs(state, q.CTEs); err != nil {
		return err
	}
	return renderSelectSet(state, q)
}

func renderSelectSet(state *State, q *SelectQuery) error {
	if q.Unions.Len() == 0 {
		return renderSelectQuery(state, q)
	}

	if err := renderSelectQueryWithoutTail(state, q); err != nil {
		return err
	}
	var renderErr error
	q.Unions.Range(func(_ int, union UnionClause) bool {
		if union.Query == nil {
			renderErr = errors.New("dbx/querydsl: union query is nil")
			return false
		}
		if union.All {
			state.WriteString(" UNION ALL ")
		} else {
			state.WriteString(" UNION ")
		}
		if err := renderUnionQuery(state, union.Query); err != nil {
			renderErr = err
			return false
		}
		return true
	})
	if renderErr != nil {
		return renderErr
	}
	return renderSelectTail(state, q)
}

func renderCTEs(state *State, ctes collectionx.List[CTE]) error {
	if ctes.Len() == 0 {
		return nil
	}
	state.WriteString("WITH ")
	var renderErr error
	ctes.Range(func(index int, cte CTE) bool {
		if strings.TrimSpace(cte.Name) == "" {
			renderErr = errors.New("dbx/querydsl: cte name cannot be empty")
			return false
		}
		if cte.Query == nil {
			renderErr = fmt.Errorf("dbx/querydsl: cte %s requires query", cte.Name)
			return false
		}
		if index > 0 {
			state.WriteString(", ")
		}
		state.WriteQuotedIdent(strings.TrimSpace(cte.Name))
		state.WriteString(" AS (")
		if err := renderSelectStatement(state, cte.Query); err != nil {
			renderErr = err
			return false
		}
		state.WriteRawByte(')')
		return true
	})
	if renderErr != nil {
		return renderErr
	}
	state.WriteRawByte(' ')
	return nil
}

func renderUnionQuery(state *State, q *SelectQuery) error {
	if q.CTEs.Len() > 0 || q.Unions.Len() > 0 || q.Orders.Len() > 0 || q.LimitN != nil || q.OffsetN != nil {
		state.WriteRawByte('(')
		if err := renderSelectStatement(state, q); err != nil {
			return err
		}
		state.WriteRawByte(')')
		return nil
	}
	return renderSelectQueryWithoutTail(state, q)
}

func renderSelectQuery(state *State, q *SelectQuery) error {
	if err := renderSelectQueryWithoutTail(state, q); err != nil {
		return err
	}
	return renderSelectTail(state, q)
}

func renderSelectQueryWithoutTail(state *State, q *SelectQuery) error {
	if err := renderSelectDistinct(state, q); err != nil {
		return err
	}
	if err := renderSelectItems(state, q); err != nil {
		return err
	}
	if err := renderSelectFrom(state, q); err != nil {
		return err
	}
	if err := renderSelectJoins(state, q); err != nil {
		return err
	}
	if err := renderSelectWhere(state, q); err != nil {
		return err
	}
	if err := renderSelectGroupBy(state, q); err != nil {
		return err
	}
	return renderSelectHaving(state, q)
}

func renderSelectDistinct(state *State, q *SelectQuery) error {
	state.WriteString("SELECT ")
	if q.Distinct {
		state.WriteString("DISTINCT ")
	}
	return nil
}

func renderSelectItems(state *State, q *SelectQuery) error {
	var renderErr error
	q.Items.Range(func(index int, item SelectItem) bool {
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

func renderSelectFrom(state *State, q *SelectQuery) error {
	state.WriteString(" FROM ")
	state.RenderTable(q.FromItem)
	return nil
}

func renderSelectJoins(state *State, q *SelectQuery) error {
	var renderErr error
	q.Joins.Range(func(_ int, join Join) bool {
		state.WriteRawByte(' ')
		state.WriteString(string(join.Type))
		state.WriteString(" JOIN ")
		state.RenderTable(join.Table)
		if join.Predicate == nil {
			return true
		}
		state.WriteString(" ON ")
		if err := RenderPredicate(state, join.Predicate); err != nil {
			renderErr = err
			return false
		}
		return true
	})
	return renderErr
}

func renderSelectWhere(state *State, q *SelectQuery) error {
	if q.WhereExp == nil {
		return nil
	}
	state.WriteString(" WHERE ")
	return RenderPredicate(state, q.WhereExp)
}

func renderSelectGroupBy(state *State, q *SelectQuery) error {
	if q.Groups.Len() == 0 {
		return nil
	}
	state.WriteString(" GROUP BY ")
	var renderErr error
	q.Groups.Range(func(index int, group Expression) bool {
		if index > 0 {
			state.WriteString(", ")
		}
		operand, err := RenderOperandValue(state, group)
		if err != nil {
			renderErr = err
			return false
		}
		state.WriteString(operand)
		return true
	})
	return renderErr
}

func renderSelectHaving(state *State, q *SelectQuery) error {
	if q.HavingExp == nil {
		return nil
	}
	state.WriteString(" HAVING ")
	return RenderPredicate(state, q.HavingExp)
}

func renderSelectTail(state *State, q *SelectQuery) error {
	if err := renderSelectOrders(state, q.Orders); err != nil {
		return err
	}
	return renderSelectLimitOffset(state, q)
}

func renderSelectOrders(state *State, orders collectionx.List[Order]) error {
	if orders.Len() == 0 {
		return nil
	}
	state.WriteString(" ORDER BY ")
	var renderErr error
	orders.Range(func(index int, order Order) bool {
		if index > 0 {
			state.WriteString(", ")
		}
		if err := RenderOrder(state, order); err != nil {
			renderErr = err
			return false
		}
		return true
	})
	return renderErr
}

func renderSelectLimitOffset(state *State, q *SelectQuery) error {
	if q.LimitN == nil && q.OffsetN == nil {
		return nil
	}
	tail, err := state.Dialect().RenderLimitOffset(q.LimitN, q.OffsetN)
	if err != nil {
		return fmt.Errorf("dbx/querydsl: render limit offset: %w", err)
	}
	if strings.TrimSpace(tail) == "" {
		return nil
	}
	state.WriteRawByte(' ')
	state.WriteString(tail)
	return nil
}
