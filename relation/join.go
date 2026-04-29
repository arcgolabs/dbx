package relation

import (
	"errors"
	"fmt"

	collectionx "github.com/arcgolabs/collectionx/list"
	"github.com/arcgolabs/dbx/querydsl"
	schemax "github.com/arcgolabs/dbx/schema"
)

type JoinSource interface {
	schemax.Resource
}

type columnOperand struct {
	meta schemax.ColumnMeta
}

type columnPredicate struct {
	left  schemax.ColumnMeta
	op    querydsl.ComparisonOperator
	right any
}

func Join(q *querydsl.SelectQuery, source JoinSource, relation Accessor, target querydsl.TableSource) (*querydsl.SelectQuery, error) {
	return join(q, querydsl.InnerJoin, source, relation, target)
}

func LeftJoin(q *querydsl.SelectQuery, source JoinSource, relation Accessor, target querydsl.TableSource) (*querydsl.SelectQuery, error) {
	return join(q, querydsl.LeftJoin, source, relation, target)
}

func RightJoin(q *querydsl.SelectQuery, source JoinSource, relation Accessor, target querydsl.TableSource) (*querydsl.SelectQuery, error) {
	return join(q, querydsl.RightJoin, source, relation, target)
}

func join(q *querydsl.SelectQuery, joinType querydsl.JoinType, source JoinSource, relation Accessor, target querydsl.TableSource) (*querydsl.SelectQuery, error) {
	if q == nil {
		return nil, errors.New("dbx/relation: select query is nil")
	}
	if source == nil {
		return nil, errors.New("dbx/relation: join requires source schema")
	}
	if relation == nil {
		return nil, errors.New("dbx/relation: join requires relation")
	}
	if target == nil {
		return nil, errors.New("dbx/relation: join requires target table")
	}

	sourceTable := querydsl.TableRef(source)
	if !containsTable(q, sourceTable) {
		return nil, fmt.Errorf("dbx/relation: source table %s is not part of the query", sourceTable.Ref())
	}

	meta := relation.RelationRef()
	targetTable := querydsl.TableRef(target)
	if meta.TargetTable != "" && targetTable.Name() != meta.TargetTable {
		return nil, fmt.Errorf("dbx/relation: relation %s targets table %s, got %s", meta.Name, meta.TargetTable, targetTable.Name())
	}

	joins, err := buildJoins(joinType, source, meta, targetTable)
	if err != nil {
		return nil, err
	}
	q.Joins = mergeList(q.Joins, joins)
	return q, nil
}

func containsTable(q *querydsl.SelectQuery, table querydsl.Table) bool {
	if sameTable(q.FromItem, table) {
		return true
	}
	_, ok := collectionx.FindList[querydsl.Join](q.Joins, func(_ int, join querydsl.Join) bool {
		return sameTable(join.Table, table)
	})
	return ok
}

func sameTable(left, right querydsl.Table) bool {
	return left.Name() == right.Name() && left.Alias() == right.Alias()
}

func buildJoins(joinType querydsl.JoinType, source JoinSource, meta schemax.RelationMeta, target querydsl.Table) (*collectionx.List[querydsl.Join], error) {
	joins := collectionx.NewListWithCapacity[querydsl.Join](2)

	switch meta.Kind {
	case schemax.RelationBelongsTo, schemax.RelationHasOne, schemax.RelationHasMany:
		predicate, err := buildDirectPredicate(source, meta, target)
		if err != nil {
			return nil, err
		}
		joins.Add(querydsl.Join{Type: joinType, Table: target, Predicate: predicate})
		return joins, nil
	case schemax.RelationManyToMany:
		through, first, second, err := buildManyToManyJoins(source, meta, target)
		if err != nil {
			return nil, err
		}
		joins.Add(querydsl.Join{Type: joinType, Table: through, Predicate: first})
		joins.Add(querydsl.Join{Type: joinType, Table: target, Predicate: second})
		return joins, nil
	default:
		return nil, fmt.Errorf("dbx/relation: unsupported relation kind %d", meta.Kind)
	}
}

func buildDirectPredicate(source JoinSource, meta schemax.RelationMeta, target querydsl.Table) (querydsl.Predicate, error) {
	localColumn, err := sourceColumn(source, meta)
	if err != nil {
		return nil, err
	}
	targetColumn, err := targetColumn(target, meta)
	if err != nil {
		return nil, err
	}
	return columnPredicate{
		left:  localColumn,
		op:    querydsl.OpEq,
		right: columnOperand{meta: targetColumn},
	}, nil
}

func buildManyToManyJoins(source JoinSource, meta schemax.RelationMeta, target querydsl.Table) (querydsl.Table, querydsl.Predicate, querydsl.Predicate, error) {
	if meta.ThroughTable == "" {
		return querydsl.Table{}, nil, nil, fmt.Errorf("dbx/relation: many-to-many relation %s requires join table", meta.Name)
	}
	if meta.ThroughLocalColumn == "" || meta.ThroughTargetColumn == "" {
		return querydsl.Table{}, nil, nil, fmt.Errorf("dbx/relation: many-to-many relation %s requires join_local and join_target", meta.Name)
	}

	sourceColumn, err := sourceColumn(source, meta)
	if err != nil {
		return querydsl.Table{}, nil, nil, err
	}
	targetColumn, err := targetColumn(target, meta)
	if err != nil {
		return querydsl.Table{}, nil, nil, err
	}

	through := querydsl.NamedTable(meta.ThroughTable)
	throughSourceColumn := schemax.ColumnMeta{Name: meta.ThroughLocalColumn, Table: through.Name(), Alias: through.Alias()}
	throughTargetColumn := schemax.ColumnMeta{Name: meta.ThroughTargetColumn, Table: through.Name(), Alias: through.Alias()}

	first := columnPredicate{left: sourceColumn, op: querydsl.OpEq, right: columnOperand{meta: throughSourceColumn}}
	second := columnPredicate{left: throughTargetColumn, op: querydsl.OpEq, right: columnOperand{meta: targetColumn}}
	return through, first, second, nil
}

func sourceColumn(source JoinSource, meta schemax.RelationMeta) (schemax.ColumnMeta, error) {
	name := meta.LocalColumn
	spec := source.Spec()
	if name == "" {
		if spec.PrimaryKey == nil || spec.PrimaryKey.Columns.Len() != 1 {
			return schemax.ColumnMeta{}, fmt.Errorf("dbx/relation: relation %s requires local column or single-column primary key", meta.Name)
		}
		name, _ = spec.PrimaryKey.Columns.GetFirst()
	}

	column, ok := columnByName(spec.Columns, name)
	if !ok {
		return schemax.ColumnMeta{}, fmt.Errorf("dbx/relation: relation %s source column %s not found", meta.Name, name)
	}
	return column, nil
}

func targetColumn(target querydsl.Table, meta schemax.RelationMeta) (schemax.ColumnMeta, error) {
	if meta.TargetColumn == "" {
		return schemax.ColumnMeta{}, fmt.Errorf("dbx/relation: relation %s requires target column", meta.Name)
	}
	return schemax.ColumnMeta{
		Name:  meta.TargetColumn,
		Table: target.Name(),
		Alias: target.Alias(),
	}, nil
}

func columnByName(columns *collectionx.List[schemax.ColumnMeta], name string) (schemax.ColumnMeta, bool) {
	return collectionx.FindList[schemax.ColumnMeta](columns, func(_ int, column schemax.ColumnMeta) bool {
		return column.Name == name
	})
}

func (columnPredicate) QueryExpression() {}
func (columnPredicate) QueryPredicate()  {}
func (columnOperand) QueryExpression()   {}

func (p columnPredicate) RenderPredicate(state *querydsl.State) error {
	state.RenderColumn(p.left)
	operand, err := querydsl.RenderOperandValue(state, p.right)
	if err != nil {
		return fmt.Errorf("dbx/relation: render predicate operand: %w", err)
	}
	state.WriteRawByte(' ')
	state.WriteString(string(p.op))
	state.WriteRawByte(' ')
	state.WriteString(operand)
	return nil
}

func (o columnOperand) RenderOperand(state *querydsl.State) (string, error) {
	var builder querydsl.Buffer
	table := o.meta.Table
	if o.meta.Alias != "" {
		table = o.meta.Alias
	}
	builder.WriteString(state.Dialect().QuoteIdent(table))
	builder.WriteRawByte('.')
	builder.WriteString(state.Dialect().QuoteIdent(o.meta.Name))
	return builder.String(), builder.Err("render relation column operand")
}

func mergeList[T any](current, next *collectionx.List[T]) *collectionx.List[T] {
	if current == nil {
		return next.Clone()
	}
	current.Merge(next)
	return current
}
