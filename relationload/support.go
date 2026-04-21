package relationload

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"

	"github.com/DaiYuANg/arcgo/collectionx"
	"github.com/arcgolabs/dbx"
	"github.com/arcgolabs/dbx/querydsl"
	schemax "github.com/arcgolabs/dbx/schema"
)

type columnSelectItem struct {
	meta schemax.ColumnMeta
}

type columnPredicate struct {
	left  schemax.ColumnMeta
	op    querydsl.ComparisonOperator
	right any
}

type columnOrder struct {
	meta       schemax.ColumnMeta
	descending bool
}

func sourceColumnFromSpec(spec schemax.TableSpec, meta schemax.RelationMeta) (schemax.ColumnMeta, error) {
	name := meta.LocalColumn
	if name == "" {
		if spec.PrimaryKey == nil || spec.PrimaryKey.Columns.Len() != 1 {
			return schemax.ColumnMeta{}, fmt.Errorf("dbx/relationload: relation %s requires local column or single-column primary key", meta.Name)
		}
		name, _ = spec.PrimaryKey.Columns.GetFirst()
	}
	return columnFromSpec(spec, name, "source", meta.Name)
}

func targetColumnFromSpec(spec schemax.TableSpec, meta schemax.RelationMeta) (schemax.ColumnMeta, error) {
	name := meta.TargetColumn
	if name == "" {
		if spec.PrimaryKey == nil || spec.PrimaryKey.Columns.Len() != 1 {
			return schemax.ColumnMeta{}, fmt.Errorf("dbx/relationload: relation %s requires target column or single-column primary key", meta.Name)
		}
		name, _ = spec.PrimaryKey.Columns.GetFirst()
	}
	return columnFromSpec(spec, name, "target", meta.Name)
}

func columnFromSpec(spec schemax.TableSpec, name, role, relationName string) (schemax.ColumnMeta, error) {
	column, ok := collectionx.FindList[schemax.ColumnMeta](spec.Columns, func(_ int, column schemax.ColumnMeta) bool {
		return column.Name == name
	})
	if !ok {
		return schemax.ColumnMeta{}, fmt.Errorf("dbx/relationload: relation %s %s column %s not found", relationName, role, name)
	}
	return column, nil
}

func allSelectItems(columns collectionx.List[schemax.ColumnMeta]) collectionx.List[querydsl.SelectItem] {
	return collectionx.MapList[schemax.ColumnMeta, querydsl.SelectItem](columns, func(_ int, column schemax.ColumnMeta) querydsl.SelectItem {
		return columnSelectItem{meta: column}
	})
}

func relationTargetOrders(spec schemax.TableSpec, targetColumn schemax.ColumnMeta) collectionx.List[querydsl.Order] {
	orders := collectionx.NewList[querydsl.Order](columnOrder{meta: targetColumn})
	if spec.PrimaryKey != nil && spec.PrimaryKey.Columns.Len() == 1 {
		if column, ok := spec.PrimaryKey.Columns.GetFirst(); ok && column != targetColumn.Name {
			if meta, ok := columnMetaByName(spec.Columns, column); ok {
				orders.Add(columnOrder{meta: meta})
			}
		}
	}
	return orders
}

func columnMetaByName(columns collectionx.List[schemax.ColumnMeta], name string) (schemax.ColumnMeta, bool) {
	return collectionx.FindList[schemax.ColumnMeta](columns, func(_ int, column schemax.ColumnMeta) bool {
		return column.Name == name
	})
}

func relationChunkSize(session dbx.Session) int {
	if session == nil || session.Dialect() == nil {
		return 256
	}
	switch strings.ToLower(strings.TrimSpace(session.Dialect().Name())) {
	case "sqlite":
		return 900
	case "postgres", "mysql":
		return 4096
	default:
		return 512
	}
}

func chunkRelationKeys(keys collectionx.List[any], chunkSize int) collectionx.List[collectionx.List[any]] {
	if keys.Len() == 0 {
		return collectionx.NewList[collectionx.List[any]]()
	}
	if chunkSize <= 0 || keys.Len() <= chunkSize {
		return collectionx.NewList[collectionx.List[any]](keys.Clone())
	}

	chunks := collectionx.NewListWithCapacity[collectionx.List[any]]((keys.Len() + chunkSize - 1) / chunkSize)
	current := collectionx.NewListWithCapacity[any](chunkSize)
	keys.Range(func(_ int, key any) bool {
		current.Add(key)
		if current.Len() < chunkSize {
			return true
		}
		chunks.Add(current)
		current = collectionx.NewListWithCapacity[any](chunkSize)
		return true
	})
	if current.Len() > 0 {
		chunks.Add(current)
	}
	return chunks
}

func scanRelationPairs(rows *sql.Rows, sourceType, targetType reflect.Type) (collectionx.List[relationKeyPair], error) {
	pairs := collectionx.NewList[relationKeyPair]()
	for rows.Next() {
		pair, ok, err := scanRelationPairRow(rows, sourceType, targetType)
		if err != nil {
			return nil, err
		}
		if ok {
			pairs.Add(pair)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, wrapRelationLoadError("iterate relation pair rows", err)
	}
	return pairs, nil
}

func scanRelationPairRow(rows *sql.Rows, sourceType, targetType reflect.Type) (relationKeyPair, bool, error) {
	sourceDest, sourceValue := relationScanDestination(sourceType)
	targetDest, targetValue := relationScanDestination(targetType)
	if err := rows.Scan(sourceDest, targetDest); err != nil {
		return relationKeyPair{}, false, wrapRelationLoadError("scan relation pair row", err)
	}
	sourceKey, targetKey, err := normalizeRelationPair(sourceValue(), targetValue())
	if err != nil {
		return relationKeyPair{}, false, err
	}
	if !sourceKey.present || !targetKey.present {
		return relationKeyPair{}, false, nil
	}
	return relationKeyPair{source: sourceKey.key, target: targetKey.key}, true, nil
}

func normalizeRelationPair(source, target any) (relationLookupValue, relationLookupValue, error) {
	sourceKey, err := normalizeRelationLookupValue(source)
	if err != nil {
		return relationLookupValue{}, relationLookupValue{}, err
	}
	targetKey, err := normalizeRelationLookupValue(target)
	if err != nil {
		return relationLookupValue{}, relationLookupValue{}, err
	}
	return sourceKey, targetKey, nil
}

func relationScanDestination(typ reflect.Type) (any, func() any) {
	baseType := typ
	for baseType != nil && baseType.Kind() == reflect.Pointer {
		baseType = baseType.Elem()
	}
	if baseType == nil {
		var value any
		return &value, func() any { return value }
	}
	holder := reflect.New(baseType)
	return holder.Interface(), func() any { return holder.Elem().Interface() }
}

func wrapRelationLoadError(op string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("dbx/relationload: %s: %w", op, err)
}

func closeRows(rows *sql.Rows) error {
	if rows == nil {
		return nil
	}
	return wrapRelationLoadError("close rows", rows.Close())
}

func (s columnSelectItem) QueryExpression() {}
func (s columnSelectItem) QuerySelectItem() {}

func (s columnSelectItem) RenderOperand(state *querydsl.State) (string, error) {
	return renderColumnOperand(state, s.meta)
}

func (s columnSelectItem) RenderSelectItem(state *querydsl.State) error {
	operand, err := s.RenderOperand(state)
	if err != nil {
		return err
	}
	state.WriteString(operand)
	return nil
}

func (columnPredicate) QueryExpression() {}
func (columnPredicate) QueryPredicate()  {}

func (p columnPredicate) RenderPredicate(state *querydsl.State) error {
	state.RenderColumn(p.left)
	operand, err := querydsl.RenderOperandValue(state, p.right)
	if err != nil {
		return wrapRelationLoadError("render relation predicate operand", err)
	}
	state.WriteRawByte(' ')
	state.WriteString(string(p.op))
	state.WriteRawByte(' ')
	state.WriteString(operand)
	return nil
}

func (columnOrder) QueryOrder() {}

func (o columnOrder) RenderOrder(state *querydsl.State) error {
	state.RenderColumn(o.meta)
	if o.descending {
		state.WriteString(" DESC")
		return nil
	}
	state.WriteString(" ASC")
	return nil
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
	return builder.String(), builder.Err("render relationload column operand")
}
