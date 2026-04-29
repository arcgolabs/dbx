package relationload

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"

	collectionx "github.com/arcgolabs/collectionx/list"
	"github.com/arcgolabs/dbx"
	"github.com/arcgolabs/dbx/querydsl"
	schemax "github.com/arcgolabs/dbx/schema"
)

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

func allSelectItems(columns *collectionx.List[schemax.ColumnMeta]) *collectionx.List[querydsl.SelectItem] {
	return collectionx.MapList[schemax.ColumnMeta, querydsl.SelectItem](columns, func(_ int, column schemax.ColumnMeta) querydsl.SelectItem {
		return relationColumn(column)
	})
}

func relationTargetOrders(spec schemax.TableSpec, targetColumn schemax.ColumnMeta) *collectionx.List[querydsl.Order] {
	orders := collectionx.NewList[querydsl.Order](relationColumn(targetColumn).Asc())
	if spec.PrimaryKey != nil && spec.PrimaryKey.Columns.Len() == 1 {
		if column, ok := spec.PrimaryKey.Columns.GetFirst(); ok && column != targetColumn.Name {
			if meta, ok := columnMetaByName(spec.Columns, column); ok {
				orders.Add(relationColumn(meta).Asc())
			}
		}
	}
	return orders
}

func relationColumn(meta schemax.ColumnMeta) querydsl.Column[any] {
	return querydsl.Col[any](querydsl.NewTableRef(meta.Table, meta.Alias, nil, nil), meta.Name)
}

func columnMetaByName(columns *collectionx.List[schemax.ColumnMeta], name string) (schemax.ColumnMeta, bool) {
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

func chunkRelationKeys(keys *collectionx.List[any], chunkSize int) *collectionx.List[*collectionx.List[any]] {
	if keys.Len() == 0 {
		return collectionx.NewList[*collectionx.List[any]]()
	}
	if chunkSize <= 0 {
		chunkSize = keys.Len()
	}
	return collectionx.NewList[*collectionx.List[any]](keys.Chunk(chunkSize)...)
}

func scanRelationPairs(rows *sql.Rows, sourceType, targetType reflect.Type) (*collectionx.List[relationKeyPair], error) {
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
