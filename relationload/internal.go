package relationload

import (
	"context"
	"fmt"
	"reflect"

	"github.com/DaiYuANg/arcgo/collectionx"
	"github.com/arcgolabs/dbx"
	mapperx "github.com/arcgolabs/dbx/mapper"
	"github.com/arcgolabs/dbx/querydsl"
	"github.com/arcgolabs/dbx/relationruntime"
	schemax "github.com/arcgolabs/dbx/schema"
	"github.com/arcgolabs/dbx/sqlstmt"
)

type relationLookupValue struct {
	present bool
	key     any
}

type relationKeyPair struct {
	source any
	target any
}

func collectSourceRelationKeys[E any](rt *relationruntime.Runtime, entities []E, mapper mapperx.Mapper[E], spec schemax.TableSpec, meta schemax.RelationMeta) (collectionx.List[any], []relationLookupValue, error) {
	localColumn, err := sourceColumnFromSpec(spec, meta)
	if err != nil {
		return nil, nil, err
	}

	lookup := collectionx.NewListWithCapacity[relationLookupValue](len(entities))
	keys := collectionx.NewListWithCapacity[any](len(entities))
	seen, err := rt.AcquireSeenSet()
	if err != nil {
		return nil, nil, wrapRelationLoadError("acquire relation seen set", err)
	}
	defer rt.ReleaseSeenSet(seen)

	for index := range entities {
		key, err := entityRelationKey(mapper, &entities[index], localColumn.Name)
		if err != nil {
			return nil, nil, err
		}
		lookup.AddAt(index, key)
		if !key.present {
			continue
		}
		if _, ok := seen.Get(key.key); ok {
			continue
		}
		seen.Set(key.key, struct{}{})
		keys.Add(key.key)
	}
	return keys, lookup.Values(), nil
}

func entityRelationKey[E any](mapper mapperx.Mapper[E], entity *E, column string) (relationLookupValue, error) {
	value, ok, err := mapper.BoundFieldValue(entity, column)
	if err != nil {
		return relationLookupValue{}, wrapRelationLoadError("read relation key", err)
	}
	if !ok {
		return relationLookupValue{}, &mapperx.UnmappedColumnError{Column: column}
	}
	return normalizeRelationLookupValue(value)
}

func normalizeRelationLookupValue(value any) (relationLookupValue, error) {
	if value == nil {
		return relationLookupValue{}, nil
	}

	current := reflect.ValueOf(value)
	for current.IsValid() && current.Kind() == reflect.Pointer {
		if current.IsNil() {
			return relationLookupValue{}, nil
		}
		current = current.Elem()
	}
	if !current.IsValid() {
		return relationLookupValue{}, nil
	}
	if !current.Type().Comparable() {
		return relationLookupValue{}, fmt.Errorf("dbx/relationload: relation key type %s is not comparable", current.Type())
	}
	return relationLookupValue{present: true, key: current.Interface()}, nil
}

func relationTargetColumnForSpec(spec schemax.TableSpec, meta schemax.RelationMeta) (schemax.ColumnMeta, error) {
	return targetColumnFromSpec(spec, meta)
}

func queryRelationTargets[E any](ctx context.Context, session dbx.Session, rt *relationruntime.Runtime, schema schemax.SchemaSource[E], mapper mapperx.Mapper[E], targetColumn schemax.ColumnMeta, keys collectionx.List[any]) (collectionx.List[E], error) {
	if keys.Len() == 0 {
		return collectionx.NewList[E](), nil
	}
	chunks := chunkRelationKeys(keys, relationChunkSize(session))
	dbx.LogRuntimeNode(session,
		"relation.targets.query.start",
		"table", schema.TableName(),
		"target_column", targetColumn.Name,
		"keys", keys.Len(),
		"chunks", chunks.Len(),
	)
	items := collectionx.NewListWithCapacity[E](keys.Len())
	var resultErr error
	chunks.Range(func(index int, chunk collectionx.List[any]) bool {
		dbx.LogRuntimeNode(session, "relation.targets.query.chunk", "index", index, "size", chunk.Len())
		bound, err := buildRelationTargetsBoundQuery[E](session, rt, schema, targetColumn, chunk)
		if err != nil {
			dbx.LogRuntimeNode(session, "relation.targets.query.error", "stage", "build_bound", "error", err)
			resultErr = err
			return false
		}
		rows, err := dbx.QueryAllBoundList[E](ctx, session, bound, mapper)
		if err != nil {
			dbx.LogRuntimeNode(session, "relation.targets.query.error", "stage", "query_rows", "index", index, "error", err)
			resultErr = err
			return false
		}
		items.Merge(rows)
		return true
	})
	if resultErr != nil {
		return nil, resultErr
	}
	dbx.LogRuntimeNode(session, "relation.targets.query.done", "table", schema.TableName(), "items", items.Len())
	return items, nil
}

func buildRelationTargetsBoundQuery[E any](session dbx.Session, rt *relationruntime.Runtime, schema schemax.SchemaSource[E], targetColumn schemax.ColumnMeta, keys collectionx.List[any]) (sqlstmt.Bound, error) {
	spec := schema.Spec()
	dialectName := session.Dialect().Name()
	tableName := schema.TableName()
	selectSigParts := collectionx.NewListWithCapacity[string](spec.Columns.Len())
	spec.Columns.Range(func(_ int, column schemax.ColumnMeta) bool {
		selectSigParts.Add(column.Name)
		return true
	})
	selectSig := selectSigParts.Join(",")
	cacheKey := fmt.Sprintf("rel:%s:%s:%s:%s:%d", dialectName, tableName, selectSig, targetColumn.Name, keys.Len())
	cachedSQL, ok, err := rt.CachedQuery(cacheKey)
	if err != nil {
		return sqlstmt.Bound{}, wrapRelationLoadError("read relation target query cache", err)
	}
	if ok {
		dbx.LogRuntimeNode(session, "relation.targets.bound.cache_hit", "table", tableName, "target_column", targetColumn.Name, "keys", keys.Len())
		return sqlstmt.Bound{SQL: cachedSQL, Args: keys.Clone()}, nil
	}
	dbx.LogRuntimeNode(session, "relation.targets.bound.cache_miss", "table", tableName, "target_column", targetColumn.Name, "keys", keys.Len())
	query := querydsl.SelectList(allSelectItems(spec.Columns)).
		From(schema).
		Where(columnPredicate{
			left:  targetColumn,
			op:    querydsl.OpIn,
			right: keys.Values(),
		}).
		OrderByList(relationTargetOrders(spec, targetColumn))
	bound, err := dbx.Build(session, query)
	if err != nil {
		dbx.LogRuntimeNode(session, "relation.targets.bound.error", "table", tableName, "error", err)
		return sqlstmt.Bound{}, wrapRelationLoadError("build relation target query", err)
	}
	rt.CacheQuery(cacheKey, bound.SQL)
	return bound, nil
}

func indexRelationTargets[E any](targets collectionx.List[E], mapper mapperx.Mapper[E], column, relationName string, enforceUnique bool) (map[any]E, error) {
	indexed := make(map[any]E, targets.Len())
	counts := make(map[any]int, targets.Len())
	var resultErr error
	targets.Range(func(_ int, target E) bool {
		key, err := presentEntityRelationKey(mapper, &target, column)
		if err != nil {
			resultErr = err
			return false
		}
		if !key.ok {
			return true
		}
		counts[key.value]++
		if enforceUnique && counts[key.value] > 1 {
			resultErr = &dbx.RelationCardinalityError{Relation: relationName, Key: key.value, Count: counts[key.value]}
			return false
		}
		indexed[key.value] = target
		return true
	})
	if resultErr != nil {
		return nil, resultErr
	}
	return indexed, nil
}

func groupRelationTargets[E any](targets collectionx.List[E], mapper mapperx.Mapper[E], column string) (collectionx.MultiMap[any, E], error) {
	grouped := collectionx.NewMultiMapWithCapacity[any, E](targets.Len())
	var resultErr error
	targets.Range(func(_ int, target E) bool {
		key, err := presentEntityRelationKey(mapper, &target, column)
		if err != nil {
			resultErr = err
			return false
		}
		if !key.ok {
			return true
		}
		grouped.Put(key.value, target)
		return true
	})
	if resultErr != nil {
		return nil, resultErr
	}
	return grouped, nil
}

func relationKeyTypeForMeta(spec schemax.TableSpec, column string) reflect.Type {
	if column == "" {
		if spec.PrimaryKey == nil || spec.PrimaryKey.Columns.Len() != 1 {
			return nil
		}
		column, _ = spec.PrimaryKey.Columns.GetFirst()
	}
	columnMeta, ok := columnMetaByName(spec.Columns, column)
	if !ok {
		return nil
	}
	return columnMeta.GoType
}
