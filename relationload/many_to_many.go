package relationload

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	collectionx "github.com/arcgolabs/collectionx/list"
	mappingx "github.com/arcgolabs/collectionx/mapping"
	"github.com/arcgolabs/dbx"
	mapperx "github.com/arcgolabs/dbx/mapper"
	"github.com/arcgolabs/dbx/querydsl"
	"github.com/arcgolabs/dbx/relationruntime"
	schemax "github.com/arcgolabs/dbx/schema"
	"github.com/arcgolabs/dbx/sqlstmt"
)

func queryManyToManyPairs(ctx context.Context, session dbx.Session, rt *relationruntime.Runtime, meta schemax.RelationMeta, sourceKeys *collectionx.List[any], sourceType, targetType reflect.Type) (*collectionx.List[relationKeyPair], error) {
	if meta.ThroughTable == "" {
		return nil, fmt.Errorf("dbx/relationload: many-to-many relation %s requires join table", meta.Name)
	}
	if meta.ThroughLocalColumn == "" || meta.ThroughTargetColumn == "" {
		return nil, fmt.Errorf("dbx/relationload: many-to-many relation %s requires join_local and join_target", meta.Name)
	}

	pairs := collectionx.NewListWithCapacity[relationKeyPair](sourceKeys.Len())
	chunks := chunkRelationKeys(sourceKeys, relationChunkSize(session))
	dbx.LogRuntimeNode(session, "relation.m2m.pairs.start", "relation", meta.Name, "keys", sourceKeys.Len(), "chunks", chunks.Len())
	var resultErr error
	chunks.Range(func(index int, chunk *collectionx.List[any]) bool {
		dbx.LogRuntimeNode(session, "relation.m2m.pairs.chunk", "relation", meta.Name, "index", index, "size", chunk.Len())
		bound, err := buildManyToManyPairsBoundQuery(session, rt, meta, chunk)
		if err != nil {
			dbx.LogRuntimeNode(session, "relation.m2m.pairs.error", "stage", "build_bound", "relation", meta.Name, "error", err)
			resultErr = err
			return false
		}
		scanned, err := queryManyToManyPairChunk(ctx, session, bound, sourceType, targetType)
		if err != nil {
			dbx.LogRuntimeNode(session, "relation.m2m.pairs.error", "stage", "query_rows", "relation", meta.Name, "index", index, "error", err)
			resultErr = err
			return false
		}
		pairs.Merge(scanned)
		return true
	})
	if resultErr != nil {
		return nil, resultErr
	}
	dbx.LogRuntimeNode(session, "relation.m2m.pairs.done", "relation", meta.Name, "pairs", pairs.Len())
	return pairs, nil
}

func queryManyToManyPairChunk(ctx context.Context, session dbx.Session, bound sqlstmt.Bound, sourceType, targetType reflect.Type) (_ *collectionx.List[relationKeyPair], err error) {
	rows, err := session.QueryBoundContext(ctx, bound)
	if err != nil {
		return nil, wrapRelationLoadError("query many-to-many rows", err)
	}
	defer func() {
		err = errors.Join(err, closeRows(rows))
	}()

	return scanRelationPairs(rows, sourceType, targetType)
}

func buildManyToManyPairsBoundQuery(session dbx.Session, rt *relationruntime.Runtime, meta schemax.RelationMeta, sourceKeys *collectionx.List[any]) (sqlstmt.Bound, error) {
	dialectName := session.Dialect().Name()
	cacheKey := fmt.Sprintf("m2m:%s:%s:%s:%s:%d", dialectName, meta.ThroughTable, meta.ThroughLocalColumn, meta.ThroughTargetColumn, sourceKeys.Len())
	cachedSQL, ok, err := rt.CachedQuery(cacheKey)
	if err != nil {
		return sqlstmt.Bound{}, wrapRelationLoadError("read many-to-many query cache", err)
	}
	if ok {
		dbx.LogRuntimeNode(session, "relation.m2m.bound.cache_hit", "relation", meta.Name, "through", meta.ThroughTable, "keys", sourceKeys.Len())
		return sqlstmt.Bound{SQL: cachedSQL, Args: sourceKeys.Clone()}, nil
	}
	dbx.LogRuntimeNode(session, "relation.m2m.bound.cache_miss", "relation", meta.Name, "through", meta.ThroughTable, "keys", sourceKeys.Len())

	through := querydsl.NamedTable(meta.ThroughTable)
	localColumn := querydsl.Col[any](through, meta.ThroughLocalColumn)
	targetColumn := querydsl.Col[any](through, meta.ThroughTargetColumn)
	query := querydsl.Select(
		localColumn,
		targetColumn,
	).From(through).Where(localColumn.In(sourceKeys.Values()...)).OrderBy(
		localColumn.Asc(),
		targetColumn.Asc(),
	)

	bound, err := dbx.Build(session, query)
	if err != nil {
		dbx.LogRuntimeNode(session, "relation.m2m.bound.error", "relation", meta.Name, "error", err)
		return sqlstmt.Bound{}, wrapRelationLoadError("build many-to-many pair query", err)
	}
	rt.CacheQuery(cacheKey, bound.SQL)
	return bound, nil
}

func uniqueRelationKeysFromPairs(rt *relationruntime.Runtime, pairs *collectionx.List[relationKeyPair], useSource bool) *collectionx.List[any] {
	keys := collectionx.NewListWithCapacity[any](pairs.Len())
	seen, err := rt.AcquireSeenSet()
	if err != nil {
		return nil
	}
	defer rt.ReleaseSeenSet(seen)

	pairs.Range(func(_ int, pair relationKeyPair) bool {
		key := pair.target
		if useSource {
			key = pair.source
		}
		if seen.Contains(key) {
			return true
		}
		seen.Add(key)
		keys.Add(key)
		return true
	})
	return keys
}

func groupManyToManyTargets[E any](pairs *collectionx.List[relationKeyPair], indexed *mappingx.Map[any, E]) *mappingx.MultiMap[any, E] {
	grouped := mappingx.NewMultiMapWithCapacity[any, E](pairs.Len())
	pairs.Range(func(_ int, pair relationKeyPair) bool {
		target, ok := indexed.Get(pair.target)
		if !ok {
			return true
		}
		grouped.Put(pair.source, target)
		return true
	})
	return grouped
}

type presentRelationKey struct {
	value any
	ok    bool
}

func presentEntityRelationKey[E any](mapper mapperx.Mapper[E], entity *E, column string) (presentRelationKey, error) {
	key, err := entityRelationKey(mapper, entity, column)
	if err != nil {
		return presentRelationKey{}, err
	}
	if !key.present {
		return presentRelationKey{}, nil
	}
	return presentRelationKey{value: key.key, ok: true}, nil
}
