package relationload

import (
	"context"
	"errors"

	collectionx "github.com/arcgolabs/collectionx/list"
	mappingx "github.com/arcgolabs/collectionx/mapping"
	"github.com/arcgolabs/dbx"
	mapperx "github.com/arcgolabs/dbx/mapper"
	relationx "github.com/arcgolabs/dbx/relation"
	"github.com/arcgolabs/dbx/relationruntime"
	schemax "github.com/arcgolabs/dbx/schema"
	"github.com/samber/mo"
)

type relationSourceState struct {
	rt     *relationruntime.Runtime
	keys   *collectionx.List[any]
	lookup *collectionx.List[relationLookupValue]
}

// SingleRelationAssigner maps a resolved single-valued relation back onto a source item.
type SingleRelationAssigner[S any, T any] func(index int, source S, value mo.Option[T]) S

// MultiRelationAssigner maps a resolved multi-valued relation back onto a source item.
type MultiRelationAssigner[S any, T any] func(index int, source S, value *collectionx.List[T]) S

func LoadBelongsTo[S any, T any](ctx context.Context, session dbx.Session, sources *collectionx.List[S], sourceSchema schemax.SchemaSource[S], sourceMapper mapperx.Mapper[S], relation relationx.BelongsTo[S, T], targetSchema schemax.SchemaSource[T], targetMapper mapperx.Mapper[T], assign SingleRelationAssigner[S, T]) error {
	dbx.LogRuntimeNode(session, "relation.load.belongs_to.start", "sources", sourceCount(sources))
	return loadSingleRelation(ctx, session, sources, sourceSchema, sourceMapper, relation.Meta(), targetSchema, targetMapper, assign)
}

func LoadHasOne[S any, T any](ctx context.Context, session dbx.Session, sources *collectionx.List[S], sourceSchema schemax.SchemaSource[S], sourceMapper mapperx.Mapper[S], relation relationx.HasOne[S, T], targetSchema schemax.SchemaSource[T], targetMapper mapperx.Mapper[T], assign SingleRelationAssigner[S, T]) error {
	dbx.LogRuntimeNode(session, "relation.load.has_one.start", "sources", sourceCount(sources))
	return loadSingleRelation(ctx, session, sources, sourceSchema, sourceMapper, relation.Meta(), targetSchema, targetMapper, assign)
}

func LoadHasMany[S any, T any](ctx context.Context, session dbx.Session, sources *collectionx.List[S], sourceSchema schemax.SchemaSource[S], sourceMapper mapperx.Mapper[S], relation relationx.HasMany[S, T], targetSchema schemax.SchemaSource[T], targetMapper mapperx.Mapper[T], assign MultiRelationAssigner[S, T]) error {
	dbx.LogRuntimeNode(session, "relation.load.has_many.start", "sources", sourceCount(sources))
	return loadMultiRelation(ctx, session, sources, sourceSchema, sourceMapper, relation.Meta(), targetSchema, targetMapper, assign)
}

func LoadManyToMany[S any, T any](ctx context.Context, session dbx.Session, sources *collectionx.List[S], sourceSchema schemax.SchemaSource[S], sourceMapper mapperx.Mapper[S], relation relationx.ManyToMany[S, T], targetSchema schemax.SchemaSource[T], targetMapper mapperx.Mapper[T], assign MultiRelationAssigner[S, T]) error {
	const logPrefix = "relation.load.many_to_many"
	dbx.LogRuntimeNode(session, logPrefix+".start", "sources", sourceCount(sources))
	if proceed, err := startRelationLoad(session, sourceCount(sources), sourceSchema, targetSchema, assign != nil, logPrefix); err != nil || !proceed {
		return err
	}
	meta := relation.Meta()
	state, err := prepareRelationSourceState(session, sources, sourceSchema, sourceMapper, meta, logPrefix)
	if err != nil {
		return err
	}
	if state.keys.Len() == 0 {
		assignEmptyRelations(sources, assign)
		logRelationLoadDone(session, logPrefix, "reason", "no_source_keys")
		return nil
	}
	grouped, targetCount, hasPairs, err := loadManyToManyGroupedTargets(ctx, session, state.rt, sourceSchema.Spec(), meta, targetSchema, targetMapper, state.keys, logPrefix)
	if err != nil {
		return err
	}
	if !hasPairs {
		assignEmptyRelations(sources, assign)
		logRelationLoadDone(session, logPrefix, "reason", "no_pairs")
		return nil
	}
	rangeSources(sources, func(index int, source S) (S, bool) {
		key, _ := state.lookup.Get(index)
		return assign(index, source, collectionx.NewList[T](grouped.Get(key.key)...)), true
	})
	logRelationLoadDone(session, logPrefix, "sources", sourceCount(sources), "targets", targetCount)
	return nil
}

func loadSingleRelation[S any, T any](ctx context.Context, session dbx.Session, sources *collectionx.List[S], sourceSchema schemax.SchemaSource[S], sourceMapper mapperx.Mapper[S], meta schemax.RelationMeta, targetSchema schemax.SchemaSource[T], targetMapper mapperx.Mapper[T], assign SingleRelationAssigner[S, T]) error {
	const logPrefix = "relation.load.single"
	if proceed, err := startRelationLoad(session, sourceCount(sources), sourceSchema, targetSchema, assign != nil, logPrefix); err != nil || !proceed {
		return err
	}
	state, err := prepareRelationSourceState(session, sources, sourceSchema, sourceMapper, meta, logPrefix)
	if err != nil {
		return err
	}
	if state.keys.Len() == 0 {
		assignMissingSingleRelations(sources, assign)
		logRelationLoadDone(session, logPrefix, "reason", "no_source_keys")
		return nil
	}
	targetsByKey, targetCount, err := loadSingleRelationTargets(ctx, session, state.rt, meta, targetSchema, targetMapper, state.keys, logPrefix)
	if err != nil {
		return err
	}
	assignLoadedSingleRelations(sources, state.lookup, targetsByKey, assign)
	logRelationLoadDone(session, logPrefix, "sources", sourceCount(sources), "targets", targetCount)
	return nil
}

func loadMultiRelation[S any, T any](ctx context.Context, session dbx.Session, sources *collectionx.List[S], sourceSchema schemax.SchemaSource[S], sourceMapper mapperx.Mapper[S], meta schemax.RelationMeta, targetSchema schemax.SchemaSource[T], targetMapper mapperx.Mapper[T], assign MultiRelationAssigner[S, T]) error {
	const logPrefix = "relation.load.multi"
	if proceed, err := startRelationLoad(session, sourceCount(sources), sourceSchema, targetSchema, assign != nil, logPrefix); err != nil || !proceed {
		return err
	}
	state, err := prepareRelationSourceState(session, sources, sourceSchema, sourceMapper, meta, logPrefix)
	if err != nil {
		return err
	}
	if state.keys.Len() == 0 {
		assignEmptyRelations(sources, assign)
		logRelationLoadDone(session, logPrefix, "reason", "no_source_keys")
		return nil
	}
	targetColumn, err := relationTargetColumnForSpec(targetSchema.Spec(), meta)
	if err != nil {
		logRelationLoadError(session, logPrefix, "resolve_target_column", err)
		return err
	}
	targets, err := queryRelationTargets(ctx, session, state.rt, targetSchema, targetMapper, targetColumn, state.keys)
	if err != nil {
		logRelationLoadError(session, logPrefix, "query_targets", err)
		return err
	}
	grouped, err := groupRelationTargets[T](targets, targetMapper, targetColumn.Name)
	if err != nil {
		logRelationLoadError(session, logPrefix, "group_targets", err)
		return err
	}
	rangeSources(sources, func(index int, source S) (S, bool) {
		key, _ := state.lookup.Get(index)
		return assign(index, source, collectionx.NewList[T](grouped.Get(key.key)...)), true
	})
	logRelationLoadDone(session, logPrefix, "sources", sourceCount(sources), "targets", targets.Len())
	return nil
}

func validateRelationLoadInputs(session dbx.Session, sourceSchema, targetSchema any) error {
	switch {
	case session == nil:
		return dbx.ErrNilDB
	case sourceSchema == nil:
		return errors.New("dbx/relationload: source schema is nil")
	case targetSchema == nil:
		return errors.New("dbx/relationload: target schema is nil")
	default:
		return nil
	}
}

func assignEmptyRelations[S any, T any](sources *collectionx.List[S], assign MultiRelationAssigner[S, T]) {
	rangeSources(sources, func(index int, source S) (S, bool) {
		return assign(index, source, collectionx.NewList[T]()), true
	})
}

func startRelationLoad(session dbx.Session, sourceCount int, sourceSchema, targetSchema any, assignProvided bool, logPrefix string) (bool, error) {
	if err := validateRelationLoadInputs(session, sourceSchema, targetSchema); err != nil {
		logRelationLoadError(session, logPrefix, "validate_inputs", err)
		return false, err
	}
	if !assignProvided {
		err := errors.New("dbx/relationload: relation loader requires assign callback")
		logRelationLoadError(session, logPrefix, "assign_callback", err)
		return false, err
	}
	if sourceCount == 0 {
		logRelationLoadDone(session, logPrefix, "reason", "empty_sources")
		return false, nil
	}
	return true, nil
}

func prepareRelationSourceState[E any](session dbx.Session, sources *collectionx.List[E], sourceSchema schemax.SchemaSource[E], sourceMapper mapperx.Mapper[E], meta schemax.RelationMeta, logPrefix string) (relationSourceState, error) {
	rt := relationruntime.For(session)
	keys, lookup, err := collectSourceRelationKeys(rt, sources, sourceMapper, sourceSchema.Spec(), meta)
	if err != nil {
		logRelationLoadError(session, logPrefix, "collect_source_keys", err)
		return relationSourceState{}, err
	}
	return relationSourceState{rt: rt, keys: keys, lookup: lookup}, nil
}

func loadManyToManyGroupedTargets[T any](ctx context.Context, session dbx.Session, rt *relationruntime.Runtime, sourceSpec schemax.TableSpec, meta schemax.RelationMeta, targetSchema schemax.SchemaSource[T], targetMapper mapperx.Mapper[T], sourceKeys *collectionx.List[any], logPrefix string) (*mappingx.MultiMap[any, T], int, bool, error) {
	targetColumn, err := relationTargetColumnForSpec(targetSchema.Spec(), meta)
	if err != nil {
		logRelationLoadError(session, logPrefix, "resolve_target_column", err)
		return nil, 0, false, err
	}
	pairs, err := queryManyToManyPairs(ctx, session, rt, meta, sourceKeys, relationKeyTypeForMeta(sourceSpec, meta.LocalColumn), targetColumn.GoType)
	if err != nil {
		logRelationLoadError(session, logPrefix, "query_pairs", err)
		return nil, 0, false, err
	}
	if pairs.Len() == 0 {
		return mappingx.NewMultiMap[any, T](), 0, false, nil
	}
	targetKeys := uniqueRelationKeysFromPairs(rt, pairs, false)
	targets, err := queryRelationTargets(ctx, session, rt, targetSchema, targetMapper, targetColumn, targetKeys)
	if err != nil {
		logRelationLoadError(session, logPrefix, "query_targets", err)
		return nil, 0, false, err
	}
	targetsByKey, err := indexRelationTargets[T](targets, targetMapper, targetColumn.Name, "", false)
	if err != nil {
		logRelationLoadError(session, logPrefix, "index_targets", err)
		return nil, 0, false, err
	}
	return groupManyToManyTargets(pairs, targetsByKey), targets.Len(), true, nil
}

func loadSingleRelationTargets[T any](ctx context.Context, session dbx.Session, rt *relationruntime.Runtime, meta schemax.RelationMeta, targetSchema schemax.SchemaSource[T], targetMapper mapperx.Mapper[T], sourceKeys *collectionx.List[any], logPrefix string) (*mappingx.Map[any, T], int, error) {
	targetColumn, err := relationTargetColumnForSpec(targetSchema.Spec(), meta)
	if err != nil {
		logRelationLoadError(session, logPrefix, "resolve_target_column", err)
		return nil, 0, err
	}
	targets, err := queryRelationTargets(ctx, session, rt, targetSchema, targetMapper, targetColumn, sourceKeys)
	if err != nil {
		logRelationLoadError(session, logPrefix, "query_targets", err)
		return nil, 0, err
	}
	targetsByKey, err := indexRelationTargets[T](targets, targetMapper, targetColumn.Name, meta.Name, meta.Kind == schemax.RelationHasOne)
	if err != nil {
		logRelationLoadError(session, logPrefix, "index_targets", err)
		return nil, 0, err
	}
	return targetsByKey, targets.Len(), nil
}

func assignMissingSingleRelations[S any, T any](sources *collectionx.List[S], assign SingleRelationAssigner[S, T]) {
	rangeSources(sources, func(index int, source S) (S, bool) {
		return assign(index, source, mo.None[T]()), true
	})
}

func assignLoadedSingleRelations[S any, T any](sources *collectionx.List[S], lookup *collectionx.List[relationLookupValue], targetsByKey *mappingx.Map[any, T], assign SingleRelationAssigner[S, T]) {
	rangeSources(sources, func(index int, source S) (S, bool) {
		key, _ := lookup.Get(index)
		target, ok := relationTargetByLookup(key, targetsByKey)
		if !ok {
			return assign(index, source, mo.None[T]()), true
		}
		return assign(index, source, mo.Some(target)), true
	})
}

func relationTargetByLookup[T any](lookup relationLookupValue, targetsByKey *mappingx.Map[any, T]) (T, bool) {
	if !lookup.present {
		var zero T
		return zero, false
	}
	target, ok := targetsByKey.Get(lookup.key)
	if !ok {
		var zero T
		return zero, false
	}
	return target, true
}

func logRelationLoadError(session dbx.Session, logPrefix, stage string, err error) {
	dbx.LogRuntimeNode(session, logPrefix+".error", "stage", stage, "error", err)
}

func logRelationLoadDone(session dbx.Session, logPrefix string, attrs ...any) {
	dbx.LogRuntimeNode(session, logPrefix+".done", attrs...)
}

func rangeSources[S any](sources *collectionx.List[S], fn func(index int, source S) (S, bool)) {
	if sources == nil {
		return
	}
	sources.Range(func(index int, source S) bool {
		updated, proceed := fn(index, source)
		sources.Set(index, updated)
		return proceed
	})
}

func sourceCount[S any](sources *collectionx.List[S]) int {
	if sources == nil {
		return 0
	}
	return sources.Len()
}
