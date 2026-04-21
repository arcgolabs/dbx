package relationload

import (
	"context"
	"errors"

	"github.com/DaiYuANg/arcgo/collectionx"
	"github.com/arcgolabs/dbx"
	mapperx "github.com/arcgolabs/dbx/mapper"
	relationx "github.com/arcgolabs/dbx/relation"
	"github.com/arcgolabs/dbx/relationruntime"
	schemax "github.com/arcgolabs/dbx/schema"
	"github.com/samber/mo"
)

type relationSourceState struct {
	rt     *relationruntime.Runtime
	keys   collectionx.List[any]
	lookup []relationLookupValue
}

func LoadBelongsTo[S any, T any](ctx context.Context, session dbx.Session, sources []S, sourceSchema schemax.SchemaSource[S], sourceMapper mapperx.Mapper[S], relation relationx.BelongsTo[S, T], targetSchema schemax.SchemaSource[T], targetMapper mapperx.Mapper[T], assign func(int, *S, mo.Option[T])) error {
	dbx.LogRuntimeNode(session, "relation.load.belongs_to.start", "sources", len(sources))
	return loadSingleRelation(ctx, session, sources, sourceSchema, sourceMapper, relation.Meta(), targetSchema, targetMapper, assign)
}

func LoadHasOne[S any, T any](ctx context.Context, session dbx.Session, sources []S, sourceSchema schemax.SchemaSource[S], sourceMapper mapperx.Mapper[S], relation relationx.HasOne[S, T], targetSchema schemax.SchemaSource[T], targetMapper mapperx.Mapper[T], assign func(int, *S, mo.Option[T])) error {
	dbx.LogRuntimeNode(session, "relation.load.has_one.start", "sources", len(sources))
	return loadSingleRelation(ctx, session, sources, sourceSchema, sourceMapper, relation.Meta(), targetSchema, targetMapper, assign)
}

func LoadHasMany[S any, T any](ctx context.Context, session dbx.Session, sources []S, sourceSchema schemax.SchemaSource[S], sourceMapper mapperx.Mapper[S], relation relationx.HasMany[S, T], targetSchema schemax.SchemaSource[T], targetMapper mapperx.Mapper[T], assign func(int, *S, []T)) error {
	dbx.LogRuntimeNode(session, "relation.load.has_many.start", "sources", len(sources))
	return loadMultiRelation(ctx, session, sources, sourceSchema, sourceMapper, relation.Meta(), targetSchema, targetMapper, assign)
}

func LoadManyToMany[S any, T any](ctx context.Context, session dbx.Session, sources []S, sourceSchema schemax.SchemaSource[S], sourceMapper mapperx.Mapper[S], relation relationx.ManyToMany[S, T], targetSchema schemax.SchemaSource[T], targetMapper mapperx.Mapper[T], assign func(int, *S, []T)) error {
	const logPrefix = "relation.load.many_to_many"
	dbx.LogRuntimeNode(session, logPrefix+".start", "sources", len(sources))
	if proceed, err := startRelationLoad(session, len(sources), sourceSchema, targetSchema, assign != nil, logPrefix); err != nil || !proceed {
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
	for index := range sources {
		key := state.lookup[index]
		assign(index, &sources[index], grouped.Get(key.key))
	}
	logRelationLoadDone(session, logPrefix, "sources", len(sources), "targets", targetCount)
	return nil
}

func loadSingleRelation[S any, T any](ctx context.Context, session dbx.Session, sources []S, sourceSchema schemax.SchemaSource[S], sourceMapper mapperx.Mapper[S], meta schemax.RelationMeta, targetSchema schemax.SchemaSource[T], targetMapper mapperx.Mapper[T], assign func(int, *S, mo.Option[T])) error {
	const logPrefix = "relation.load.single"
	if proceed, err := startRelationLoad(session, len(sources), sourceSchema, targetSchema, assign != nil, logPrefix); err != nil || !proceed {
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
	logRelationLoadDone(session, logPrefix, "sources", len(sources), "targets", targetCount)
	return nil
}

func loadMultiRelation[S any, T any](ctx context.Context, session dbx.Session, sources []S, sourceSchema schemax.SchemaSource[S], sourceMapper mapperx.Mapper[S], meta schemax.RelationMeta, targetSchema schemax.SchemaSource[T], targetMapper mapperx.Mapper[T], assign func(int, *S, []T)) error {
	const logPrefix = "relation.load.multi"
	if proceed, err := startRelationLoad(session, len(sources), sourceSchema, targetSchema, assign != nil, logPrefix); err != nil || !proceed {
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
	for index := range sources {
		key := state.lookup[index]
		assign(index, &sources[index], grouped.Get(key.key))
	}
	logRelationLoadDone(session, logPrefix, "sources", len(sources), "targets", targets.Len())
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

func assignEmptyRelations[S any, T any](sources []S, assign func(int, *S, []T)) {
	for index := range sources {
		assign(index, &sources[index], nil)
	}
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

func prepareRelationSourceState[E any](session dbx.Session, sources []E, sourceSchema schemax.SchemaSource[E], sourceMapper mapperx.Mapper[E], meta schemax.RelationMeta, logPrefix string) (relationSourceState, error) {
	rt := relationruntime.For(session)
	keys, lookup, err := collectSourceRelationKeys(rt, sources, sourceMapper, sourceSchema.Spec(), meta)
	if err != nil {
		logRelationLoadError(session, logPrefix, "collect_source_keys", err)
		return relationSourceState{}, err
	}
	return relationSourceState{rt: rt, keys: keys, lookup: lookup}, nil
}

func loadManyToManyGroupedTargets[T any](ctx context.Context, session dbx.Session, rt *relationruntime.Runtime, sourceSpec schemax.TableSpec, meta schemax.RelationMeta, targetSchema schemax.SchemaSource[T], targetMapper mapperx.Mapper[T], sourceKeys collectionx.List[any], logPrefix string) (collectionx.MultiMap[any, T], int, bool, error) {
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
		return collectionx.NewMultiMap[any, T](), 0, false, nil
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

func loadSingleRelationTargets[T any](ctx context.Context, session dbx.Session, rt *relationruntime.Runtime, meta schemax.RelationMeta, targetSchema schemax.SchemaSource[T], targetMapper mapperx.Mapper[T], sourceKeys collectionx.List[any], logPrefix string) (map[any]T, int, error) {
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

func assignMissingSingleRelations[S any, T any](sources []S, assign func(int, *S, mo.Option[T])) {
	for index := range sources {
		assign(index, &sources[index], mo.None[T]())
	}
}

func assignLoadedSingleRelations[S any, T any](sources []S, lookup []relationLookupValue, targetsByKey map[any]T, assign func(int, *S, mo.Option[T])) {
	for index := range sources {
		target, ok := relationTargetByLookup(lookup[index], targetsByKey)
		if !ok {
			assign(index, &sources[index], mo.None[T]())
			continue
		}
		assign(index, &sources[index], mo.Some(target))
	}
}

func relationTargetByLookup[T any](lookup relationLookupValue, targetsByKey map[any]T) (T, bool) {
	if !lookup.present {
		var zero T
		return zero, false
	}
	target, ok := targetsByKey[lookup.key]
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
