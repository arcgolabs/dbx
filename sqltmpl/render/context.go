package render

import (
	"reflect"

	collectionx "github.com/arcgolabs/collectionx/list"
	"github.com/arcgolabs/dbx/dialect"
)

type state struct {
	dialect dialect.Contract
	params  any
	args    *collectionx.List[any]
	bindN   int
	env     map[string]any
}

func newState(params any, d dialect.Contract) *state {
	return &state{dialect: d, params: params, args: collectionx.NewList[any]()}
}

func (s *state) nextBind() string {
	s.bindN++
	return s.dialect.BindVar(s.bindN)
}

func (s *state) exprEnv() map[string]any {
	if s.env == nil {
		s.env = exprEnv(s.params)
	}
	return s.env
}

func exprEnv(params any) map[string]any {
	env := envMap(params)
	env["empty"] = isEmpty
	env["blank"] = isBlank
	env["present"] = isPresent
	return env
}

func envMap(params any) map[string]any {
	if provider, ok := params.(paramEnv); ok {
		return provider.SQLTemplateParamEnv()
	}
	v, ok := indirectValue(params)
	if !ok {
		return map[string]any{}
	}
	if v.Kind() == reflect.Map {
		return mapEnv(v)
	}
	if v.Kind() == reflect.Struct {
		return structEnv(v)
	}
	return map[string]any{}
}

func mapEnv(value reflect.Value) map[string]any {
	out := make(map[string]any, value.Len())
	iter := value.MapRange()
	for iter.Next() {
		key := iter.Key()
		if key.Kind() == reflect.String {
			out[key.String()] = iter.Value().Interface()
		}
	}
	return out
}

func structEnv(value reflect.Value) map[string]any {
	meta := cachedStructMetadata(value.Type())
	out := make(map[string]any, meta.envKeyCount)
	meta.fields.Range(func(_ int, field structFieldMetadata) bool {
		assignStructField(out, field, value.Field(field.index).Interface())
		return true
	})
	return out
}

func assignStructField(out map[string]any, field structFieldMetadata, value any) {
	field.envKeys.Range(func(_ int, key string) bool {
		out[key] = value
		return true
	})
}
