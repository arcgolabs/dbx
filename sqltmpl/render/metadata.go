package render

import (
	"reflect"
	"strings"

	collectionx "github.com/arcgolabs/collectionx/list"
	mappingx "github.com/arcgolabs/collectionx/mapping"
	setx "github.com/arcgolabs/collectionx/set"
	"github.com/samber/hot"
)

var structMetadataCache = hot.NewHotCache[reflect.Type, *structMetadata](hot.LRU, 256).Build()

type structMetadata struct {
	fields      *collectionx.List[structFieldMetadata]
	lookup      *mappingx.Map[string, structFieldMetadata]
	envKeyCount int
}

type structFieldMetadata struct {
	index      int
	name       string
	foldedName string
	aliases    *collectionx.List[string]
	envKeys    *collectionx.List[string]
}

type methodMetadata struct {
	lookup *mappingx.Map[string, int]
}

func cachedStructMetadata(t reflect.Type) *structMetadata {
	if cached, ok := structMetadataCache.Peek(t); ok {
		return cached
	}

	metadata := buildStructMetadata(t)
	if cached, ok := structMetadataCache.Peek(t); ok {
		return cached
	}
	structMetadataCache.Set(t, metadata)
	return metadata
}

func buildStructMetadata(t reflect.Type) *structMetadata {
	fields := collectionx.NewListWithCapacity[structFieldMetadata](t.NumField())
	for index := range t.NumField() {
		field := t.Field(index)
		if !field.IsExported() {
			continue
		}

		aliases := fieldAliases(field)
		fields.Add(structFieldMetadata{
			index:      index,
			name:       field.Name,
			foldedName: strings.ToLower(field.Name),
			aliases:    aliases,
			envKeys:    fieldEnvKeys(field.Name, aliases),
		})
	}

	envKeyCount := 0
	lookup := mappingx.NewMapWithCapacity[string, structFieldMetadata](fields.Len() * 3)
	fields.Range(func(_ int, field structFieldMetadata) bool {
		lookup.Set(field.name, field)
		lookup.Set(field.foldedName, field)
		field.aliases.Range(func(_ int, alias string) bool {
			lookup.Set(alias, field)
			lookup.Set(strings.ToLower(alias), field)
			return true
		})
		envKeyCount += field.envKeys.Len()
		return true
	})

	return &structMetadata{
		fields:      fields,
		lookup:      lookup,
		envKeyCount: envKeyCount,
	}
}

func fieldEnvKeys(name string, aliases *collectionx.List[string]) *collectionx.List[string] {
	keys := collectionx.NewListWithCapacity[string](aliases.Len() + 2)
	seen := setx.NewSetWithCapacity[string](aliases.Len() + 2)
	addEnvKey(keys, seen, name)
	addEnvKey(keys, seen, strings.ToLower(name))
	aliases.Range(func(_ int, alias string) bool {
		addEnvKey(keys, seen, alias)
		return true
	})
	return keys
}

func addEnvKey(keys *collectionx.List[string], seen *setx.Set[string], key string) {
	if key == "" || seen.Contains(key) {
		return
	}
	seen.Add(key)
	keys.Add(key)
}

var methodMetadataCache = hot.NewHotCache[reflect.Type, *methodMetadata](hot.LRU, 256).Build()

func cachedMethodMetadata(t reflect.Type) *methodMetadata {
	if cached, ok := methodMetadataCache.Peek(t); ok {
		return cached
	}

	metadata := buildMethodMetadata(t)
	if cached, ok := methodMetadataCache.Peek(t); ok {
		return cached
	}
	methodMetadataCache.Set(t, metadata)
	return metadata
}

func buildMethodMetadata(t reflect.Type) *methodMetadata {
	lookup := mappingx.NewMapWithCapacity[string, int](t.NumMethod() * 2)
	for index := range t.NumMethod() {
		method := t.Method(index)
		if method.Type.NumIn() != 1 || method.Type.NumOut() != 1 {
			continue
		}
		lookup.Set(method.Name, index)
		lookup.Set(strings.ToLower(method.Name), index)
	}
	return &methodMetadata{lookup: lookup}
}

func indirectValue(input any) (reflect.Value, bool) {
	value := reflect.ValueOf(input)
	for value.IsValid() && value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return reflect.Value{}, false
		}
		value = value.Elem()
	}

	return value, value.IsValid()
}
