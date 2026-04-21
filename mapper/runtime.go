package mapper

import (
	"reflect"

	codecx "github.com/arcgolabs/dbx/codec"
	"github.com/samber/hot"
)

type mapperRegistry struct {
	structMappers *hot.HotCache[reflect.Type, *mapperMetadata]
}

type mapperRuntime struct {
	registry *mapperRegistry
	codecs   *codecx.Registry
}

var defaultMapperRuntime = newMapperRuntime()

func newMapperRuntime() *mapperRuntime {
	runtime := &mapperRuntime{
		registry: newMapperRegistry(),
		codecs:   codecx.DefaultRegistry(),
	}
	return runtime
}

func newMapperRegistry() *mapperRegistry {
	return &mapperRegistry{
		structMappers: hot.NewHotCache[reflect.Type, *mapperMetadata](hot.LRU, 256).Build(),
	}
}

func getOrBuildMapperMetadata[E any](runtime *mapperRuntime) (*mapperMetadata, error) {
	entityType := reflect.TypeFor[E]()
	if cached, ok := runtime.registry.structMappers.Peek(entityType); ok {
		return cached, nil
	}

	mapper, err := buildMapperMetadata(entityType, runtime.codecs)
	if err != nil {
		return nil, err
	}
	if cached, ok := runtime.registry.structMappers.Peek(entityType); ok {
		return cached, nil
	}
	runtime.registry.structMappers.Set(entityType, mapper)
	return mapper, nil
}
