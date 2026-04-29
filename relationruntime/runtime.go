package relationruntime

import (
	"errors"
	"fmt"
	"sync"

	collectionx "github.com/arcgolabs/collectionx/set"
	"github.com/samber/hot"
)

// Runtime holds relation-load caches and pools per DB instance.
type Runtime struct {
	queryCache  *hot.HotCache[string, string]
	seenSetPool sync.Pool
}

type Provider interface {
	RelationRuntime() *Runtime
}

func New() *Runtime {
	rt := &Runtime{
		queryCache: hot.NewHotCache[string, string](hot.LRU, 64).Build(),
	}
	rt.seenSetPool = sync.Pool{New: func() any { return collectionx.NewSet[any]() }}
	return rt
}

var defaultRuntime = New()

func Default() *Runtime {
	return defaultRuntime
}

func For(session any) *Runtime {
	if p, ok := session.(Provider); ok {
		return p.RelationRuntime()
	}
	return defaultRuntime
}

func (rt *Runtime) AcquireSeenSet() (*collectionx.Set[any], error) {
	seen, ok := rt.seenSetPool.Get().(*collectionx.Set[any])
	if !ok {
		return collectionx.NewSet[any](), errors.New("dbx/relationruntime: invalid relation seen-set pool value")
	}
	return seen, nil
}

func (rt *Runtime) ReleaseSeenSet(seen *collectionx.Set[any]) {
	if rt == nil || seen == nil {
		return
	}
	seen.Clear()
	rt.seenSetPool.Put(seen)
}

func (rt *Runtime) CachedQuery(cacheKey string) (string, bool, error) {
	value, ok, err := rt.queryCache.Get(cacheKey)
	if err != nil {
		return "", false, fmt.Errorf("dbx/relationruntime: read relation query cache: %w", err)
	}
	return value, ok, nil
}

func (rt *Runtime) CacheQuery(cacheKey, query string) {
	if rt == nil || rt.queryCache == nil {
		return
	}
	rt.queryCache.Set(cacheKey, query)
}
