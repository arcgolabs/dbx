package codec

import (
	"errors"
	"fmt"
	"maps"
	"sync"
)

var (
	ErrNil     = errors.New("dbx/codec: codec is nil")
	ErrUnknown = errors.New("dbx/codec: codec is not registered")
)

type UnknownError struct {
	Name string
}

func (e *UnknownError) Error() string {
	if e.Name != "" {
		return fmt.Sprintf("dbx/codec: codec %q is not registered", e.Name)
	}
	return "dbx/codec: codec is not registered"
}

func (e *UnknownError) Unwrap() error {
	return ErrUnknown
}

type Registry struct {
	mu     sync.RWMutex
	codecs map[string]Codec
}

var defaultRegistry = NewRegistry()

func DefaultRegistry() *Registry {
	return defaultRegistry
}

func Register(codec Codec) error {
	return defaultRegistry.Register(codec)
}

func MustRegister(codec Codec) {
	defaultRegistry.MustRegister(codec)
}

func Lookup(name string) (Codec, bool) {
	return defaultRegistry.Lookup(name)
}

func NewRegistry() *Registry {
	registry := newEmptyRegistry()
	registerBuiltins(registry)
	return registry
}

func newEmptyRegistry() *Registry {
	return &Registry{
		codecs: make(map[string]Codec, 10),
	}
}

func (r *Registry) Clone() *Registry {
	if r == nil {
		return NewRegistry()
	}
	r.mu.RLock()
	defer r.mu.RUnlock()

	cloned := newEmptyRegistry()
	maps.Copy(cloned.codecs, r.codecs)
	return cloned
}

func (r *Registry) Register(codec Codec) error {
	if IsNil(codec) {
		return ErrNil
	}

	name := NormalizeName(codec.Name())
	if name == "" {
		return errors.New("dbx/codec: codec name cannot be empty")
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.codecs[name]; ok {
		return fmt.Errorf("dbx/codec: codec %q is already registered", name)
	}
	r.codecs[name] = codec
	return nil
}

func (r *Registry) MustRegister(codec Codec) {
	if err := r.Register(codec); err != nil {
		panic(err)
	}
}

func (r *Registry) Lookup(name string) (Codec, bool) {
	if r == nil {
		return nil, false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	codec, ok := r.codecs[NormalizeName(name)]
	return codec, ok
}
