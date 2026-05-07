package activerecord

import (
	"context"
	"fmt"

	"github.com/arcgolabs/dbx/repository"
	"github.com/samber/mo"
)

// FindByKeySet loads a model by a typed key set.
func (s *Store[E, S]) FindByKeySet(ctx context.Context, key repository.TypedKeySet) (*Model[E, S], error) {
	entity, err := s.repository.GetByKeySet(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("find entity by typed key set: %w", err)
	}
	legacyKey, err := key.Key()
	if err != nil {
		return nil, fmt.Errorf("build model key from typed key set: %w", err)
	}
	return s.newKeyedModel(&entity, legacyKey), nil
}

// FindByKeySetOption loads a model by a typed key set and returns an empty option when absent.
func (s *Store[E, S]) FindByKeySetOption(ctx context.Context, key repository.TypedKeySet) (mo.Option[*Model[E, S]], error) {
	legacyKey, err := key.Key()
	if err != nil {
		return mo.None[*Model[E, S]](), fmt.Errorf("build model key from typed key set: %w", err)
	}
	entity, err := s.repository.GetByKeySetOption(ctx, key)
	if err != nil {
		return mo.None[*Model[E, S]](), fmt.Errorf("find entity by typed key set: %w", err)
	}
	return mapOption(entity, func(item E) *Model[E, S] {
		entity := item
		return s.newKeyedModel(&entity, legacyKey)
	}), nil
}
