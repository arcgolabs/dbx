package activerecord

import (
	"context"
	"fmt"

	collectionx "github.com/arcgolabs/collectionx/list"
	"github.com/arcgolabs/dbx"
	"github.com/arcgolabs/dbx/paging"
	"github.com/arcgolabs/dbx/repository"
	"github.com/samber/mo"
)

// Store provides active-record style access for an entity schema.
type Store[E any, S repository.EntitySchema[E]] struct {
	repository *repository.Base[E, S]
}

// New returns a Store backed by the provided database and schema.
func New[E any, S repository.EntitySchema[E]](db *dbx.DB, schema S) *Store[E, S] {
	return NewWithOptions[E](db, schema)
}

// NewWithOptions returns a Store backed by a repository configured with options.
func NewWithOptions[E any, S repository.EntitySchema[E]](db *dbx.DB, schema S, opts ...repository.Option) *Store[E, S] {
	return &Store[E, S]{repository: repository.NewWithOptions[E](db, schema, opts...)}
}

// Repository returns the underlying repository.
func (s *Store[E, S]) Repository() *repository.Base[E, S] {
	return s.repository
}

// Create persists one entity and returns the wrapped model.
func (s *Store[E, S]) Create(ctx context.Context, entity *E) (*Model[E, S], error) {
	model := s.Wrap(entity)
	if err := model.Save(ctx); err != nil {
		return nil, err
	}
	return model, nil
}

// CreateReturning persists one entity using RETURNING and returns the refreshed model.
func (s *Store[E, S]) CreateReturning(ctx context.Context, entity *E) (*Model[E, S], error) {
	if s == nil || s.repository == nil {
		return nil, dbx.ErrNilDB
	}
	created, err := s.repository.CreateReturning(ctx, entity)
	if err != nil {
		return nil, fmt.Errorf("create returning entity: %w", err)
	}
	*entity = created
	return s.newKeyedModel(entity, s.keyOf(entity)), nil
}

// Wrap binds an entity instance to a Model.
func (s *Store[E, S]) Wrap(entity *E) *Model[E, S] {
	return s.newModel(entity)
}

// FindByKey loads a model by its repository key.
func (s *Store[E, S]) FindByKey(ctx context.Context, key repository.Key) (*Model[E, S], error) {
	entity, err := s.repository.GetByKey(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("find entity by key: %w", err)
	}
	return s.newKeyedModel(&entity, key), nil
}

// FindByKeyOption loads a model by its repository key and returns an empty option when absent.
func (s *Store[E, S]) FindByKeyOption(ctx context.Context, key repository.Key) (mo.Option[*Model[E, S]], error) {
	entity, err := s.repository.GetByKeyOption(ctx, key)
	if err != nil {
		return mo.None[*Model[E, S]](), fmt.Errorf("find entity by key: %w", err)
	}
	return mapOption(entity, func(item E) *Model[E, S] {
		return s.newKeyedModel(new(item), key)
	}), nil
}

// List returns models matching the provided repository specifications.
func (s *Store[E, S]) List(ctx context.Context, specs ...repository.Spec) (*collectionx.List[*Model[E, S]], error) {
	items, err := s.repository.ListSpec(ctx, specs...)
	if err != nil {
		return nil, fmt.Errorf("list entities: %w", err)
	}
	return collectionx.MapList[E, *Model[E, S]](items, func(_ int, item E) *Model[E, S] {
		entity := item
		return s.newKeyedModel(&entity, s.keyOf(&entity))
	}), nil
}

// ListPage returns one page of models matching the provided repository specifications.
func (s *Store[E, S]) ListPage(ctx context.Context, request paging.Request, specs ...repository.Spec) (paging.Result[*Model[E, S]], error) {
	if s == nil || s.repository == nil {
		return paging.Result[*Model[E, S]]{}, dbx.ErrNilDB
	}
	page, err := s.repository.ListPageSpecRequest(ctx, request, specs...)
	if err != nil {
		return paging.Result[*Model[E, S]]{}, fmt.Errorf("list entity page: %w", err)
	}
	return paging.MapResult[E, *Model[E, S]](page, func(_ int, item E) *Model[E, S] {
		entity := item
		return s.newKeyedModel(&entity, s.keyOf(&entity))
	}), nil
}

// ListPageBy returns one page of models using page and page size values.
func (s *Store[E, S]) ListPageBy(ctx context.Context, page, pageSize int, specs ...repository.Spec) (paging.Result[*Model[E, S]], error) {
	return s.ListPage(ctx, paging.NewRequest(page, pageSize), specs...)
}

func (s *Store[E, S]) newModel(entity *E) *Model[E, S] {
	return &Model[E, S]{store: s, entity: entity}
}

func (s *Store[E, S]) newKeyedModel(entity *E, key repository.Key) *Model[E, S] {
	return &Model[E, S]{store: s, entity: entity, key: cloneKey(key)}
}

func (s *Store[E, S]) wrapOption(entity mo.Option[E]) mo.Option[*Model[E, S]] {
	return mapOption(entity, func(item E) *Model[E, S] {
		entity := item
		return s.newKeyedModel(&entity, s.keyOf(&entity))
	})
}

func mapOption[T any, R any](value mo.Option[T], mapper func(T) R) mo.Option[R] {
	item, ok := value.Get()
	if !ok {
		return mo.None[R]()
	}
	return mo.Some(mapper(item))
}
