package activerecord

import (
	"context"
	"errors"
	"fmt"
	"github.com/arcgolabs/dbx/querydsl"

	"github.com/DaiYuANg/arcgo/collectionx"
	"github.com/arcgolabs/dbx"
	"github.com/arcgolabs/dbx/repository"
)

// Model wraps an entity with active-record style persistence operations.
type Model[E any, S repository.EntitySchema[E]] struct {
	store  *Store[E, S]
	entity *E
	key    repository.Key
}

// Entity returns the wrapped entity instance.
func (m *Model[E, S]) Entity() *E {
	return m.entity
}

// Key returns a copy of the model key.
func (m *Model[E, S]) Key() repository.Key {
	if m.key == nil && m.entity != nil && m.store != nil && m.store.repository != nil {
		m.key = m.store.keyOf(m.entity)
	}
	return cloneKey(m.key)
}

// Save persists the wrapped entity.
func (m *Model[E, S]) Save(ctx context.Context) error {
	if err := m.validateEntity(); err != nil {
		return err
	}

	if m.shouldCreate() {
		return m.create(ctx)
	}

	return m.update(ctx)
}

// Reload refreshes the wrapped entity from the repository.
func (m *Model[E, S]) Reload(ctx context.Context) error {
	key, err := m.requireKey()
	if err != nil {
		return err
	}

	latest, err := m.store.repository.GetByKey(ctx, key)
	if err != nil {
		return fmt.Errorf("reload entity by key: %w", err)
	}

	*m.entity = latest
	m.key = m.store.keyOf(m.entity)
	return nil
}

// Delete removes the wrapped entity from the repository.
func (m *Model[E, S]) Delete(ctx context.Context) error {
	key, err := m.requireKey()
	if err != nil {
		return err
	}

	if _, err = m.store.repository.DeleteByKey(ctx, key); err != nil {
		return fmt.Errorf("delete entity by key: %w", err)
	}

	return nil
}

func (m *Model[E, S]) validateEntity() error {
	if m == nil || m.store == nil || m.store.repository == nil {
		return dbx.ErrNilDB
	}
	if m.entity == nil {
		return &repository.ValidationError{Message: "entity is nil"}
	}
	return nil
}

func (m *Model[E, S]) requireKey() (repository.Key, error) {
	if err := m.validateEntity(); err != nil {
		return nil, err
	}

	key := m.Key()
	if len(key) == 0 {
		return nil, &repository.ValidationError{Message: "entity key is empty"}
	}

	return key, nil
}

func (m *Model[E, S]) shouldCreate() bool {
	key := m.Key()
	return len(key) == 0 || hasZeroKeyValue(key)
}

func (m *Model[E, S]) create(ctx context.Context) error {
	if err := m.store.repository.Create(ctx, m.entity); err != nil {
		return fmt.Errorf("create entity: %w", err)
	}

	m.key = m.store.keyOf(m.entity)
	return nil
}

func (m *Model[E, S]) update(ctx context.Context) error {
	assignments, err := m.updateAssignments()
	if err != nil {
		return err
	}

	if assignments.IsEmpty() {
		return nil
	}

	if _, err = m.store.repository.UpdateByKey(ctx, m.Key(), assignments.Values()...); err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			return m.create(ctx)
		}
		return fmt.Errorf("update entity by key: %w", err)
	}

	return nil
}

func (m *Model[E, S]) updateAssignments() (collectionx.List[querydsl.Assignment], error) {
	assignments, err := m.store.repository.Mapper().UpdateAssignments(
		m.store.repository.Schema(),
		m.entity,
	)
	if err != nil {
		return nil, fmt.Errorf("build update assignments: %w", err)
	}
	return assignments, nil
}
