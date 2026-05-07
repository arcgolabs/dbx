package repository

import (
	"context"
	"database/sql"
	"strings"

	collectionx "github.com/arcgolabs/collectionx/list"
	"github.com/arcgolabs/dbx"
	"github.com/arcgolabs/dbx/querydsl"
	schemax "github.com/arcgolabs/dbx/schema"
	"github.com/samber/mo"
)

// KeyPart binds one typed key column to one value.
type KeyPart struct {
	column    schemax.ColumnMeta
	value     any
	predicate querydsl.Predicate
}

// Part creates one typed key part from a typed schema column.
func Part[T any](column KeyColumn[T], value T) KeyPart {
	return KeyPart{
		column:    schemax.CloneColumnMeta(column.ColumnRef()),
		value:     value,
		predicate: column.Eq(value),
	}
}

// Column returns the key part column metadata.
func (p KeyPart) Column() schemax.ColumnMeta {
	return schemax.CloneColumnMeta(p.column)
}

// Value returns the key part value.
func (p KeyPart) Value() any {
	return p.value
}

// Predicate returns the typed equality predicate for this key part.
func (p KeyPart) Predicate() querydsl.Predicate {
	return p.predicate
}

// TypedKeySet groups one or more typed key parts.
type TypedKeySet struct {
	parts *collectionx.List[KeyPart]
}

// KeySet creates a typed key set from typed key parts.
func KeySet(parts ...KeyPart) TypedKeySet {
	return TypedKeySet{parts: collectionx.NewList[KeyPart](parts...)}
}

// Len returns the number of key parts.
func (k TypedKeySet) Len() int {
	if k.parts == nil {
		return 0
	}
	return k.parts.Len()
}

// IsEmpty reports whether the key set has no parts.
func (k TypedKeySet) IsEmpty() bool {
	return k.Len() == 0
}

// Predicate returns the combined key predicate.
func (k TypedKeySet) Predicate() (querydsl.Predicate, error) {
	parts, err := k.validatedParts()
	if err != nil {
		return nil, err
	}
	predicates := collectionx.MapList[KeyPart, querydsl.Predicate](parts, func(_ int, part KeyPart) querydsl.Predicate {
		return part.predicate
	})
	return querydsl.AndList(predicates), nil
}

// Key converts the typed key set into the legacy dynamic key map.
func (k TypedKeySet) Key() (Key, error) {
	parts, err := k.validatedParts()
	if err != nil {
		return nil, err
	}
	key := make(Key, parts.Len())
	parts.Range(func(_ int, part KeyPart) bool {
		key[part.column.Name] = part.value
		return true
	})
	return key, nil
}

func (k TypedKeySet) validatedParts() (*collectionx.List[KeyPart], error) {
	if k.parts == nil || k.parts.IsEmpty() {
		return nil, &ValidationError{Message: "key set is empty"}
	}
	seen := make(map[string]struct{}, k.parts.Len())
	parts := collectionx.NewListWithCapacity[KeyPart](k.parts.Len())
	var validationErr error
	k.parts.Range(func(_ int, part KeyPart) bool {
		name := strings.TrimSpace(part.column.Name)
		if name == "" {
			validationErr = &ValidationError{Message: "key part column is empty"}
			return false
		}
		if part.predicate == nil {
			validationErr = &ValidationError{Message: "key part predicate is nil"}
			return false
		}
		if _, ok := seen[name]; ok {
			validationErr = &ValidationError{Message: "duplicate key part column: " + name}
			return false
		}
		seen[name] = struct{}{}
		part.column.Name = name
		parts.Add(part)
		return true
	})
	if validationErr != nil {
		return nil, validationErr
	}
	return parts, nil
}

// GetByKeySet returns the entity identified by the typed key set.
func (r *Base[E, S]) GetByKeySet(ctx context.Context, key TypedKeySet) (E, error) {
	var zero E
	query, err := r.selectByKeySet(key)
	if err != nil {
		return zero, err
	}
	return r.First(ctx, query)
}

// GetByKeySetOption returns the entity identified by the typed key set as an option.
func (r *Base[E, S]) GetByKeySetOption(ctx context.Context, key TypedKeySet) (mo.Option[E], error) {
	return optionFromResult(r.GetByKeySet(ctx, key))
}

// UpdateByKeySet updates rows matched by the typed key set.
func (r *Base[E, S]) UpdateByKeySet(ctx context.Context, key TypedKeySet, assignments ...querydsl.Assignment) (sql.Result, error) {
	if r == nil || r.session == nil {
		return nil, dbx.ErrNilDB
	}
	if len(assignments) == 0 {
		return nil, ErrNilMutation
	}
	predicate, err := key.Predicate()
	if err != nil {
		return nil, err
	}
	result, err := r.Update(ctx, querydsl.Update(r.schema).Set(assignments...).Where(predicate))
	if err != nil {
		return nil, err
	}
	if r.byIDNotFoundAsError && !hasAffectedRows(result) {
		return nil, ErrNotFound
	}
	return result, nil
}

// DeleteByKeySet deletes rows matched by the typed key set.
func (r *Base[E, S]) DeleteByKeySet(ctx context.Context, key TypedKeySet) (sql.Result, error) {
	if r == nil || r.session == nil {
		return nil, dbx.ErrNilDB
	}
	predicate, err := key.Predicate()
	if err != nil {
		return nil, err
	}
	result, err := r.Delete(ctx, querydsl.DeleteFrom(r.schema).Where(predicate))
	if err != nil {
		return nil, err
	}
	if r.byIDNotFoundAsError && !hasAffectedRows(result) {
		return nil, ErrNotFound
	}
	return result, nil
}

func (r *Base[E, S]) selectByKeySet(key TypedKeySet) (*querydsl.SelectQuery, error) {
	if r == nil || r.session == nil {
		return nil, dbx.ErrNilDB
	}
	predicate, err := key.Predicate()
	if err != nil {
		return nil, err
	}
	return r.defaultSelect().Where(predicate), nil
}
