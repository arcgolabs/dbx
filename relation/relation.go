package relation

import (
	"reflect"

	schemax "github.com/arcgolabs/dbx/schema"
)

type Accessor interface {
	RelationRef() schemax.RelationMeta
}

type BelongsTo[E any, T any] struct {
	meta schemax.RelationMeta
}

type HasOne[E any, T any] struct {
	meta schemax.RelationMeta
}

type HasMany[E any, T any] struct {
	meta schemax.RelationMeta
}

type ManyToMany[E any, T any] struct {
	meta schemax.RelationMeta
}

func (r BelongsTo[E, T]) BindRelation(binding schemax.RelationBinding) any {
	r.meta = binding.Meta
	return r
}

func (r HasOne[E, T]) BindRelation(binding schemax.RelationBinding) any {
	r.meta = binding.Meta
	return r
}

func (r HasMany[E, T]) BindRelation(binding schemax.RelationBinding) any {
	r.meta = binding.Meta
	return r
}

func (r ManyToMany[E, T]) BindRelation(binding schemax.RelationBinding) any {
	r.meta = binding.Meta
	return r
}

func (BelongsTo[E, T]) RelationKind() schemax.RelationKind  { return schemax.RelationBelongsTo }
func (HasOne[E, T]) RelationKind() schemax.RelationKind     { return schemax.RelationHasOne }
func (HasMany[E, T]) RelationKind() schemax.RelationKind    { return schemax.RelationHasMany }
func (ManyToMany[E, T]) RelationKind() schemax.RelationKind { return schemax.RelationManyToMany }

func (BelongsTo[E, T]) TargetType() reflect.Type  { return reflect.TypeFor[T]() }
func (HasOne[E, T]) TargetType() reflect.Type     { return reflect.TypeFor[T]() }
func (HasMany[E, T]) TargetType() reflect.Type    { return reflect.TypeFor[T]() }
func (ManyToMany[E, T]) TargetType() reflect.Type { return reflect.TypeFor[T]() }

func (r BelongsTo[E, T]) Name() string  { return r.meta.Name }
func (r HasOne[E, T]) Name() string     { return r.meta.Name }
func (r HasMany[E, T]) Name() string    { return r.meta.Name }
func (r ManyToMany[E, T]) Name() string { return r.meta.Name }

func (r BelongsTo[E, T]) Meta() schemax.RelationMeta  { return r.meta }
func (r HasOne[E, T]) Meta() schemax.RelationMeta     { return r.meta }
func (r HasMany[E, T]) Meta() schemax.RelationMeta    { return r.meta }
func (r ManyToMany[E, T]) Meta() schemax.RelationMeta { return r.meta }

func (r BelongsTo[E, T]) RelationRef() schemax.RelationMeta  { return r.meta }
func (r HasOne[E, T]) RelationRef() schemax.RelationMeta     { return r.meta }
func (r HasMany[E, T]) RelationRef() schemax.RelationMeta    { return r.meta }
func (r ManyToMany[E, T]) RelationRef() schemax.RelationMeta { return r.meta }
