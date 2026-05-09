package repository

import (
	"time"

	collectionx "github.com/arcgolabs/collectionx/list"
	"github.com/arcgolabs/dbx"
	mapperx "github.com/arcgolabs/dbx/mapper"
	"github.com/arcgolabs/dbx/querydsl"
	"github.com/arcgolabs/pkg/option"
)

// Option configures repository construction behavior.
type Option func(*baseOptions)

type baseOptions struct {
	keyNotFoundAsError bool
	defaultSpecs       []Spec
	softDeleteAssign   func() querydsl.Assignment
	softDeleteSpec     Spec
}

func defaultOptions() baseOptions { return baseOptions{} }

// WithKeyNotFoundAsError makes key-based updates and deletes return ErrNotFound
// when no rows are affected.
func WithKeyNotFoundAsError(enabled bool) Option {
	return func(opts *baseOptions) { opts.keyNotFoundAsError = enabled }
}

// WithDefaultSpecs applies specs to repository reads unless explicitly bypassed
// through Query(repo).WithDeleted().
func WithDefaultSpecs(specs ...Spec) Option {
	return func(opts *baseOptions) {
		opts.defaultSpecs = append(opts.defaultSpecs, compactSpecs(specs...)...)
	}
}

func compactSpecs(specs ...Spec) []Spec {
	return collectionx.FilterList[Spec](collectionx.NewList[Spec](specs...), func(_ int, spec Spec) bool {
		return spec != nil
	}).Values()
}

// SoftDeleteTimeColumn is the behavior needed for nullable timestamp soft delete columns.
type SoftDeleteTimeColumn interface {
	IsNull() querydsl.Predicate
	IsNotNull() querydsl.Predicate
	Set(time.Time) querydsl.Assignment
}

// WithSoftDeleteTime filters reads to rows where column IS NULL and configures SoftDelete* helpers.
func WithSoftDeleteTime(column SoftDeleteTimeColumn, now func() time.Time) Option {
	return func(opts *baseOptions) {
		if column == nil {
			return
		}
		if now == nil {
			now = time.Now
		}
		opts.defaultSpecs = append(opts.defaultSpecs, Where(column.IsNull()))
		opts.softDeleteSpec = Where(column.IsNotNull())
		opts.softDeleteAssign = func() querydsl.Assignment {
			return column.Set(now().UTC())
		}
	}
}

// SoftDeleteFlagColumn is the behavior needed for flag-based soft delete columns.
type SoftDeleteFlagColumn[T comparable] interface {
	Eq(T) querydsl.Predicate
	Set(T) querydsl.Assignment
}

// WithSoftDeleteFlag filters reads to activeValue and configures SoftDelete* helpers to set deletedValue.
func WithSoftDeleteFlag[T comparable](column SoftDeleteFlagColumn[T], activeValue, deletedValue T) Option {
	return func(opts *baseOptions) {
		if column == nil {
			return
		}
		opts.defaultSpecs = append(opts.defaultSpecs, Where(column.Eq(activeValue)))
		opts.softDeleteSpec = Where(column.Eq(deletedValue))
		opts.softDeleteAssign = func() querydsl.Assignment {
			return column.Set(deletedValue)
		}
	}
}

// New constructs a repository with default options.
func New[E any, S EntitySchema[E]](db *dbx.DB, schema S) *Base[E, S] {
	return NewWithOptions[E](db, schema)
}

// NewWithOptions constructs a repository with explicit options.
func NewWithOptions[E any, S EntitySchema[E]](db *dbx.DB, schema S, opts ...Option) *Base[E, S] {
	config := defaultOptions()
	option.Apply(&config, opts...)
	return &Base[E, S]{
		db:                 db,
		session:            db,
		schema:             schema,
		mapper:             mapperx.MustMapper[E](schema),
		keyNotFoundAsError: config.keyNotFoundAsError,
		defaultSpecs:       append([]Spec(nil), config.defaultSpecs...),
		softDeleteAssign:   config.softDeleteAssign,
		softDeleteSpec:     config.softDeleteSpec,
	}
}
