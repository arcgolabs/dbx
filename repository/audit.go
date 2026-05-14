package repository

import (
	"context"
	"database/sql"
	"fmt"

	collectionx "github.com/arcgolabs/collectionx/list"
	"github.com/arcgolabs/dbx"
	"github.com/samber/mo"
)

// AuditOperation identifies the repository mutation kind passed to an audit writer.
type AuditOperation = string

const (
	AuditOperationInsert AuditOperation = "insert"
	AuditOperationUpdate AuditOperation = "update"
	AuditOperationUpsert AuditOperation = "upsert"
	AuditOperationDelete AuditOperation = "delete"
)

// AuditWriter is the repository-side hook for audit integrations.
//
// The concrete audit package lives in a separate Go module, so repository keeps
// this contract deliberately small and dependency-free.
type AuditWriter interface {
	WriteAudit(ctx context.Context, session dbx.Session, operation AuditOperation, entity any) error
}

// WithAuditWriter enables automatic audit writes for repository mutation helpers
// that have a concrete entity snapshot.
func WithAuditWriter(writer AuditWriter) Option {
	return func(opts *baseOptions) { opts.auditWriter = writer }
}

func (r *Base[E, S]) writeAudit(ctx context.Context, operation AuditOperation, entity *E) error {
	if r == nil || r.auditWriter == nil || entity == nil {
		return nil
	}
	if r.session == nil {
		return dbx.ErrNilDB
	}
	if err := r.auditWriter.WriteAudit(ctx, r.session, operation, entity); err != nil {
		return fmt.Errorf("repository audit %s: %w", operation, err)
	}
	return nil
}

func (r *Base[E, S]) writeAuditValue(ctx context.Context, operation AuditOperation, entity E) error {
	return r.writeAudit(ctx, operation, &entity)
}

func (r *Base[E, S]) writeAuditEntities(ctx context.Context, operation AuditOperation, entities ...*E) error {
	var auditErr error
	collectionx.NewList[*E](entities...).Range(func(_ int, entity *E) bool {
		if err := r.writeAudit(ctx, operation, entity); err != nil {
			auditErr = err
			return false
		}
		return true
	})
	return auditErr
}

func (r *Base[E, S]) hasAuditWriter() bool {
	return r != nil && r.auditWriter != nil
}

func (r *Base[E, S]) getByKeyForAudit(ctx context.Context, key Key) (E, error) {
	return r.first(ctx, r.applySpecsWithDefaults(false, Where(keyPredicate(r.schema, key))), false)
}

func (r *Base[E, S]) getByKeyForAuditOption(ctx context.Context, key Key) (mo.Option[E], error) {
	return optionFromResult(r.getByKeyForAudit(ctx, key))
}

func (r *Base[E, S]) entityForDeleteAuditByKey(ctx context.Context, key Key) (mo.Option[E], error) {
	if !r.hasAuditWriter() {
		return mo.None[E](), nil
	}
	found, err := r.getByKeyForAuditOption(ctx, key)
	if err != nil {
		return mo.None[E](), err
	}
	return found, nil
}

func (r *Base[E, S]) auditUpdatedByKey(ctx context.Context, result sql.Result, key Key) error {
	if !hasAffectedRows(result) || !r.hasAuditWriter() {
		return nil
	}
	item, err := r.getByKeyForAudit(ctx, key)
	if err != nil {
		return err
	}
	return r.writeAuditValue(ctx, AuditOperationUpdate, item)
}

func (r *Base[E, S]) getByKeySetForAudit(ctx context.Context, key TypedKeySet) (E, error) {
	var zero E
	predicate, err := key.Predicate()
	if err != nil {
		return zero, err
	}
	return r.first(ctx, r.applySpecsWithDefaults(false, Where(predicate)), false)
}

func (r *Base[E, S]) getByKeySetForAuditOption(ctx context.Context, key TypedKeySet) (mo.Option[E], error) {
	return optionFromResult(r.getByKeySetForAudit(ctx, key))
}

func (r *Base[E, S]) entityForDeleteAuditByKeySet(ctx context.Context, key TypedKeySet) (mo.Option[E], error) {
	if !r.hasAuditWriter() {
		return mo.None[E](), nil
	}
	found, err := r.getByKeySetForAuditOption(ctx, key)
	if err != nil {
		return mo.None[E](), err
	}
	return found, nil
}

func (r *Base[E, S]) auditUpdatedByKeySet(ctx context.Context, result sql.Result, key TypedKeySet) error {
	if !hasAffectedRows(result) || !r.hasAuditWriter() {
		return nil
	}
	item, err := r.getByKeySetForAudit(ctx, key)
	if err != nil {
		return err
	}
	return r.writeAuditValue(ctx, AuditOperationUpdate, item)
}

func (r *Base[E, S]) auditDeletedValue(ctx context.Context, result sql.Result, item mo.Option[E]) error {
	if !hasAffectedRows(result) || item.IsAbsent() {
		return nil
	}
	value, _ := item.Get()
	return r.writeAuditValue(ctx, AuditOperationDelete, value)
}

func (k TypedKey[E, S, T]) getForAudit(ctx context.Context, value T) (E, error) {
	var zero E
	if k.repo == nil {
		return zero, dbx.ErrNilDB
	}
	return k.repo.first(ctx, k.repo.applySpecsWithDefaults(false, Where(k.column.Eq(value))), false)
}

func (k TypedKey[E, S, T]) getForAuditOption(ctx context.Context, value T) (mo.Option[E], error) {
	return optionFromResult(k.getForAudit(ctx, value))
}

func (k TypedKey[E, S, T]) entityForDeleteAudit(ctx context.Context, value T) (mo.Option[E], error) {
	if k.repo == nil || !k.repo.hasAuditWriter() {
		return mo.None[E](), nil
	}
	found, err := k.getForAuditOption(ctx, value)
	if err != nil {
		return mo.None[E](), err
	}
	return found, nil
}

func (k TypedKey[E, S, T]) auditUpdated(ctx context.Context, result sql.Result, value T) error {
	if k.repo == nil || !hasAffectedRows(result) || !k.repo.hasAuditWriter() {
		return nil
	}
	item, err := k.getForAudit(ctx, value)
	if err != nil {
		return err
	}
	return k.repo.writeAuditValue(ctx, AuditOperationUpdate, item)
}
