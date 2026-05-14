package audit

import (
	"context"
	"fmt"

	"github.com/arcgolabs/dbx"
)

// Writer is the low-level audit API. Use it when mutation code is not going
// through repository helpers.
type Writer[E any] struct {
	spec EntitySpec[E]
}

// NewWriter creates a low-level audit writer.
func NewWriter[E any](spec EntitySpec[E]) Writer[E] {
	return Writer[E]{spec: spec}
}

// Writer returns a low-level audit writer for this entity binding.
func (s EntitySpec[E]) Writer() Writer[E] {
	return NewWriter(s)
}

// Insert writes an insert audit row.
func (w Writer[E]) Insert(ctx context.Context, session dbx.Session, entity *E) error {
	return w.Write(ctx, session, OperationInsert, entity)
}

// Update writes an update audit row.
func (w Writer[E]) Update(ctx context.Context, session dbx.Session, entity *E) error {
	return w.Write(ctx, session, OperationUpdate, entity)
}

// Upsert writes an upsert audit row.
func (w Writer[E]) Upsert(ctx context.Context, session dbx.Session, entity *E) error {
	return w.Write(ctx, session, OperationUpsert, entity)
}

// Delete writes a delete audit row.
func (w Writer[E]) Delete(ctx context.Context, session dbx.Session, entity *E) error {
	return w.Write(ctx, session, OperationDelete, entity)
}

// Write writes an audit row for operation and entity.
func (w Writer[E]) Write(ctx context.Context, session dbx.Session, operation Operation, entity *E) error {
	return w.spec.Write(ctx, session, operation, entity)
}

// WriteAudit implements repository.AuditWriter without importing repository.
func (s EntitySpec[E]) WriteAudit(ctx context.Context, session dbx.Session, operation string, entity any) error {
	switch typed := entity.(type) {
	case *E:
		return s.Write(ctx, session, Operation(operation), typed)
	case E:
		return s.Write(ctx, session, Operation(operation), &typed)
	default:
		return fmt.Errorf("dbx/audit: unsupported repository audit entity %T", entity)
	}
}
