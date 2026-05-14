package audit

import (
	"context"
	"errors"
	"fmt"

	collectionx "github.com/arcgolabs/collectionx/list"
	"github.com/arcgolabs/dbx"
	mapperx "github.com/arcgolabs/dbx/mapper"
	"github.com/arcgolabs/dbx/querydsl"
	schemax "github.com/arcgolabs/dbx/schema"
)

// ColumnCopy maps one source entity column into one audit table column.
type ColumnCopy struct {
	Source schemax.ColumnMeta
	Audit  schemax.ColumnMeta
}

// EntitySpec binds one entity schema to one explicit audit table schema.
type EntitySpec[E any] struct {
	revision   RevisionSpec
	source     schemax.SchemaSource[E]
	audit      querydsl.TableSource
	mapper     mapperx.Mapper[E]
	revisionID schemax.ColumnMeta
	operation  schemax.ColumnMeta
	copies     *collectionx.List[ColumnCopy]
	validated  bool
}

type entityConfig struct {
	revisionID schemax.ColumnMeta
	operation  schemax.ColumnMeta
	copies     *collectionx.List[ColumnCopy]
}

// EntityOption configures an entity audit binding.
type EntityOption func(*entityConfig)

// Entity creates an audit binding for an entity schema and audit table.
func Entity[E any, S schemax.SchemaSource[E], A querydsl.TableSource](revision RevisionSpec, source S, auditTable A, opts ...EntityOption) (EntitySpec[E], error) {
	config := newEntityConfig(opts...)
	spec := EntitySpec[E]{
		revision:   revision,
		source:     source,
		audit:      auditTable,
		revisionID: config.revisionID,
		operation:  config.operation,
		copies:     config.copies.Clone(),
	}
	if err := spec.validate(); err != nil {
		return EntitySpec[E]{}, err
	}
	mapper, err := mapperx.NewMapper[E](source)
	if err != nil {
		return EntitySpec[E]{}, err
	}
	spec.mapper = mapper
	spec.validated = true
	spec.revision.validated = true
	return spec, nil
}

func newEntityConfig(opts ...EntityOption) entityConfig {
	config := entityConfig{copies: collectionx.NewList[ColumnCopy]()}
	collectionx.FilterList[EntityOption](collectionx.NewList[EntityOption](opts...), func(_ int, opt EntityOption) bool {
		return opt != nil
	}).Range(func(_ int, opt EntityOption) bool {
		opt(&config)
		return true
	})
	return config
}

// MustEntity is Entity and panics on invalid bindings.
func MustEntity[E any, S schemax.SchemaSource[E], A querydsl.TableSource](revision RevisionSpec, source S, auditTable A, opts ...EntityOption) EntitySpec[E] {
	spec, err := Entity[E](revision, source, auditTable, opts...)
	if err != nil {
		panic(err)
	}
	return spec
}

// AuditRevisionID maps the audit row to the revision ID column.
func AuditRevisionID(column querydsl.ColumnAccessor) EntityOption {
	return func(config *entityConfig) { config.revisionID = cloneColumn(column) }
}

// OperationColumn maps the audit row operation column.
func OperationColumn(column querydsl.ColumnAccessor) EntityOption {
	return func(config *entityConfig) { config.operation = cloneColumn(column) }
}

// Copy maps a source entity column into the audit table.
func Copy(source, audit querydsl.ColumnAccessor) EntityOption {
	return func(config *entityConfig) {
		config.copies.Add(ColumnCopy{Source: cloneColumn(source), Audit: cloneColumn(audit)})
	}
}

// Key is an alias for Copy used to make primary-key audit mappings read clearly.
func Key(source, audit querydsl.ColumnAccessor) EntityOption {
	return Copy(source, audit)
}

// Source returns the audited entity schema.
func (s EntitySpec[E]) Source() schemax.SchemaSource[E] {
	return s.source
}

// AuditTable returns the explicit audit table schema.
func (s EntitySpec[E]) AuditTable() querydsl.TableSource {
	return s.audit
}

// Copies returns a copy of the configured source-to-audit column mappings.
func (s EntitySpec[E]) Copies() *collectionx.List[ColumnCopy] {
	if s.copies == nil {
		return collectionx.NewList[ColumnCopy]()
	}
	return s.copies.Clone()
}

// Write inserts one audit row with a concrete entity snapshot.
func (s EntitySpec[E]) Write(ctx context.Context, session dbx.Session, operation Operation, entity *E) error {
	if err := s.ensureValid(); err != nil {
		return err
	}
	if entity == nil {
		return errors.New("dbx/audit: entity is nil")
	}
	revision, err := EnsureRevision(ctx, session, s.revision)
	if err != nil {
		return err
	}
	assignments, err := s.auditAssignments(entity, revision.ID, operation)
	if err != nil {
		return err
	}
	if _, err := dbx.Exec(ctx, session, querydsl.InsertInto(s.audit).ValuesList(assignments)); err != nil {
		return fmt.Errorf("insert audit row: %w", err)
	}
	return nil
}

func (s EntitySpec[E]) auditAssignments(entity *E, revisionID any, operation Operation) (*collectionx.List[querydsl.Assignment], error) {
	assignments := collectionx.NewList[querydsl.Assignment](
		assignment(s.audit, s.revisionID, revisionID),
		assignment(s.audit, s.operation, string(operation)),
	)
	var bindErr error
	s.copies.Range(func(_ int, mapping ColumnCopy) bool {
		value, ok, err := s.mapper.BoundFieldValue(entity, mapping.Source.Name)
		if err != nil {
			bindErr = fmt.Errorf("bind audit column %s from source column %s: %w", mapping.Audit.Name, mapping.Source.Name, err)
			return false
		}
		if !ok {
			bindErr = fmt.Errorf("bind audit column %s from source column %s: unmapped source column", mapping.Audit.Name, mapping.Source.Name)
			return false
		}
		assignments.Add(assignment(s.audit, mapping.Audit, value))
		return true
	})
	if bindErr != nil {
		return nil, bindErr
	}
	return assignments, nil
}

func (s EntitySpec[E]) ensureValid() error {
	if s.validated {
		return nil
	}
	return s.validate()
}

func (s EntitySpec[E]) validate() error {
	if err := s.revision.validate(); err != nil {
		return err
	}
	if err := s.validateRequiredFields(); err != nil {
		return err
	}
	return s.validateCopies()
}

func (s EntitySpec[E]) validateRequiredFields() error {
	switch {
	case s.source == nil:
		return errors.New("dbx/audit: source schema is nil")
	case s.audit == nil:
		return errors.New("dbx/audit: audit table is nil")
	case !hasColumn(s.revisionID):
		return errors.New("dbx/audit: audit revision id column is empty")
	case !hasColumn(s.operation):
		return errors.New("dbx/audit: audit operation column is empty")
	case s.copies == nil || s.copies.IsEmpty():
		return errors.New("dbx/audit: audit column mappings are empty")
	default:
		return nil
	}
}

func (s EntitySpec[E]) validateCopies() error {
	var validationErr error
	s.copies.Range(func(_ int, mapping ColumnCopy) bool {
		if !hasColumn(mapping.Source) {
			validationErr = errors.New("dbx/audit: source copy column is empty")
			return false
		}
		if !hasColumn(mapping.Audit) {
			validationErr = errors.New("dbx/audit: audit copy column is empty")
			return false
		}
		return true
	})
	return validationErr
}
