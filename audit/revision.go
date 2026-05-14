package audit

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	collectionx "github.com/arcgolabs/collectionx/list"
	"github.com/arcgolabs/dbx"
	columnx "github.com/arcgolabs/dbx/column"
	"github.com/arcgolabs/dbx/idgen"
	"github.com/arcgolabs/dbx/querydsl"
	schemax "github.com/arcgolabs/dbx/schema"
)

// RevisionIDFactory creates a revision primary key.
type RevisionIDFactory func(context.Context, dbx.Session) (any, error)

// RevisionSpec binds an explicit revision table schema.
type RevisionSpec struct {
	table     querydsl.TableSource
	id        schemax.ColumnMeta
	createdAt schemax.ColumnMeta
	actor     schemax.ColumnMeta
	tenant    schemax.ColumnMeta
	reason    schemax.ColumnMeta
	metadata  schemax.ColumnMeta

	idFactory RevisionIDFactory
	clock     func() time.Time
	validated bool
}

// RevisionRecord is the created or reused revision row identity.
type RevisionRecord struct {
	ID        any
	CreatedAt time.Time
	Info      RevisionInfo
}

// RevisionOption configures a revision table binding.
type RevisionOption func(*RevisionSpec)

// RevisionTable declares the table and required columns used for revision rows.
func RevisionTable(table querydsl.TableSource, id, createdAt querydsl.ColumnAccessor, opts ...RevisionOption) RevisionSpec {
	spec := RevisionSpec{
		table:     table,
		id:        cloneColumn(id),
		createdAt: cloneColumn(createdAt),
		idFactory: defaultRevisionID,
		clock:     func() time.Time { return time.Now().UTC() },
	}
	applyRevisionOptions(&spec, opts...)
	spec.validated = spec.validate() == nil
	return spec
}

func applyRevisionOptions(spec *RevisionSpec, opts ...RevisionOption) {
	collectionx.FilterList[RevisionOption](collectionx.NewList[RevisionOption](opts...), func(_ int, opt RevisionOption) bool {
		return opt != nil
	}).Range(func(_ int, opt RevisionOption) bool {
		opt(spec)
		return true
	})
}

// RevisionActor maps RevisionInfo.Actor to a revision table column.
func RevisionActor(column querydsl.ColumnAccessor) RevisionOption {
	return func(spec *RevisionSpec) { spec.actor = cloneColumn(column) }
}

// RevisionTenant maps RevisionInfo.Tenant to a revision table column.
func RevisionTenant(column querydsl.ColumnAccessor) RevisionOption {
	return func(spec *RevisionSpec) { spec.tenant = cloneColumn(column) }
}

// RevisionReason maps RevisionInfo.Reason to a revision table column.
func RevisionReason(column querydsl.ColumnAccessor) RevisionOption {
	return func(spec *RevisionSpec) { spec.reason = cloneColumn(column) }
}

// RevisionMetadata maps RevisionInfo.Metadata to a revision table column.
func RevisionMetadata(column querydsl.ColumnAccessor) RevisionOption {
	return func(spec *RevisionSpec) { spec.metadata = cloneColumn(column) }
}

// WithRevisionIDFactory overrides revision ID generation.
func WithRevisionIDFactory(factory RevisionIDFactory) RevisionOption {
	return func(spec *RevisionSpec) {
		if factory != nil {
			spec.idFactory = factory
		}
	}
}

// WithClock overrides revision timestamp generation.
func WithClock(clock func() time.Time) RevisionOption {
	return func(spec *RevisionSpec) {
		if clock != nil {
			spec.clock = clock
		}
	}
}

// EnsureRevision creates or reuses a revision row for ctx and spec.
func EnsureRevision(ctx context.Context, session dbx.Session, spec RevisionSpec) (RevisionRecord, error) {
	if err := spec.ensureValid(); err != nil {
		return RevisionRecord{}, err
	}
	if scope, ok := revisionScopeFromContext(ctx); ok {
		return ensureScopedRevision(ctx, session, spec, scope)
	}
	return createRevision(ctx, session, spec)
}

func ensureScopedRevision(ctx context.Context, session dbx.Session, spec RevisionSpec, scope *revisionScope) (RevisionRecord, error) {
	scope.mu.Lock()
	defer scope.mu.Unlock()
	if scope.records == nil {
		scope.records = make(map[string]RevisionRecord)
	}
	key := spec.scopeKey()
	if record, ok := scope.records[key]; ok {
		return record, nil
	}
	record, err := createRevision(ctx, session, spec)
	if err != nil {
		return RevisionRecord{}, err
	}
	scope.records[key] = record
	return record, nil
}

func createRevision(ctx context.Context, session dbx.Session, spec RevisionSpec) (RevisionRecord, error) {
	if session == nil {
		return RevisionRecord{}, dbx.ErrNilDB
	}
	info := revisionInfoFromContext(ctx)
	createdAt := info.CreatedAt
	if createdAt.IsZero() {
		createdAt = spec.clock()
	}
	id, err := spec.idFactory(ctx, session)
	if err != nil {
		return RevisionRecord{}, fmt.Errorf("create revision id: %w", err)
	}
	record := RevisionRecord{ID: id, CreatedAt: createdAt, Info: info}
	assignments := revisionAssignments(spec, record)

	if _, err := dbx.Exec(ctx, session, querydsl.InsertInto(spec.table).ValuesList(assignments)); err != nil {
		return RevisionRecord{}, fmt.Errorf("insert revision: %w", err)
	}
	return record, nil
}

func revisionAssignments(spec RevisionSpec, record RevisionRecord) *collectionx.List[querydsl.Assignment] {
	assignments := collectionx.NewList[querydsl.Assignment](
		assignment(spec.table, spec.id, record.ID),
		assignment(spec.table, spec.createdAt, record.CreatedAt),
	)
	addOptionalAssignment(assignments, spec.table, spec.actor, record.Info.Actor)
	addOptionalAssignment(assignments, spec.table, spec.tenant, record.Info.Tenant)
	addOptionalAssignment(assignments, spec.table, spec.reason, record.Info.Reason)
	addOptionalAssignment(assignments, spec.table, spec.metadata, record.Info.Metadata)
	return assignments
}

func (s RevisionSpec) ensureValid() error {
	if s.validated {
		return nil
	}
	return s.validate()
}

func (s RevisionSpec) validate() error {
	if s.table == nil {
		return errors.New("dbx/audit: revision table is nil")
	}
	if !hasColumn(s.id) {
		return errors.New("dbx/audit: revision id column is empty")
	}
	if !hasColumn(s.createdAt) {
		return errors.New("dbx/audit: revision created_at column is empty")
	}
	if s.idFactory == nil {
		return errors.New("dbx/audit: revision id factory is nil")
	}
	if s.clock == nil {
		return errors.New("dbx/audit: revision clock is nil")
	}
	return nil
}

func (s RevisionSpec) scopeKey() string {
	if s.table == nil {
		return s.id.Name
	}
	return s.table.TableName() + "." + s.id.Name
}

func defaultRevisionID(ctx context.Context, session dbx.Session) (any, error) {
	type idGeneratorCarrier interface {
		IDGenerator() idgen.Generator
	}
	carrier, ok := any(session).(idGeneratorCarrier)
	if !ok || carrier.IDGenerator() == nil {
		return time.Now().UTC().UnixNano(), nil
	}
	id, err := carrier.IDGenerator().GenerateID(ctx, idgen.Request{Strategy: idgen.StrategySnowflake})
	if err != nil {
		return nil, fmt.Errorf("generate snowflake revision id: %w", err)
	}
	return id, nil
}

func addOptionalAssignment(assignments *collectionx.List[querydsl.Assignment], table querydsl.TableSource, column schemax.ColumnMeta, value any) {
	if !hasColumn(column) {
		return
	}
	assignments.Add(assignment(table, column, value))
}

func assignment(table querydsl.TableSource, column schemax.ColumnMeta, value any) querydsl.Assignment {
	return columnx.Named[any](table, column.Name).Set(value)
}

func cloneColumn(column querydsl.ColumnAccessor) schemax.ColumnMeta {
	if column == nil {
		return schemax.ColumnMeta{}
	}
	return schemax.CloneColumnMeta(column.ColumnRef())
}

func hasColumn(column schemax.ColumnMeta) bool {
	return strings.TrimSpace(column.Name) != ""
}
