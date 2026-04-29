//revive:disable:file-length-limit Mapper scan helpers are kept together to preserve related scan behavior.

package mapper

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"

	collectionx "github.com/arcgolabs/collectionx/list"
	scanlib "github.com/stephenafamo/scan"
)

type scanPlan struct {
	fields      *collectionx.List[MappedField]
	codecFields *collectionx.List[scanCodecField]
}

type scanCodecField struct {
	index int
	field MappedField
}

type Cursor[E any] interface {
	Close() error
	Next() bool
	Get() (E, error)
	Err() error
}

type scanCursor[E any] struct {
	cursor scanlib.ICursor[E]
}

func (c scanCursor[E]) Close() error {
	return wrapDBError("close scan cursor", c.cursor.Close())
}

func (c scanCursor[E]) Next() bool {
	return c.cursor.Next()
}

func (c scanCursor[E]) Get() (E, error) {
	value, err := c.cursor.Get()
	return value, wrapDBError("get scan cursor value", err)
}

func (c scanCursor[E]) Err() error {
	return wrapDBError("read scan cursor error", c.cursor.Err())
}

func (m StructMapper[E]) ScanRows(rows *sql.Rows) (*collectionx.List[E], error) {
	return m.scanRowsWithCapacity(rows, 0)
}

func (m StructMapper[E]) ScanRowsWithCapacity(rows *sql.Rows, capacityHint int) (*collectionx.List[E], error) {
	return m.scanRowsWithCapacity(rows, capacityHint)
}

func (m StructMapper[E]) scanRowsWithCapacity(rows *sql.Rows, capacityHint int) (*collectionx.List[E], error) {
	if m.meta == nil {
		return nil, ErrNilMapper
	}
	if rows == nil {
		return nil, errors.New("dbx: rows is nil")
	}

	columns, err := rows.Columns()
	if err != nil {
		return nil, wrapDBError("read row columns", err)
	}
	plan, err := m.scanPlan(columns)
	if err != nil {
		return nil, err
	}
	if capacityHint < 0 {
		capacityHint = 0
	}
	return m.collectRowsWithCapacity(context.Background(), plan, rows, capacityHint)
}

func (m StructMapper[E]) collectRowsWithCapacity(ctx context.Context, plan *scanPlan, rows *sql.Rows, capacityHint int) (_ *collectionx.List[E], err error) {
	cursor, err := scanlib.CursorFromRows(ctx, m.scanMapper(plan), rows)
	if err != nil {
		return nil, wrapDBError("open scan cursor", err)
	}
	defer func() {
		err = errors.Join(err, wrapDBError("close scan cursor", cursor.Close()))
	}()
	result := collectionx.NewListWithCapacity[E](capacityHint)
	for cursor.Next() {
		value, getErr := cursor.Get()
		if getErr != nil {
			return nil, wrapDBError("get scan cursor value", getErr)
		}
		result.Add(value)
	}
	return result, wrapDBError("read scan cursor error", cursor.Err())
}

func (m StructMapper[E]) scanCursor(ctx context.Context, rows *sql.Rows) (Cursor[E], error) {
	if m.meta == nil {
		return nil, ErrNilMapper
	}
	if rows == nil {
		return nil, errors.New("dbx: rows is nil")
	}

	columns, err := rows.Columns()
	if err != nil {
		return nil, wrapDBError("read row columns", err)
	}
	plan, err := m.scanPlan(columns)
	if err != nil {
		return nil, err
	}

	cursor, err := scanlib.CursorFromRows(ctx, m.scanMapper(plan), rows)
	if err != nil {
		return nil, wrapDBError("open scan cursor", err)
	}
	return scanCursor[E]{cursor: cursor}, nil
}

func (m StructMapper[E]) ScanCursor(ctx context.Context, rows *sql.Rows) (Cursor[E], error) {
	return m.scanCursor(ctx, rows)
}

func (m StructMapper[E]) ScanPlan(columns []string) error {
	_, err := m.scanPlan(columns)
	return err
}

func (m StructMapper[E]) scanPlan(columns []string) (*scanPlan, error) {
	signature := scanSignature(columns)
	if cached, ok := m.meta.scanPlans.Peek(signature); ok {
		return cached, nil
	}

	fields := collectionx.NewListWithCapacity[MappedField](len(columns))
	for _, column := range columns {
		field, ok := m.resolveFieldByResultColumn(column)
		if !ok {
			return nil, &UnmappedColumnError{Column: column}
		}
		fields.Add(field)
	}

	plan := newScanPlan(fields)
	if cached, ok := m.meta.scanPlans.Peek(signature); ok {
		return cached, nil
	}
	m.meta.scanPlans.Set(signature, plan)
	return plan, nil
}

func newScanPlan(fields *collectionx.List[MappedField]) *scanPlan {
	return &scanPlan{
		fields: fields,
		codecFields: collectionx.FilterMapList[MappedField, scanCodecField](fields, func(index int, field MappedField) (scanCodecField, bool) {
			return scanCodecField{index: index, field: field}, field.codec != nil
		}),
	}
}

type rowScanState struct {
	value        reflect.Value
	codecSources []any
}

func (m StructMapper[E]) scanMapper(plan *scanPlan) scanlib.Mapper[E] {
	return func(_ context.Context, _ []string) (func(*scanlib.Row) (any, error), func(any) (E, error)) {
		return m.scheduleScanState(plan), m.decodeScanState(plan)
	}
}

func (m StructMapper[E]) scheduleScanState(plan *scanPlan) func(*scanlib.Row) (any, error) {
	return func(row *scanlib.Row) (any, error) {
		state := m.newRowScanState(plan)
		if err := m.scheduleMappedFieldScans(plan, row, &state); err != nil {
			return nil, err
		}
		return state, nil
	}
}

func (m StructMapper[E]) decodeScanState(plan *scanPlan) func(any) (E, error) {
	return func(state any) (E, error) {
		current, err := scanStateFromAny[E](state)
		if err != nil {
			return zeroValue[E](), err
		}
		if err := m.decodeCodecFields(plan, current); err != nil {
			return zeroValue[E](), err
		}
		return scannedValueFromState[E](current)
	}
}

func (m StructMapper[E]) newRowScanState(plan *scanPlan) rowScanState {
	state := rowScanState{
		value: reflect.New(m.meta.entityType).Elem(),
	}
	if plan.codecFields.Len() > 0 {
		state.codecSources = make([]any, plan.fields.Len())
	}
	return state
}

func (m StructMapper[E]) scheduleMappedFieldScans(plan *scanPlan, row *scanlib.Row, state *rowScanState) error {
	var scanErr error
	plan.fields.Range(func(index int, field MappedField) bool {
		if err := m.scheduleMappedFieldScan(row, state, field, index); err != nil {
			scanErr = err
			return false
		}
		return true
	})
	return scanErr
}

func (m StructMapper[E]) scheduleMappedFieldScan(row *scanlib.Row, state *rowScanState, field MappedField, index int) error {
	fieldValue, err := ensureFieldValue(state.value, field)
	if err != nil {
		return err
	}
	if field.codec != nil {
		row.ScheduleScanByIndexX(index, reflect.ValueOf(&state.codecSources[index]))
		return nil
	}
	row.ScheduleScanByIndexX(index, fieldValue.Addr())
	return nil
}

func (m StructMapper[E]) decodeCodecFields(plan *scanPlan, state rowScanState) error {
	if plan.codecFields.Len() == 0 {
		return nil
	}
	var decodeErr error
	plan.codecFields.Range(func(_ int, item scanCodecField) bool {
		if err := m.decodeCodecField(state, item.field, item.index); err != nil {
			decodeErr = err
			return false
		}
		return true
	})
	return decodeErr
}

func (m StructMapper[E]) decodeCodecField(state rowScanState, field MappedField, index int) error {
	if field.codec == nil {
		return nil
	}
	fieldValue, err := ensureFieldValue(state.value, field)
	if err != nil {
		return err
	}
	if err := field.codec.Decode(state.codecSources[index], fieldValue); err != nil {
		return wrapDBError("decode mapped field", err)
	}
	return nil
}

func scanStateFromAny[E any](state any) (rowScanState, error) {
	current, ok := state.(rowScanState)
	if !ok {
		return rowScanState{}, fmt.Errorf("dbx: unexpected scan state %T", state)
	}
	return current, nil
}

func scannedValueFromState[E any](state rowScanState) (E, error) {
	value, ok := state.value.Interface().(E)
	if !ok {
		return zeroValue[E](), fmt.Errorf("dbx: scanned value type %T does not match target", state.value.Interface())
	}
	return value, nil
}

func zeroValue[T any]() T {
	var zero T
	return zero
}

func (m StructMapper[E]) resolveFieldByResultColumn(column string) (MappedField, bool) {
	if m.meta == nil {
		return MappedField{}, false
	}
	if field, ok := m.meta.byColumn.Get(column); ok {
		return field, true
	}
	normalized := normalizeResultColumnName(column)
	if normalized == "" {
		return MappedField{}, false
	}
	return m.meta.byNormalizedColumn.Get(normalized)
}

func scanSignature(columns []string) string {
	return strings.Join(columns, "\x1f")
}

func normalizeResultColumnName(column string) string {
	trimmed := strings.TrimSpace(column)
	if trimmed == "" {
		return ""
	}
	parts := strings.Split(trimmed, ".")
	last := parts[len(parts)-1]
	last = strings.TrimSpace(last)
	last = strings.Trim(last, "`\"")
	last = strings.TrimPrefix(last, "[")
	last = strings.TrimSuffix(last, "]")
	return strings.ToLower(strings.TrimSpace(last))
}
