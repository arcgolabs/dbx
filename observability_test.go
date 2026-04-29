package dbx_test

import (
	"context"
	"github.com/arcgolabs/dbx/querydsl"
	schemax "github.com/arcgolabs/dbx/schema"
	"log/slog"
	"sync"
	"testing"
	"time"

	collectionx "github.com/arcgolabs/collectionx/list"
)

type memoryHandler struct {
	mu      sync.Mutex
	records []memoryRecord
}

type memoryRecord struct {
	level   slog.Level
	message string
	attrs   map[string]any
}

func (h *memoryHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *memoryHandler) Handle(_ context.Context, record slog.Record) error {
	entry := memoryRecord{
		level:   record.Level,
		message: record.Message,
		attrs:   make(map[string]any, record.NumAttrs()),
	}
	record.Attrs(func(attr slog.Attr) bool {
		entry.attrs[attr.Key] = attr.Value.Any()
		return true
	})

	h.mu.Lock()
	h.records = append(h.records, entry)
	h.mu.Unlock()
	return nil
}

func (h *memoryHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *memoryHandler) WithGroup(string) slog.Handler      { return h }

func findRecordByAttr(records []memoryRecord, key string, expected any) (memoryRecord, bool) {
	for _, record := range records {
		if value, ok := record.attrs[key]; ok && value == expected {
			return record, true
		}
	}
	return memoryRecord{}, false
}

func TestDBDebugLoggingAndHooks(t *testing.T) {
	sqlDB, cleanup := OpenTestSQLiteWithSchema(t, `INSERT INTO "roles" ("id","name") VALUES (9,'admin')`)
	defer cleanup()

	handler := &memoryHandler{records: make([]memoryRecord, 0, 4)}
	logger := slog.New(handler)
	beforeCount := 0
	afterCount := 0

	db := MustNewWithOptions(
		sqlDB,
		testSQLiteDialect{},
		WithLogger(logger),
		WithDebug(true),
		WithHooks(HookFuncs{
			BeforeFunc: func(ctx context.Context, event *HookEvent) (context.Context, error) {
				beforeCount++
				if event.Operation != OperationExec {
					t.Fatalf("unexpected hook operation: %s", event.Operation)
				}
				return ctx, nil
			},
			AfterFunc: func(_ context.Context, event *HookEvent) {
				afterCount++
				if event.Err != nil {
					t.Fatalf("unexpected hook error: %v", event.Err)
				}
			},
		}),
	)

	users := MustSchema("users", UserSchema{})
	mapper := MustMapper[User](users)
	entity := &User{Username: "alice", Email: "alice@example.com", Status: 1, RoleID: 9}
	assignments, err := mapper.InsertAssignments(db, users, entity)
	if err != nil {
		t.Fatalf("InsertAssignments returned error: %v", err)
	}
	mustExecInsert(context.Background(), t, db, InsertInto(users).Values(assignments.Values()...))
	assertDebugHookCounts(t, beforeCount, afterCount)
	record := mustFindOperationRecord(t, handler.records, OperationExec)
	assertDebugRecord(t, record, OperationExec)
}

func TestSchemaOperationsEmitObserverEvents(t *testing.T) {
	handler := &memoryHandler{records: make([]memoryRecord, 0, 8)}
	logger := slog.New(handler)
	beforeOps := make([]Operation, 0, 2)
	afterOps := make([]Operation, 0, 2)

	users := MustSchema("users", UserSchema{})
	schemaDialect := newFakeSchemaDialect()
	schemaDialect.tables["users"] = observedUserTableState(t, users)

	db := MustNewWithOptions(
		nil,
		schemaDialect,
		WithLogger(logger),
		WithDebug(true),
		WithHooks(HookFuncs{
			BeforeFunc: func(ctx context.Context, event *HookEvent) (context.Context, error) {
				beforeOps = append(beforeOps, event.Operation)
				return ctx, nil
			},
			AfterFunc: func(_ context.Context, event *HookEvent) {
				afterOps = append(afterOps, event.Operation)
			},
		}),
	)

	if _, err := ValidateSchemas(context.Background(), db, users); err != nil {
		t.Fatalf("ValidateSchemas returned error: %v", err)
	}
	if _, err := AutoMigrate(context.Background(), db, users); err != nil {
		t.Fatalf("AutoMigrate returned error: %v", err)
	}

	if len(beforeOps) != 2 || beforeOps[0] != OperationValidate || beforeOps[1] != OperationAutoMigrate {
		t.Fatalf("unexpected before ops: %#v", beforeOps)
	}
	if len(afterOps) != 2 || afterOps[0] != OperationValidate || afterOps[1] != OperationAutoMigrate {
		t.Fatalf("unexpected after ops: %#v", afterOps)
	}
	if len(handler.records) < 2 {
		t.Fatalf("expected schema operation logs, got %d", len(handler.records))
	}
}

func observedUserTableState(t *testing.T, users UserSchema) schemax.TableState {
	t.Helper()

	spec := TableSpecForTest(users)
	columns := users.Columns()
	columnStateAt := func(index int) schemax.ColumnState {
		column, ok := columns.Get(index)
		if !ok {
			t.Fatalf("expected column at index %d", index)
		}
		return toColumnState(column)
	}

	return schemax.TableState{
		Exists:      true,
		Name:        "users",
		Columns:     collectionx.NewList[schemax.ColumnState](columnStateAt(0), columnStateAt(1), columnStateAt(2), columnStateAt(3), columnStateAt(4)),
		Indexes:     toIndexStates(spec.Indexes),
		PrimaryKey:  &schemax.PrimaryKeyState{Name: spec.PrimaryKey.Name, Columns: spec.PrimaryKey.Columns.Clone()},
		ForeignKeys: toForeignKeyStates(spec.ForeignKeys),
	}
}

func TestHookEventMetadataAndDuration(t *testing.T) {
	sqlDB, cleanup := OpenTestSQLiteWithSchema(t, `INSERT INTO "roles" ("id","name") VALUES (1,'user')`)
	defer cleanup()

	handler := &memoryHandler{records: make([]memoryRecord, 0, 4)}
	logger := slog.New(handler)
	var afterEvent *HookEvent

	users := MustSchema("users", UserSchema{})
	mapper := MustMapper[User](users)
	entity := &User{Username: "bob", Email: "bob@example.com", Status: 1, RoleID: 1}
	db := MustNewWithOptions(
		sqlDB,
		testSQLiteDialect{},
		WithLogger(logger),
		WithDebug(true),
		WithHooks(HookFuncs{
			BeforeFunc: func(ctx context.Context, event *HookEvent) (context.Context, error) {
				event.SetMetadata("trace_id", "abc-123")
				event.SetMetadata("request_id", "req-456")
				return ctx, nil
			},
			AfterFunc: func(_ context.Context, event *HookEvent) {
				afterEvent = event
			},
		}),
	)
	assignments, err := mapper.InsertAssignments(db, users, entity)
	if err != nil {
		t.Fatalf("InsertAssignments returned error: %v", err)
	}

	type ctxKey struct{}
	ctx := context.WithValue(context.Background(), ctxKey{}, "abc-123")
	mustExecInsert(ctx, t, db, InsertInto(users).Values(assignments.Values()...))
	assertHookEventMetadata(t, afterEvent, "abc-123", "req-456")
	assertTraceRecord(t, mustFindOperationRecord(t, handler.records, OperationExec), "abc-123", "req-456")
}

func mustExecInsert(ctx context.Context, t *testing.T, db *DB, query *querydsl.InsertQuery) {
	t.Helper()
	if _, err := Exec(ctx, db, query); err != nil {
		t.Fatalf("Exec returned error: %v", err)
	}
}

func assertDebugHookCounts(t *testing.T, beforeCount, afterCount int) {
	t.Helper()
	if beforeCount != 1 || afterCount != 1 {
		t.Fatalf("unexpected hook counts: before=%d after=%d", beforeCount, afterCount)
	}
}

func mustFindOperationRecord(t *testing.T, records []memoryRecord, operation Operation) memoryRecord {
	t.Helper()
	if len(records) == 0 {
		t.Fatal("expected debug log record")
	}
	record, ok := findRecordByAttr(records, "operation", operation)
	if !ok {
		t.Fatalf("expected operation log record, got %#v", records)
	}
	return record
}

func assertDebugRecord(t *testing.T, record memoryRecord, operation Operation) {
	t.Helper()
	if record.level != slog.LevelDebug {
		t.Fatalf("unexpected log level: %v", record.level)
	}
	if record.attrs["operation"] != operation {
		t.Fatalf("unexpected log attrs: %#v", record.attrs)
	}
	if record.attrs["sql"] == "" {
		t.Fatalf("expected sql log attr, got %#v", record.attrs)
	}
}

func assertHookEventMetadata(t *testing.T, event *HookEvent, traceID, requestID string) {
	t.Helper()
	if event == nil {
		t.Fatal("AfterFunc was not called")
	}
	traceValue, _ := event.Metadata.Get("trace_id")
	if traceValue != traceID {
		t.Fatalf("unexpected trace_id: %v", traceValue)
	}
	requestValue, _ := event.Metadata.Get("request_id")
	if requestValue != requestID {
		t.Fatalf("unexpected request_id: %v", requestValue)
	}
	if event.StartedAt.IsZero() {
		t.Fatal("expected StartedAt to be set")
	}
	if event.StartedAt.After(time.Now()) {
		t.Fatalf("expected StartedAt <= now: %v", event.StartedAt)
	}
	if event.Duration < 0 {
		t.Fatalf("expected non-negative Duration: %v", event.Duration)
	}
}

func assertTraceRecord(t *testing.T, record memoryRecord, traceID, requestID string) {
	t.Helper()
	if record.attrs["trace_id"] != traceID {
		t.Fatalf("expected trace_id in log attrs: %#v", record.attrs)
	}
	if record.attrs["request_id"] != requestID {
		t.Fatalf("expected request_id in log attrs: %#v", record.attrs)
	}
}
