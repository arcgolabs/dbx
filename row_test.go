package dbx_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"
)

func TestQueryRowContextBeforeHookReturnsScannableRow(t *testing.T) {
	sqlDB, cleanup := OpenTestSQLite(t)
	defer cleanup()

	want := errors.New("blocked")
	core := MustNewWithOptions(sqlDB, testSQLiteDialect{}, WithHooks(HookFuncs{
		BeforeFunc: func(ctx context.Context, event *HookEvent) (context.Context, error) {
			return ctx, want
		},
	}))

	var count int
	err := core.QueryRowContext(context.Background(), `SELECT 1`).Scan(&count)
	if !errors.Is(err, want) {
		t.Fatalf("expected before hook error, got: %v", err)
	}
}

func TestQueryRowContextAfterHookReceivesScanError(t *testing.T) {
	sqlDB, cleanup := OpenTestSQLite(t)
	defer cleanup()

	var afterErr error
	core := MustNewWithOptions(sqlDB, testSQLiteDialect{}, WithHooks(HookFuncs{
		AfterFunc: func(_ context.Context, event *HookEvent) {
			if event != nil && event.Operation == OperationQueryRow {
				afterErr = event.Err
			}
		},
	}))

	var value int
	err := core.QueryRowContext(context.Background(), `SELECT 'bad'`).Scan(&value)
	if err == nil {
		t.Fatal("expected scan error")
	}
	if afterErr == nil {
		t.Fatal("expected after hook to receive scan error")
	}
}

func TestQueryRowContextAfterHookReceivesNoRows(t *testing.T) {
	sqlDB, cleanup := OpenTestSQLite(t)
	defer cleanup()

	var afterErr error
	core := MustNewWithOptions(sqlDB, testSQLiteDialect{}, WithHooks(HookFuncs{
		AfterFunc: func(_ context.Context, event *HookEvent) {
			if event != nil && event.Operation == OperationQueryRow {
				afterErr = event.Err
			}
		},
	}))

	var value int
	err := core.QueryRowContext(context.Background(), `SELECT 1 WHERE 1 = 0`).Scan(&value)
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected no rows error, got: %v", err)
	}
	if !errors.Is(afterErr, sql.ErrNoRows) {
		t.Fatalf("expected after hook to receive sql.ErrNoRows, got: %v", afterErr)
	}
}
