package shared

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"

	"github.com/DaiYuANg/arcgo/dbx"
	sqlitedialect "github.com/DaiYuANg/arcgo/dbx/dialect/sqlite"
	"github.com/DaiYuANg/arcgo/logx"
	_ "modernc.org/sqlite" // Register the SQLite driver used by the examples.
)

// NewLogger creates a debug logger for dbx example programs.
func NewLogger() *slog.Logger {
	return logx.MustNew(
		logx.WithConsole(true),
		logx.WithLevel(slog.LevelDebug),
	)
}

// OpenSQLite opens a SQLite DB with connection managed by dbx. Returns (db, closeFn, err).
// Call closeFn() or db.Close() when done.
func OpenSQLite(name string, opts ...dbx.Option) (*dbx.DB, func() error, error) {
	dsn := fmt.Sprintf("file:%s?mode=memory&cache=shared", name)
	db, err := dbx.Open(
		dbx.WithDriver("sqlite"),
		dbx.WithDSN(dsn),
		dbx.WithDialect(sqlitedialect.New()),
		dbx.ApplyOptions(opts...),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("open dbx SQLite connection: %w", err)
	}
	if _, err = db.ExecContext(context.Background(), `PRAGMA foreign_keys = ON`); err != nil {
		return nil, nil, joinCloseError(fmt.Errorf("enable SQLite foreign keys: %w", err), "dbx SQLite connection", db.Close)
	}

	return db, db.Close, nil
}

// OpenSQLiteRaw returns dbx wrapping an existing *sql.DB. Caller owns raw and must close it.
func OpenSQLiteRaw(name string, opts ...dbx.Option) (*dbx.DB, func() error, error) {
	raw, err := sql.Open("sqlite", fmt.Sprintf("file:%s?mode=memory&cache=shared", name))
	if err != nil {
		return nil, nil, fmt.Errorf("open raw SQLite connection: %w", err)
	}
	if err = raw.PingContext(context.Background()); err != nil {
		return nil, nil, joinCloseError(fmt.Errorf("ping raw SQLite connection: %w", err), "raw SQLite connection", raw.Close)
	}
	if _, err = raw.ExecContext(context.Background(), `PRAGMA foreign_keys = ON`); err != nil {
		return nil, nil, joinCloseError(fmt.Errorf("enable raw SQLite foreign keys: %w", err), "raw SQLite connection", raw.Close)
	}

	db, err := dbx.NewWithOptions(raw, sqlitedialect.New(), opts...)
	if err != nil {
		return nil, nil, joinCloseError(fmt.Errorf("wrap raw SQLite connection with dbx: %w", err), "raw SQLite connection", raw.Close)
	}

	return db, raw.Close, nil
}

func joinCloseError(baseErr error, target string, closeFn func() error) error {
	closeErr := closeFn()
	if closeErr == nil {
		return baseErr
	}

	return errors.Join(baseErr, fmt.Errorf("close %s: %w", target, closeErr))
}
