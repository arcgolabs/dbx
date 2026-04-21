package dbx

import (
	"context"
	"database/sql"
	"log/slog"

	"github.com/DaiYuANg/arcgo/collectionx"
	"github.com/arcgolabs/dbx/dialect"
	"github.com/arcgolabs/dbx/idgen"
	"github.com/arcgolabs/dbx/relationruntime"
	"github.com/arcgolabs/dbx/sqlexec"
	"github.com/arcgolabs/dbx/sqlstmt"

	"github.com/samber/oops"
)

type DB struct {
	raw         *sql.DB
	dialect     dialect.Dialect
	observe     runtimeObserver
	relation    *relationruntime.Runtime
	idGenerator idgen.Generator
	nodeID      uint16
}

func New(raw *sql.DB, d dialect.Dialect) *DB {
	db, err := NewWithOptions(raw, d)
	if err != nil {
		panic(err)
	}
	return db
}

func NewWithOptions(raw *sql.DB, d dialect.Dialect, opts ...Option) (*DB, error) {
	return NewWithOptionsList(raw, d, collectionx.NewList[Option](opts...))
}

func NewWithOptionsList(raw *sql.DB, d dialect.Dialect, opts collectionx.List[Option]) (*DB, error) {
	config, err := applyOptionsList(opts)
	if err != nil {
		return nil, err
	}
	logRuntimeNodeWithLogger(config.logger, config.debug,
		"db.new.start",
		"has_sql_db", raw != nil,
		"dialect", dialectName(d),
		"hooks", config.hooks.Len(),
		"has_id_generator", config.idGenerator != nil,
		"node_id", config.nodeID,
	)
	idGenerator := config.idGenerator
	if idGenerator == nil {
		idGenerator, err = idgen.NewDefault(config.nodeID)
		if err != nil {
			logRuntimeNodeWithLogger(config.logger, config.debug, "db.new.error", "stage", "id_generator", "error", err)
			return nil, wrapDBError("create default id generator", err)
		}
	}
	db := &DB{
		raw:         raw,
		dialect:     d,
		observe:     newRuntimeObserver(config),
		relation:    relationruntime.New(),
		idGenerator: idGenerator,
		nodeID:      config.nodeID,
	}
	logRuntimeNodeWithLogger(config.logger, config.debug,
		"db.new.done",
		"dialect", dialectName(d),
		"hooks", config.hooks.Len(),
		"node_id", config.nodeID,
	)
	return db, nil
}

func MustNewWithOptions(raw *sql.DB, d dialect.Dialect, opts ...Option) *DB {
	db, err := NewWithOptions(raw, d, opts...)
	if err != nil {
		panic(err)
	}
	return db
}

func MustNewWithOptionsList(raw *sql.DB, d dialect.Dialect, opts collectionx.List[Option]) *DB {
	db, err := NewWithOptionsList(raw, d, opts)
	if err != nil {
		panic(err)
	}
	return db
}

func (db *DB) SQLDB() *sql.DB {
	return db.raw
}

func (db *DB) Dialect() dialect.Dialect {
	return db.dialect
}

func (db *DB) WithSQLDB(raw *sql.DB) *DB {
	return &DB{
		raw:         raw,
		dialect:     db.dialect,
		observe:     db.observe,
		relation:    db.relation,
		idGenerator: db.idGenerator,
		nodeID:      db.nodeID,
	}
}

// RelationRuntime returns the relation load runtime for this DB.
func (db *DB) RelationRuntime() *relationruntime.Runtime {
	if db == nil || db.relation == nil {
		return relationruntime.Default()
	}
	return db.relation
}

func (db *DB) Logger() *slog.Logger {
	return db.observe.logger
}

func (db *DB) Hooks() collectionx.List[Hook] {
	return db.observe.hooks.Clone()
}

func (db *DB) Debug() bool {
	return db.observe.debug
}

func (db *DB) IDGenerator() idgen.Generator {
	if db == nil {
		return nil
	}
	return db.idGenerator
}

func (db *DB) NodeID() uint16 {
	if db == nil {
		return 0
	}
	return db.nodeID
}

func (db *DB) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return db.queryContext(ctx, "", query, args...)
}

func (db *DB) queryContext(ctx context.Context, statement, query string, args ...any) (*sql.Rows, error) {
	if db == nil {
		return nil, oops.In("dbx").
			With("op", "query", "statement", statement).
			Wrapf(ErrNilDB, "validate db")
	}
	if db.raw == nil {
		return nil, oops.In("dbx").
			With("op", "query", "statement", statement).
			Wrapf(ErrNilSQLDB, "validate sql db")
	}

	return observedQueryContext(ctx, db.observe, statement, query, args, db.raw.QueryContext)
}

func (db *DB) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return db.execContext(ctx, "", query, args...)
}

func (db *DB) execContext(ctx context.Context, statement, query string, args ...any) (sql.Result, error) {
	if db == nil {
		return nil, oops.In("dbx").
			With("op", "exec", "statement", statement).
			Wrapf(ErrNilDB, "validate db")
	}
	if db.raw == nil {
		return nil, oops.In("dbx").
			With("op", "exec", "statement", statement).
			Wrapf(ErrNilSQLDB, "validate sql db")
	}

	return observedExecContext(ctx, db.observe, statement, query, args, db.raw.ExecContext)
}

func (db *DB) QueryRowContext(ctx context.Context, query string, args ...any) *Row {
	if db == nil {
		return errorRow(oops.In("dbx").
			With("op", "query_row").
			Wrapf(ErrNilDB, "validate db"))
	}
	if db.raw == nil {
		return errorRow(oops.In("dbx").
			With("op", "query_row").
			Wrapf(ErrNilSQLDB, "validate sql db"))
	}
	ctx, event, err := db.observe.before(ctx, HookEvent{Operation: OperationQueryRow, SQL: query, Args: collectionx.NewList[any](args...)})
	if err != nil {
		db.observe.after(ctx, event)
		return errorRow(err)
	}
	rows, queryErr := db.raw.QueryContext(ctx, query, args...)
	if queryErr != nil {
		event.Err = oops.In("dbx").
			With("op", "query_row").
			Wrapf(queryErr, "query row")
		db.observe.after(ctx, event)
		return errorRow(event.Err)
	}
	return observedRow(ctx, db.observe, event, rows)
}

func (db *DB) Bound(rawSQL string, args ...any) sqlstmt.Bound {
	return sqlstmt.Bound{SQL: rawSQL, Args: collectionx.NewList[any](args...)}
}

func (db *DB) QueryBoundContext(ctx context.Context, bound sqlstmt.Bound) (*sql.Rows, error) {
	return db.queryContext(ctx, bound.Name, bound.SQL, bound.Args.Values()...)
}

func (db *DB) ExecBoundContext(ctx context.Context, bound sqlstmt.Bound) (sql.Result, error) {
	return db.execContext(ctx, bound.Name, bound.SQL, bound.Args.Values()...)
}

func (db *DB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*Tx, error) {
	if db == nil {
		return nil, oops.In("dbx").
			With("op", "begin_tx").
			Wrapf(ErrNilDB, "validate db")
	}
	if db.raw == nil {
		return nil, oops.In("dbx").
			With("op", "begin_tx").
			Wrapf(ErrNilSQLDB, "validate sql db")
	}
	ctx, event, err := db.observe.before(ctx, HookEvent{Operation: OperationBeginTx})
	if err != nil {
		db.observe.after(ctx, event)
		return nil, err
	}
	tx, err := db.raw.BeginTx(ctx, opts)
	if err != nil {
		event.Err = oops.In("dbx").
			With("op", "begin_tx").
			Wrapf(err, "begin transaction")
		db.observe.after(ctx, event)
		return nil, event.Err
	}
	db.observe.after(ctx, event)
	return &Tx{raw: tx, dialect: db.dialect, observe: db.observe, relation: db.relation, idGenerator: db.idGenerator, nodeID: db.nodeID}, nil
}

func (db *DB) WithTx(tx *sql.Tx) *Tx {
	if tx == nil {
		return nil
	}
	return &Tx{raw: tx, dialect: db.dialect, observe: db.observe, relation: db.relation, idGenerator: db.idGenerator, nodeID: db.nodeID}
}

func (db *DB) SQL() *sqlexec.Executor {
	return sqlexec.New(db)
}

// Close closes the underlying database connection. Call when the DB is no longer needed.
// Safe to call if raw is nil. After Close, the DB should not be used for execution.
func (db *DB) Close() error {
	if db == nil || db.raw == nil {
		return nil
	}
	logRuntimeNode(db, "db.close.start")
	err := db.raw.Close()
	if err != nil {
		logRuntimeNode(db, "db.close.error", "error", err)
		return wrapDBError("close database", err)
	}
	logRuntimeNode(db, "db.close.done")
	return nil
}

func dialectName(d dialect.Dialect) string {
	if d == nil {
		return ""
	}
	return d.Name()
}
