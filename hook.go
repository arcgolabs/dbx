package dbx

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/DaiYuANg/arcgo/collectionx"
)

type Operation string

const (
	OperationQuery       Operation = "query"
	OperationExec        Operation = "exec"
	OperationQueryRow    Operation = "query_row"
	OperationBeginTx     Operation = "begin_tx"
	OperationCommitTx    Operation = "commit_tx"
	OperationRollbackTx  Operation = "rollback_tx"
	OperationAutoMigrate Operation = "auto_migrate"
	OperationValidate    Operation = "validate_schema"
)

// HookEvent carries operation details through Before/After hooks.
// Use it for logging, metrics, tracing, and slow-query detection.
type HookEvent struct {
	Operation       Operation
	Statement       string
	SQL             string
	Args            collectionx.List[any]
	Table           string
	StartedAt       time.Time // Set in Before, use with Duration for slow-query detection.
	Duration        time.Duration
	RowsAffected    int64
	HasRowsAffected bool
	Err             error

	// Metadata holds arbitrary key-value pairs (e.g. trace_id, request_id) for observability.
	// Hooks can set it in Before and read it in After; values are included in logs when present.
	// Use SetMetadata to initialize and populate it.
	Metadata collectionx.Map[string, any]
}

// SetMetadata sets a key-value pair in Metadata, initializing the map if needed.
func (e *HookEvent) SetMetadata(key string, value any) {
	if e.Metadata == nil {
		e.Metadata = collectionx.NewMap[string, any]()
	}
	e.Metadata.Set(key, value)
}

type Hook interface {
	Before(context.Context, *HookEvent) (context.Context, error)
	After(context.Context, *HookEvent)
}

type HookFuncs struct {
	BeforeFunc func(context.Context, *HookEvent) (context.Context, error)
	AfterFunc  func(context.Context, *HookEvent)
}

func (h HookFuncs) Before(ctx context.Context, event *HookEvent) (context.Context, error) {
	if h.BeforeFunc == nil {
		return ctx, nil
	}
	return h.BeforeFunc(ctx, event)
}

func (h HookFuncs) After(ctx context.Context, event *HookEvent) {
	if h.AfterFunc != nil {
		h.AfterFunc(ctx, event)
	}
}

type runtimeObserver struct {
	logger *slog.Logger
	hooks  collectionx.List[Hook]
	debug  bool
}

func newRuntimeObserver(opts options) runtimeObserver {
	return runtimeObserver{
		logger: opts.logger,
		hooks:  opts.hooks.Clone(),
		debug:  opts.debug,
	}
}

func (o runtimeObserver) before(ctx context.Context, event HookEvent) (context.Context, *HookEvent, error) {
	event.Args = event.Args.Clone()
	event.Metadata = event.Metadata.Clone()
	event.StartedAt = time.Now()

	ctx, err := o.applyBeforeHooks(ctx, &event, 0)
	if err != nil {
		event.Err = err
		return ctx, &event, err
	}
	return ctx, &event, nil
}

func (o runtimeObserver) applyBeforeHooks(ctx context.Context, event *HookEvent, index int) (context.Context, error) {
	if index >= o.hooks.Len() {
		return ctx, nil
	}
	hook, ok := o.hooks.Get(index)
	if !ok {
		return ctx, nil
	}
	nextCtx, err := hook.Before(ctx, event)
	if err != nil {
		return ctx, fmt.Errorf("dbx: before hook failed: %w", err)
	}
	return o.applyBeforeHooks(nextCtx, event, index+1)
}

func (o runtimeObserver) after(ctx context.Context, event *HookEvent) {
	if event == nil {
		return
	}
	if event.StartedAt.IsZero() {
		event.StartedAt = time.Now()
	}
	if event.Duration == 0 {
		event.Duration = time.Since(event.StartedAt)
	}

	o.log(*event)
	o.hooks.Range(func(_ int, hook Hook) bool {
		hook.After(ctx, event)
		return true
	})
}

func (o runtimeObserver) log(event HookEvent) {
	if o.logger == nil {
		return
	}
	if !o.debug && event.Err == nil {
		return
	}
	attrs := o.buildLogAttrs(event)
	if event.Err != nil {
		o.logger.Error("dbx operation failed", attrs...)
		return
	}
	o.logger.Debug("dbx operation", attrs...)
}

func (o runtimeObserver) buildLogAttrs(event HookEvent) []any {
	attrs := collectionx.NewListWithCapacity[any](14,
		"operation", event.Operation,
		"duration", event.Duration,
	)
	if event.Table != "" {
		attrs.Add("table", event.Table)
	}
	if event.Statement != "" {
		attrs.Add("statement", event.Statement)
	}
	if event.SQL != "" {
		attrs.Add("sql", event.SQL)
	}
	if event.Args.Len() > 0 {
		attrs.Add("args", event.Args.Values())
	}
	if event.HasRowsAffected {
		attrs.Add("rows_affected", event.RowsAffected)
	}
	event.Metadata.Range(func(key string, value any) bool {
		attrs.Add(key, value)
		return true
	})
	if event.Err != nil {
		attrs.Add("error", event.Err)
	}
	return attrs.Values()
}

// ObserveOperation runs fn through DB/Tx hooks when the session carries a runtime observer.
func ObserveOperation[T any](ctx context.Context, session Session, event HookEvent, fn func(context.Context) (T, error)) (T, error) {
	var zero T
	observer, ok := observerForSession(session)
	if !ok {
		return fn(ctx)
	}
	ctx, observedEvent, err := observer.before(ctx, event)
	if err != nil {
		observer.after(ctx, observedEvent)
		return zero, err
	}
	value, runErr := fn(ctx)
	observedEvent.Err = runErr
	observer.after(ctx, observedEvent)
	return value, runErr
}

func observerForSession(session Session) (runtimeObserver, bool) {
	switch typed := session.(type) {
	case *DB:
		if typed == nil {
			return runtimeObserver{}, false
		}
		return typed.observe, true
	case *Tx:
		if typed == nil {
			return runtimeObserver{}, false
		}
		return typed.observe, true
	default:
		return runtimeObserver{}, false
	}
}
