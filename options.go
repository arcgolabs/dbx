package dbx

import (
	"log/slog"

	"github.com/DaiYuANg/arcgo/collectionx"
	"github.com/arcgolabs/dbx/idgen"
)

// Option configures a DB instance. Options are composable; later options override earlier ones.
type Option func(*options)

type options struct {
	logger         *slog.Logger
	hooks          collectionx.List[Hook]
	debug          bool
	idGenerator    idgen.Generator
	nodeID         uint16
	hasIDGenerator bool
	hasNodeID      bool
}

func defaultOptions() options {
	return options{
		logger: slog.Default(),
		hooks:  collectionx.NewListWithCapacity[Hook](4),
		debug:  false,
		nodeID: idgen.ResolveNodeIDFromHostName(),
	}
}

// DefaultOptions returns no options; use when you want explicit defaults (logger=slog.Default, debug=false, no hooks).
func DefaultOptions() []Option {
	return DefaultOptionsList().Values()
}

// DefaultOptionsList returns no options as a collectionx.List.
func DefaultOptionsList() collectionx.List[Option] {
	return collectionx.NewList[Option]()
}

// ProductionOptions returns options suitable for production: debug off, no extra hooks.
// Combine with WithLogger for custom logging. Same as defaults; use for explicitness.
func ProductionOptions() []Option {
	return ProductionOptionsList().Values()
}

// ProductionOptionsList returns production defaults as a collectionx.List.
func ProductionOptionsList() collectionx.List[Option] {
	return collectionx.NewList[Option](WithDebug(false))
}

// TestOptions returns options for tests: debug on (SQL logged). Combine with WithLogger, WithHooks as needed.
func TestOptions() []Option {
	return TestOptionsList().Values()
}

// TestOptionsList returns test defaults as a collectionx.List.
func TestOptionsList() collectionx.List[Option] {
	return collectionx.NewList[Option](WithDebug(true))
}

// WithLogger sets the logger for operation events. Default: slog.Default().
// When debug is false, only errors are logged; when true, all operations are logged at Debug level.
func WithLogger(logger *slog.Logger) Option {
	return func(opts *options) {
		if logger != nil {
			opts.logger = logger
		}
	}
}

// WithHooks appends hooks that run before/after each operation (query, exec, begin/commit/rollback, etc.).
// Hooks are additive; pass multiple or call WithHooks multiple times to combine.
func WithHooks(hooks ...Hook) Option {
	return WithHooksList(collectionx.NewList[Hook](hooks...))
}

// WithHooksList appends hooks from a collectionx.List.
func WithHooksList(hooks collectionx.List[Hook]) Option {
	filtered := collectionx.FilterList[Hook](hooks, func(_ int, hook Hook) bool {
		return hook != nil
	})
	return func(opts *options) {
		opts.hooks = mergeList(opts.hooks, filtered)
	}
}

// WithDebug enables SQL logging for all operations when true. Default: false.
// When false, only errors are logged. Use in development or tests to inspect queries.
func WithDebug(enabled bool) Option {
	return func(opts *options) {
		opts.debug = enabled
	}
}

// WithIDGenerator sets the DB-scoped ID generator used by mapper insert assignment helpers.
// Mutually exclusive with WithNodeID.
func WithIDGenerator(generator idgen.Generator) Option {
	return func(opts *options) {
		if generator == nil {
			return
		}
		opts.idGenerator = generator
		opts.hasIDGenerator = true
	}
}

// WithNodeID sets the DB node id used by the default Snowflake generator.
// Mutually exclusive with WithIDGenerator.
func WithNodeID(nodeID uint16) Option {
	return func(opts *options) {
		opts.nodeID = nodeID
		opts.hasNodeID = true
	}
}

func applyOptions(opts ...Option) (options, error) {
	return applyOptionsList(collectionx.NewList[Option](opts...))
}

func applyOptionsList(opts collectionx.List[Option]) (options, error) {
	config := defaultOptions()
	filtered := collectionx.FilterList[Option](opts, func(_ int, opt Option) bool {
		return opt != nil
	})
	filtered.Range(func(_ int, opt Option) bool {
		opt(&config)
		return true
	})
	if config.hasIDGenerator && config.hasNodeID {
		return options{}, ErrIDGeneratorNodeIDConflict
	}
	if config.hasNodeID {
		if config.nodeID < idgen.MinNodeID || config.nodeID > idgen.MaxNodeID {
			return options{}, &idgen.NodeIDOutOfRangeError{NodeID: config.nodeID, Min: idgen.MinNodeID, Max: idgen.MaxNodeID}
		}
	}
	return config, nil
}

func mergeList[T any](current, next collectionx.List[T]) collectionx.List[T] {
	if current == nil {
		return next.Clone()
	}
	current.Merge(next)
	return current
}
