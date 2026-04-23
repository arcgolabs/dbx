package dbx

import (
	"database/sql"
	"strings"

	"github.com/arcgolabs/collectionx"
	"github.com/arcgolabs/dbx/dialect"
	"github.com/arcgolabs/pkg/option"
	"github.com/samber/oops"
)

// OpenOption configures Open. Required: WithDriver, WithDSN, WithDialect.
// Use ApplyOptions to pass Option (WithLogger, WithHooks, WithDebug).
type OpenOption func(*openConfig) error

type openConfig struct {
	driver  string
	dsn     string
	dialect dialect.Dialect
	observe options
}

func defaultOpenConfig() openConfig {
	return openConfig{
		observe: defaultOptions(),
	}
}

// WithDriver sets the database driver name (e.g. "sqlite", "mysql", "postgres"). Required for Open.
func WithDriver(driver string) OpenOption {
	return func(c *openConfig) error {
		c.driver = strings.TrimSpace(driver)
		return nil
	}
}

// WithDSN sets the data source name. Required for Open.
func WithDSN(dsn string) OpenOption {
	return func(c *openConfig) error {
		c.dsn = strings.TrimSpace(dsn)
		return nil
	}
}

// WithDialect sets the dialect for query building. Required for Open.
func WithDialect(d dialect.Dialect) OpenOption {
	return func(c *openConfig) error {
		c.dialect = d
		return nil
	}
}

// ApplyOptions applies Option (WithLogger, WithHooks, WithDebug) to the DB created by Open.
func ApplyOptions(opts ...Option) OpenOption {
	return func(c *openConfig) error {
		observe, err := applyOptions(opts...)
		if err != nil {
			return err
		}
		c.observe = observe
		return nil
	}
}

// Open creates a DB with connection managed internally. Requires WithDriver, WithDSN, WithDialect.
// Returns error if any required option is missing or invalid. Call db.Close() when done.
func Open(opts ...OpenOption) (*DB, error) {
	config := defaultOpenConfig()
	logRuntimeNodeWithLogger(config.observe.logger, config.observe.debug, "db.open.start", "options", len(opts))
	if err := option.ApplyErr(&config, opts...); err != nil {
		logRuntimeNodeWithLogger(config.observe.logger, config.observe.debug, "db.open.error", "stage", "apply_option", "error", err)
		return nil, oops.In("dbx").
			With("op", "open", "stage", "apply_options", "option_count", len(opts)).
			Wrapf(err, "apply open options")
	}
	logRuntimeNodeWithLogger(config.observe.logger, config.observe.debug,
		"db.open.configured",
		"driver", config.driver,
		"dialect", dialectName(config.dialect),
		"hooks", config.observe.hooks.Len(),
	)

	if config.driver == "" {
		logRuntimeNodeWithLogger(config.observe.logger, config.observe.debug, "db.open.error", "stage", "validate", "error", ErrMissingDriver)
		return nil, oops.In("dbx").
			With("op", "open", "stage", "validate").
			Wrapf(ErrMissingDriver, "validate open config")
	}
	if config.dsn == "" {
		logRuntimeNodeWithLogger(config.observe.logger, config.observe.debug, "db.open.error", "stage", "validate", "error", ErrMissingDSN)
		return nil, oops.In("dbx").
			With("op", "open", "stage", "validate", "driver", config.driver).
			Wrapf(ErrMissingDSN, "validate open config")
	}
	if config.dialect == nil {
		logRuntimeNodeWithLogger(config.observe.logger, config.observe.debug, "db.open.error", "stage", "validate", "error", ErrMissingDialect)
		return nil, oops.In("dbx").
			With("op", "open", "stage", "validate", "driver", config.driver).
			Wrapf(ErrMissingDialect, "validate open config")
	}

	raw, err := sql.Open(config.driver, config.dsn)
	if err != nil {
		logRuntimeNodeWithLogger(config.observe.logger, config.observe.debug, "db.open.error", "stage", "sql_open", "error", err)
		return nil, oops.In("dbx").
			With("op", "open", "stage", "sql_open", "driver", config.driver, "dialect", dialectName(config.dialect)).
			Wrapf(err, "open database")
	}
	logRuntimeNodeWithLogger(config.observe.logger, config.observe.debug, "db.open.sql_opened", "driver", config.driver)

	dbOpts := collectionx.NewList[Option](
		WithLogger(config.observe.logger),
		WithHooksList(config.observe.hooks),
		WithDebug(config.observe.debug),
	)
	if config.observe.hasIDGenerator {
		dbOpts.Add(WithIDGenerator(config.observe.idGenerator))
	}
	if config.observe.hasNodeID {
		dbOpts.Add(WithNodeID(config.observe.nodeID))
	}
	db, err := NewWithOptionsList(raw, config.dialect, dbOpts)
	if err != nil {
		logRuntimeNodeWithLogger(config.observe.logger, config.observe.debug, "db.open.error", "stage", "new_with_options", "error", err)
		return nil, oops.In("dbx").
			With("op", "open", "stage", "build_runtime", "driver", config.driver, "dialect", dialectName(config.dialect)).
			Wrapf(err, "build db runtime")
	}
	logRuntimeNodeWithLogger(config.observe.logger, config.observe.debug, "db.open.done", "driver", config.driver, "dialect", dialectName(config.dialect))
	return db, nil
}
