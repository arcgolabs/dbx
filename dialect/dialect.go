package dialect

// Contract is the minimal dialect interface. Required for parameter binding and identification.
// Used by sqltmpl, dbx render, and other consumers that need Name and BindVar only.
type Contract interface {
	Name() string
	BindVar(n int) string
}

// QueryFeatures describes dialect-specific SQL rendering behavior for query DSL.
// Used by dbx render to avoid switch on dialect name. Implement QueryFeaturesProvider to customize.
type QueryFeatures struct {
	// InsertIgnoreForUpsertNothing: when true, use "INSERT IGNORE INTO" for upsert DoNothing (MySQL).
	InsertIgnoreForUpsertNothing bool
	// UpsertVariant: "on_conflict" (Postgres, SQLite ON CONFLICT), "on_duplicate_key" (MySQL), or "" (not supported).
	UpsertVariant string
	// ExcludedRefStyle: "excluded" (EXCLUDED.col), "values" (VALUES(col)), or "" (not supported).
	ExcludedRefStyle string
	// SupportsReturning: whether INSERT/UPDATE/DELETE support RETURNING clause (Postgres, SQLite).
	SupportsReturning bool
}

// QueryFeaturesProvider provides dialect-specific query rendering features.
// Dialect implementations should implement this to avoid name-based dispatch.
// If not implemented, DefaultQueryFeatures(name) is used for known dialects.
type QueryFeaturesProvider interface {
	QueryFeatures() QueryFeatures
}

// DefaultQueryFeatures returns features for known dialect names.
// Used when a dialect does not implement QueryFeaturesProvider.
func DefaultQueryFeatures(name string) QueryFeatures {
	switch name {
	case "postgres", "sqlite":
		return QueryFeatures{
			UpsertVariant:     "on_conflict",
			ExcludedRefStyle:  "excluded",
			SupportsReturning: true,
		}
	case "mysql":
		return QueryFeatures{
			InsertIgnoreForUpsertNothing: true,
			UpsertVariant:                "on_duplicate_key",
			ExcludedRefStyle:             "values",
			SupportsReturning:            false,
		}
	default:
		return QueryFeatures{}
	}
}

// Dialect extends Contract with query building capabilities (quoting, limit/offset).
// Implement this for basic query DSL support. Optionally implement QueryFeaturesProvider
// to declare upsert/returning support without name-based branching.
type Dialect interface {
	Contract
	QuoteIdent(ident string) string
	RenderLimitOffset(limit, offset *int) (string, error)
}
