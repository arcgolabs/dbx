package dialect

import (
	"errors"
	"fmt"
	"strings"
)

// Selector is a canonical database dialect selector.
type Selector uint8

const (
	// SelectorAny is the wildcard selector used when no dialect is specified.
	SelectorAny Selector = iota
	// SelectorMySQL selects MySQL-compatible behavior.
	SelectorMySQL
	// SelectorPostgres selects PostgreSQL-compatible behavior.
	SelectorPostgres
	// SelectorSQLite selects SQLite-compatible behavior.
	SelectorSQLite
	// SelectorSQLServer selects SQL Server-compatible behavior.
	SelectorSQLServer
)

// ErrInvalidSelector reports an unknown dialect selector name.
var ErrInvalidSelector = errors.New("dbx/dialect: unknown dialect selector")

var selectorAliases = map[string]Selector{
	"":           SelectorAny,
	"mysql":      SelectorMySQL,
	"postgres":   SelectorPostgres,
	"postgresql": SelectorPostgres,
	"sqlite":     SelectorSQLite,
	"sqlite3":    SelectorSQLite,
	"sqlserver":  SelectorSQLServer,
	"mssql":      SelectorSQLServer,
}

// String returns the canonical selector name.
func (s Selector) String() string {
	switch s {
	case SelectorAny:
		return ""
	case SelectorMySQL:
		return "mysql"
	case SelectorPostgres:
		return "postgres"
	case SelectorSQLite:
		return "sqlite"
	case SelectorSQLServer:
		return "sqlserver"
	default:
		return ""
	}
}

// IsKnown reports whether the selector names an explicit supported database.
func (s Selector) IsKnown() bool {
	switch s {
	case SelectorMySQL, SelectorPostgres, SelectorSQLite, SelectorSQLServer:
		return true
	case SelectorAny:
		return false
	default:
		return false
	}
}

// IsValid reports whether the selector is known or the wildcard value.
func (s Selector) IsValid() bool {
	return s.IsAny() || s.IsKnown()
}

// IsAny reports whether the selector should match every dialect.
func (s Selector) IsAny() bool {
	return s == SelectorAny
}

// ParseSelector parses a canonical or aliased dialect selector name.
func ParseSelector(raw string) (Selector, error) {
	normalized := strings.ToLower(strings.TrimSpace(raw))
	selector, ok := selectorAliases[normalized]
	if !ok {
		return SelectorAny, fmt.Errorf("%w: %q", ErrInvalidSelector, raw)
	}
	return selector, nil
}

// MustSelector panics when ParseSelector fails.
func MustSelector(raw string) Selector {
	selector, err := ParseSelector(raw)
	if err != nil {
		panic(err)
	}
	return selector
}

// SelectorFromContract converts a dialect contract into a canonical selector.
func SelectorFromContract(d Contract) (Selector, error) {
	if d == nil {
		return SelectorAny, fmt.Errorf("%w: <nil>", ErrInvalidSelector)
	}
	return ParseSelector(d.Name())
}
