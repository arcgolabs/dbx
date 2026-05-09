package migrate

import (
	"errors"
	"fmt"
	"strings"

	"github.com/arcgolabs/dbx/dialect"
)

// DialectName is a migration dialect selector.
type DialectName uint8

const (
	// DialectAny is the wildcard selector used when no dialect is specified.
	DialectAny DialectName = iota
	// DialectMySQL is the canonical MySQL migration selector.
	DialectMySQL
	// DialectPostgres is the canonical PostgreSQL migration selector.
	DialectPostgres
	// DialectSQLite is the canonical SQLite migration selector.
	DialectSQLite
	// DialectSQLServer is provided for compatibility, though it is not built in.
	DialectSQLServer
)

var (
	errDialectAliases = map[string]DialectName{
		"":           DialectAny,
		"mysql":      DialectMySQL,
		"postgres":   DialectPostgres,
		"postgresql": DialectPostgres,
		"sqlite":     DialectSQLite,
		"sqlite3":    DialectSQLite,
		"sqlserver":  DialectSQLServer,
		"mssql":      DialectSQLServer,
	}

	errInvalidDialect = errors.New("dbx/migrate: unknown migration dialect")
)

// String returns the canonical string representation for a migration dialect name.
func (d DialectName) String() string {
	switch d {
	case DialectAny:
		return ""
	case DialectMySQL:
		return "mysql"
	case DialectPostgres:
		return "postgres"
	case DialectSQLite:
		return "sqlite"
	case DialectSQLServer:
		return "sqlserver"
	default:
		return ""
	}
}

// IsKnown reports whether the dialect is an explicit supported database name.
func (d DialectName) IsKnown() bool {
	switch d {
	case DialectAny:
		return false
	case DialectMySQL, DialectPostgres, DialectSQLite, DialectSQLServer:
		return true
	default:
		return false
	}
}

// IsValid reports whether the value is either a known dialect or the wildcard.
func (d DialectName) IsValid() bool {
	return d.IsKnown() || d.IsAny()
}

// IsAny reports whether the selector should match all migration dialects.
func (d DialectName) IsAny() bool {
	return d == DialectAny
}

// ParseDialectName parses a canonical or aliased dialect name.
func ParseDialectName(raw string) (DialectName, error) {
	norm := strings.ToLower(strings.TrimSpace(raw))
	dialectName, ok := errDialectAliases[norm]
	if !ok {
		return DialectAny, fmt.Errorf("%w: %q", errInvalidDialect, raw)
	}
	return dialectName, nil
}

// MustDialectName panics when ParseDialectName fails.
func MustDialectName(raw string) DialectName {
	dialectName, err := ParseDialectName(raw)
	if err != nil {
		panic(err)
	}
	return dialectName
}

// DialectFromDialect converts a query dialect to a migration selector.
func DialectFromDialect(d dialect.Contract) (DialectName, error) {
	if d == nil {
		return DialectAny, fmt.Errorf("%w: <nil>", errInvalidDialect)
	}
	return ParseDialectName(d.Name())
}
