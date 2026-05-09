package migrate

import (
	"errors"
	"fmt"

	"github.com/arcgolabs/dbx/dialect"
)

// DialectName is a migration dialect selector.
type DialectName = dialect.Selector

const (
	// DialectAny is the wildcard selector used when no dialect is specified.
	DialectAny DialectName = dialect.SelectorAny
	// DialectMySQL is the canonical MySQL migration selector.
	DialectMySQL DialectName = dialect.SelectorMySQL
	// DialectPostgres is the canonical PostgreSQL migration selector.
	DialectPostgres DialectName = dialect.SelectorPostgres
	// DialectSQLite is the canonical SQLite migration selector.
	DialectSQLite DialectName = dialect.SelectorSQLite
	// DialectSQLServer is provided for compatibility, though it is not built in.
	DialectSQLServer DialectName = dialect.SelectorSQLServer
)

var errInvalidDialect = errors.New("dbx/migrate: unknown migration dialect")

// ParseDialectName parses a canonical or aliased dialect name.
func ParseDialectName(raw string) (DialectName, error) {
	selector, err := dialect.ParseSelector(raw)
	if err != nil {
		return DialectAny, fmt.Errorf("%w: %q", errInvalidDialect, raw)
	}
	return selector, nil
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
	selector, err := dialect.SelectorFromContract(d)
	if err != nil {
		return DialectAny, fmt.Errorf("%w: %v", errInvalidDialect, err)
	}
	return selector, nil
}
