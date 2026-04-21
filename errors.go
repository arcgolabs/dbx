package dbx

import (
	"errors"
	"fmt"
)

var (
	ErrNilDB                     = errors.New("dbx: db is nil")
	ErrNilSQLDB                  = errors.New("dbx: sql.DB is nil")
	ErrMissingDriver             = errors.New("dbx: Open requires WithDriver")
	ErrMissingDSN                = errors.New("dbx: Open requires WithDSN")
	ErrMissingDialect            = errors.New("dbx: Open requires WithDialect")
	ErrIDGeneratorNodeIDConflict = errors.New("dbx: WithIDGenerator and WithNodeID cannot be used together")
	ErrNilDialect                = errors.New("dbx: dialect is nil")
	ErrNilQuery                  = errors.New("dbx: query is nil")
	ErrNilMapper                 = errors.New("dbx: mapper is nil")
	ErrNilRow                    = errors.New("dbx: row is nil")
	ErrRelationCardinality       = errors.New("dbx: relation cardinality violation")
	ErrUnsupportedSchema         = errors.New("dbx: schema type is unsupported")
)

// RelationCardinalityError reports when a relation declared as one-to-one
// resolves to multiple rows for the same source key.
type RelationCardinalityError struct {
	Relation string
	Key      any
	Count    int
}

func (e *RelationCardinalityError) Error() string {
	switch {
	case e == nil:
		return "dbx: relation cardinality violation"
	case e.Relation != "" && e.Count > 0:
		return fmt.Sprintf("dbx: relation %q expected at most one row for key %v, got %d", e.Relation, e.Key, e.Count)
	case e.Relation != "":
		return fmt.Sprintf("dbx: relation %q violated one-to-one cardinality", e.Relation)
	default:
		return "dbx: relation cardinality violation"
	}
}

func (e *RelationCardinalityError) Unwrap() error {
	return ErrRelationCardinality
}
