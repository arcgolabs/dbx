package sqlstmt

import "github.com/DaiYuANg/arcgo/collectionx"

// Bound is a SQL statement with bind arguments ready for execution.
type Bound struct {
	Name         string
	SQL          string
	Args         collectionx.List[any]
	CapacityHint int // when >0, hint for pre-allocating result slice (e.g. from LIMIT)
}
