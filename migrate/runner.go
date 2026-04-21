package migrate

import "github.com/DaiYuANg/arcgo/collectionx"

// RunReport describes migrations applied by a runner operation.
type RunReport struct {
	Applied collectionx.List[AppliedRecord]
}
