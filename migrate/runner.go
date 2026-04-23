package migrate

import "github.com/arcgolabs/collectionx"

// RunReport describes migrations applied by a runner operation.
type RunReport struct {
	Applied collectionx.List[AppliedRecord]
}
