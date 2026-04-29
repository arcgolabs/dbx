package migrate

import collectionx "github.com/arcgolabs/collectionx/list"

// RunReport describes migrations applied by a runner operation.
type RunReport struct {
	Applied *collectionx.List[AppliedRecord]
}
