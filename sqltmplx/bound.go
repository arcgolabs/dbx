package sqltmplx

import collectionx "github.com/arcgolabs/collectionx/list"

// BoundSQL contains rendered SQL text and its bind arguments.
type BoundSQL struct {
	Query string
	Args  *collectionx.List[any]
}
