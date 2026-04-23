package sqltmplx

import "github.com/arcgolabs/collectionx"

// BoundSQL contains rendered SQL text and its bind arguments.
type BoundSQL struct {
	Query string
	Args  collectionx.List[any]
}
