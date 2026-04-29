package render

import collectionx "github.com/arcgolabs/collectionx/list"

// Result contains rendered SQL text and its bind arguments.
type Result struct {
	Query string
	Args  *collectionx.List[any]
}
