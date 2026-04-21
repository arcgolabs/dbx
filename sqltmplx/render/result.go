package render

import "github.com/DaiYuANg/arcgo/collectionx"

// Result contains rendered SQL text and its bind arguments.
type Result struct {
	Query string
	Args  collectionx.List[any]
}
