package parse

import (
	"github.com/DaiYuANg/arcgo/collectionx"
	"github.com/expr-lang/expr/vm"
)

// Node represents a parsed template node.
type Node interface {
	node()
}

// TextNode stores literal SQL text.
type TextNode struct {
	Text string
}

func (TextNode) node() {}

// IfNode stores a conditional block.
type IfNode struct {
	RawExpr string
	Program *vm.Program
	Body    collectionx.List[Node]
}

func (*IfNode) node() {}

// WhereNode stores a conditional WHERE block.
type WhereNode struct {
	Body collectionx.List[Node]
}

func (*WhereNode) node() {}

// SetNode stores a conditional SET block.
type SetNode struct {
	Body collectionx.List[Node]
}

func (*SetNode) node() {}
