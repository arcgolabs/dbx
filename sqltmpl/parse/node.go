package parse

import (
	collectionx "github.com/arcgolabs/collectionx/list"
	"github.com/arcgolabs/dbx/sqltmpl/scan"
	"github.com/expr-lang/expr/vm"
)

// Node represents a parsed template node.
type Node interface {
	node()
}

// TextNode stores literal SQL text.
type TextNode struct {
	Text string
	Span scan.Span
}

func (TextNode) node() {}

// ParamNode stores a compiled placeholder parameter.
type ParamNode struct {
	Name   string
	Spread bool
	Span   scan.Span
}

func (ParamNode) node() {}

// IfNode stores a conditional block.
type IfNode struct {
	RawExpr string
	Program *vm.Program
	Body    *collectionx.List[Node]
	Span    scan.Span
}

func (*IfNode) node() {}

// WhereNode stores a conditional WHERE block.
type WhereNode struct {
	Body *collectionx.List[Node]
	Span scan.Span
}

func (*WhereNode) node() {}

// SetNode stores a conditional SET block.
type SetNode struct {
	Body *collectionx.List[Node]
	Span scan.Span
}

func (*SetNode) node() {}
