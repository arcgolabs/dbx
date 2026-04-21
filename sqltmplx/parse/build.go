package parse

import (
	"errors"
	"fmt"

	"github.com/DaiYuANg/arcgo/collectionx"
	"github.com/arcgolabs/dbx/sqltmplx/scan"
	"github.com/expr-lang/expr"
)

var (
	errUnexpectedEnd = errors.New("sqltmplx: unexpected end")
	errUnclosedBlock = errors.New("sqltmplx: unclosed block")
)

type frameKind int

const (
	frameRoot frameKind = iota
	frameIf
	frameWhere
	frameSet
)

type frame struct {
	kind frameKind
	out  *collectionx.List[Node]
}

// Build converts scanned tokens into a parse tree.
func Build(tokens []scan.Token) ([]Node, error) {
	nodes, err := BuildList(collectionx.NewList[scan.Token](tokens...))
	if err != nil {
		return nil, err
	}
	return nodes.Values(), nil
}

// BuildList converts scanned tokens into a parse tree as a collectionx.List.
func BuildList(tokens collectionx.List[scan.Token]) (collectionx.List[Node], error) {
	nodes := collectionx.NewList[Node]()
	stack := collectionx.NewListWithCapacity[frame](4, frame{kind: frameRoot, out: &nodes})

	appendNode := func(n Node) {
		appendFrameNode(stack, n)
	}

	var buildErr error
	tokens.Range(func(_ int, token scan.Token) bool {
		if err := consumeToken(token, stack, appendNode); err != nil {
			buildErr = err
			return false
		}
		return true
	})
	if buildErr != nil {
		return nil, buildErr
	}

	if stack.Len() != 1 {
		return nil, errUnclosedBlock
	}
	return nodes, nil
}

func consumeToken(tok scan.Token, stack collectionx.List[frame], appendNode func(Node)) error {
	switch tok.Kind {
	case scan.Text:
		appendNode(TextNode{Text: tok.Value})
		return nil
	case scan.Directive:
		return consumeDirective(tok.Value, stack, appendNode)
	default:
		return nil
	}
}

func consumeDirective(value string, stack collectionx.List[frame], appendNode func(Node)) error {
	directive, err := parseDirective(value)
	if err != nil {
		return err
	}

	switch {
	case directive.If != nil:
		return pushIfNode(directive.If, stack, appendNode)
	case directive.Where != nil:
		pushWhereNode(stack, appendNode)
		return nil
	case directive.Set != nil:
		pushSetNode(stack, appendNode)
		return nil
	case directive.End != nil:
		return popFrame(stack)
	default:
		return nil
	}
}

func pushIfNode(directive *IfDirective, stack collectionx.List[frame], appendNode func(Node)) error {
	program, err := expr.Compile(directive.Expr)
	if err != nil {
		return fmt.Errorf("sqltmplx: compile expr %q: %w", directive.Expr, err)
	}

	node := &IfNode{RawExpr: directive.Expr, Program: program}
	appendNode(node)
	stack.Add(frame{kind: frameIf, out: &node.Body})
	return nil
}

func pushWhereNode(stack collectionx.List[frame], appendNode func(Node)) {
	node := &WhereNode{}
	appendNode(node)
	stack.Add(frame{kind: frameWhere, out: &node.Body})
}

func pushSetNode(stack collectionx.List[frame], appendNode func(Node)) {
	node := &SetNode{}
	appendNode(node)
	stack.Add(frame{kind: frameSet, out: &node.Body})
}

func popFrame(stack collectionx.List[frame]) error {
	if stack.Len() == 1 {
		return errUnexpectedEnd
	}
	_, _ = stack.RemoveAt(stack.Len() - 1)
	return nil
}

func appendFrameNode(stack collectionx.List[frame], node Node) {
	current, _ := stack.Get(stack.Len() - 1)
	out := current.out
	if *out == nil {
		*out = collectionx.NewList[Node]()
	}
	(*out).Add(node)
}
