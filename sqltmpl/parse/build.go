package parse

import (
	"errors"
	"fmt"

	collectionx "github.com/arcgolabs/collectionx/list"
	"github.com/arcgolabs/dbx/sqltmpl/scan"
	"github.com/expr-lang/expr"
)

var (
	errUnexpectedEnd = errors.New("sqltmpl: unexpected end")
	errUnclosedBlock = errors.New("sqltmpl: unclosed block")
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
func BuildList(tokens *collectionx.List[scan.Token]) (*collectionx.List[Node], error) {
	nodes := collectionx.NewList[Node]()
	stack := collectionx.NewListWithCapacity[frame](4, frame{kind: frameRoot, out: nodes})

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

func consumeToken(tok scan.Token, stack *collectionx.List[frame], appendNode func(Node)) error {
	switch tok.Kind {
	case scan.Text:
		nodes, err := compileTextToken(tok)
		if err != nil {
			return err
		}
		nodes.Range(func(_ int, node Node) bool {
			appendNode(node)
			return true
		})
		return nil
	case scan.Directive:
		return consumeDirective(tok, stack, appendNode)
	default:
		return nil
	}
}

func consumeDirective(tok scan.Token, stack *collectionx.List[frame], appendNode func(Node)) error {
	directive, err := parseDirective(tok.Value)
	if err != nil {
		return wrapParseError(tok.Span.Start, err)
	}

	switch {
	case directive.If != nil:
		return pushIfNode(directive.If, tok.Span, stack, appendNode)
	case directive.Where != nil:
		pushWhereNode(tok.Span, stack, appendNode)
		return nil
	case directive.Set != nil:
		pushSetNode(tok.Span, stack, appendNode)
		return nil
	case directive.End != nil:
		return popFrame(tok.Span.Start, stack)
	default:
		return nil
	}
}

func pushIfNode(directive *IfDirective, span scan.Span, stack *collectionx.List[frame], appendNode func(Node)) error {
	program, err := expr.Compile(directive.Expr)
	if err != nil {
		return wrapParseError(span.Start, fmt.Errorf("sqltmpl: compile expr %q: %w", directive.Expr, err))
	}

	node := &IfNode{RawExpr: directive.Expr, Program: program, Body: collectionx.NewList[Node](), Span: span}
	appendNode(node)
	stack.Add(frame{kind: frameIf, out: node.Body})
	return nil
}

func pushWhereNode(span scan.Span, stack *collectionx.List[frame], appendNode func(Node)) {
	node := &WhereNode{Body: collectionx.NewList[Node](), Span: span}
	appendNode(node)
	stack.Add(frame{kind: frameWhere, out: node.Body})
}

func pushSetNode(span scan.Span, stack *collectionx.List[frame], appendNode func(Node)) {
	node := &SetNode{Body: collectionx.NewList[Node](), Span: span}
	appendNode(node)
	stack.Add(frame{kind: frameSet, out: node.Body})
}

func popFrame(position scan.Position, stack *collectionx.List[frame]) error {
	if stack.Len() == 1 {
		return wrapParseError(position, errUnexpectedEnd)
	}
	_, _ = stack.RemoveAt(stack.Len() - 1)
	return nil
}

func appendFrameNode(stack *collectionx.List[frame], node Node) {
	current, _ := stack.Get(stack.Len() - 1)
	if current.out == nil {
		return
	}
	current.out.Add(node)
}

func wrapParseError(position scan.Position, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%w at %d:%d", err, position.Line, position.Column)
}
