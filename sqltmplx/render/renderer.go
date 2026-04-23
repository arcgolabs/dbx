package render

import (
	"errors"
	"fmt"
	"strings"

	"github.com/arcgolabs/collectionx"
	"github.com/arcgolabs/dbx/dialect"
	"github.com/arcgolabs/dbx/sqltmplx/parse"
	"github.com/expr-lang/expr/vm"
)

var errIfExpressionNotBool = errors.New("sqltmplx: if expression must return bool")

// Render renders parsed template nodes into SQL.
func Render(nodes []parse.Node, params any, d dialect.Contract) (Result, error) {
	return RenderList(collectionx.NewListWithCapacity[parse.Node](len(nodes), nodes...), params, d)
}

// RenderList renders parsed template nodes from a collectionx.List into SQL.
func RenderList(nodes collectionx.List[parse.Node], params any, d dialect.Contract) (Result, error) {
	st := newState(params, d)
	query, err := renderNodes(nodes, st)
	if err != nil {
		return Result{}, err
	}
	return Result{Query: compactWhitespace(query), Args: st.args}, nil
}

func renderNodes(nodes collectionx.List[parse.Node], st *state) (string, error) {
	var sb strings.Builder
	var renderErr error
	nodes.Range(func(_ int, node parse.Node) bool {
		text, err := renderNode(node, st)
		if err != nil {
			renderErr = err
			return false
		}
		writeBuilderString(&sb, text)
		return true
	})
	if renderErr != nil {
		return "", renderErr
	}
	return sb.String(), nil
}

func renderNode(node parse.Node, st *state) (string, error) {
	switch typed := node.(type) {
	case parse.TextNode:
		return bindText(typed.Text, st)
	case *parse.IfNode:
		return renderIfNode(typed, st)
	case *parse.WhereNode:
		return renderCleanedBlock(typed.Body, st, cleanupWhere)
	case *parse.SetNode:
		return renderCleanedBlock(typed.Body, st, cleanupSet)
	default:
		return "", fmt.Errorf("sqltmplx: unsupported node %T", node)
	}
}

func renderIfNode(node *parse.IfNode, st *state) (string, error) {
	ok, err := evalIf(node.Program, st)
	if err != nil {
		return "", err
	}
	if !ok {
		return "", nil
	}
	return renderNodes(node.Body, st)
}

func renderCleanedBlock(body collectionx.List[parse.Node], st *state, cleanup func(string) string) (string, error) {
	text, err := renderNodes(body, st)
	if err != nil {
		return "", err
	}
	cleaned := cleanup(text)
	if cleaned == "" {
		return "", nil
	}
	return " " + cleaned + " ", nil
}

func evalIf(program *vm.Program, st *state) (bool, error) {
	out, err := exprRun(program, st.exprEnv())
	if err != nil {
		return false, err
	}
	b, ok := out.(bool)
	if !ok {
		return false, errIfExpressionNotBool
	}
	return b, nil
}

func writeBuilderString(builder *strings.Builder, value string) {
	if _, err := builder.WriteString(value); err != nil {
		panic(err)
	}
}
