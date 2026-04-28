package sqltmplx

import (
	"fmt"

	"github.com/arcgolabs/collectionx"
	"github.com/arcgolabs/dbx/dialect"
	"github.com/arcgolabs/dbx/sqlstmt"
	"github.com/arcgolabs/dbx/sqltmplx/parse"
	"github.com/arcgolabs/dbx/sqltmplx/render"
	"github.com/arcgolabs/dbx/sqltmplx/scan"
	"github.com/arcgolabs/dbx/sqltmplx/validate"
)

// Template stores a compiled SQL template.
type Template struct {
	name      string
	nodes     collectionx.List[parse.Node]
	metadata  TemplateMetadata
	dialect   dialect.Contract
	validator validate.Validator
}

func compileTemplate(name, tpl string, d dialect.Contract, cfg config) (*Template, error) {
	tokens, err := scan.ScanList(tpl)
	if err != nil {
		return nil, fmt.Errorf("scan template: %w", err)
	}
	nodes, err := parse.BuildList(tokens)
	if err != nil {
		return nil, fmt.Errorf("build template nodes: %w", err)
	}
	return &Template{
		name:      name,
		nodes:     nodes,
		metadata:  buildTemplateMetadata(nodes),
		dialect:   d,
		validator: cfg.validator,
	}, nil
}

// StatementName returns the template statement name.
func (t *Template) StatementName() string {
	if t == nil {
		return ""
	}
	return t.name
}

// Metadata returns a copy of the template's static metadata.
func (t *Template) Metadata() TemplateMetadata {
	if t == nil {
		return TemplateMetadata{
			Parameters:       collectionx.NewList[string](),
			SpreadParameters: collectionx.NewList[string](),
			Conditions:       collectionx.NewList[string](),
		}
	}
	return cloneTemplateMetadata(t.metadata)
}

// Render renders the template into SQL and bind arguments.
func (t *Template) Render(params any) (BoundSQL, error) {
	bound, err := render.RenderList(t.nodes, params, t.dialect)
	if err != nil {
		return BoundSQL{}, fmt.Errorf("render template: %w", err)
	}
	if t.validator != nil {
		if err := t.validator.Validate(bound.Query); err != nil {
			return BoundSQL{}, fmt.Errorf("validate rendered sql: %w", err)
		}
	}
	return BoundSQL{Query: bound.Query, Args: bound.Args}, nil
}

// Bind renders the template into a dbx bound query.
func (t *Template) Bind(params any) (sqlstmt.Bound, error) {
	if t == nil {
		return sqlstmt.Bound{}, sqlstmt.ErrNilStatement
	}

	bound, err := t.Render(params)
	if err != nil {
		return sqlstmt.Bound{}, fmt.Errorf("render bound query: %w", err)
	}
	return sqlstmt.Bound{
		Name: t.name,
		SQL:  bound.Query,
		Args: bound.Args,
	}, nil
}
