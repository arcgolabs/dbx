package sqltmpl

import (
	"strings"

	"github.com/arcgolabs/dbx/paging"
	"github.com/arcgolabs/dbx/sqlstmt"
	"github.com/arcgolabs/dbx/sqltmpl/render"
)

type pageParamOverlay interface {
	LookupSQLTemplateParam(string) (any, bool)
	SQLTemplateParamEnv() map[string]any
}

// PageParams carries typed template parameters plus normalized pagination.
type PageParams[P any] struct {
	Params P
	Page   paging.Request

	overlay pageParamOverlay
}

// Page creates a normalized page request.
func Page(page, pageSize int) paging.Request {
	return paging.Page(page, pageSize)
}

// NewPageRequest creates a normalized page request.
func NewPageRequest(page, pageSize int) paging.Request {
	return paging.NewRequest(page, pageSize)
}

// WithPage overlays a normalized paging.Request under the Page template parameter.
func WithPage(params any, request paging.Request) any {
	return render.WithParam(params, "Page", request.Normalize())
}

// WithTypedPage overlays a normalized paging.Request while preserving the base params type.
func WithTypedPage[P any](params P, request paging.Request) PageParams[P] {
	request = request.Normalize()
	return PageParams[P]{
		Params:  params,
		Page:    request,
		overlay: newPageParamOverlay(params, request),
	}
}

// LookupSQLTemplateParam resolves Page or a field from the wrapped params.
func (p PageParams[P]) LookupSQLTemplateParam(name string) (any, bool) {
	return p.resolvedOverlay().LookupSQLTemplateParam(name)
}

// SQLTemplateParamEnv returns expression variables for the wrapped params and Page.
func (p PageParams[P]) SQLTemplateParamEnv() map[string]any {
	return p.resolvedOverlay().SQLTemplateParamEnv()
}

func (p PageParams[P]) resolvedOverlay() pageParamOverlay {
	if p.overlay != nil {
		return p.overlay
	}
	return newPageParamOverlay(p.Params, p.Page)
}

func newPageParamOverlay(params any, request paging.Request) pageParamOverlay {
	request = request.Normalize()
	overlay, ok := render.WithParam(params, "Page", request).(pageParamOverlay)
	if ok {
		return overlay
	}
	return staticPageParams{page: request}
}

type staticPageParams struct {
	page paging.Request
}

func (p staticPageParams) LookupSQLTemplateParam(name string) (any, bool) {
	if strings.EqualFold(strings.TrimSpace(name), "Page") {
		return p.page, true
	}
	return nil, false
}

func (p staticPageParams) SQLTemplateParamEnv() map[string]any {
	return map[string]any{"Page": p.page, "page": p.page}
}

// RenderPage renders the template with normalized pagination parameters.
func (t *Template) RenderPage(params any, request paging.Request) (BoundSQL, error) {
	return t.Render(WithPage(params, request))
}

// BindPage renders the template with normalized pagination parameters into a bound SQL statement.
func (t *Template) BindPage(params any, request paging.Request) (sqlstmt.Bound, error) {
	request = request.Normalize()
	bound, err := t.Bind(WithPage(params, request))
	if err != nil {
		return sqlstmt.Bound{}, err
	}
	bound.CapacityHint = request.Limit()
	return bound, nil
}

// RenderPage compiles and renders a template with normalized pagination parameters.
func (e *Engine) RenderPage(tpl string, params any, request paging.Request) (BoundSQL, error) {
	return e.Render(tpl, WithPage(params, request))
}
