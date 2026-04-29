package sqltmplx

import (
	"fmt"

	collectionx "github.com/arcgolabs/collectionx/list"
	"github.com/arcgolabs/dbx/dialect"
	"github.com/samber/hot"
)

// Engine compiles and renders SQL templates for a dialect.
type Engine struct {
	dialect       dialect.Contract
	cfg           config
	templateCache *hot.HotCache[templateCacheKey, *Template]
}

type templateCacheKey struct {
	name string
	text string
}

// New returns a template engine configured for the provided dialect.
func New(d dialect.Contract, opts ...Option) *Engine {
	cfg := defaultConfig()
	collectionx.NewList[Option](opts...).Range(func(_ int, opt Option) bool {
		if opt != nil {
			opt(&cfg)
		}
		return true
	})
	return &Engine{dialect: d, cfg: cfg, templateCache: newTemplateCache(cfg.templateCacheSize)}
}

func newTemplateCache(size int) *hot.HotCache[templateCacheKey, *Template] {
	if size <= 0 {
		return nil
	}
	return hot.NewHotCache[templateCacheKey, *Template](hot.LRU, size).Build()
}

// Compile compiles an unnamed template.
func (e *Engine) Compile(tpl string) (*Template, error) {
	return e.CompileNamed("", tpl)
}

// CompileNamed compiles a named template.
func (e *Engine) CompileNamed(name, tpl string) (*Template, error) {
	key := templateCacheKey{name: name, text: tpl}
	if e.templateCache != nil {
		if cached, ok := e.templateCache.Peek(key); ok {
			return cached, nil
		}
	}
	compiled, err := compileTemplate(name, tpl, e.dialect, e.cfg)
	if err != nil {
		return nil, err
	}
	if e.templateCache != nil {
		if cached, ok := e.templateCache.Peek(key); ok {
			return cached, nil
		}
		e.templateCache.Set(key, compiled)
	}
	return compiled, nil
}

// Analyze compiles a template and returns its static metadata.
func (e *Engine) Analyze(tpl string) (TemplateMetadata, error) {
	return e.AnalyzeNamed("", tpl)
}

// AnalyzeNamed compiles a named template and returns its static metadata.
func (e *Engine) AnalyzeNamed(name, tpl string) (TemplateMetadata, error) {
	t, err := e.CompileNamed(name, tpl)
	if err != nil {
		return TemplateMetadata{}, fmt.Errorf("compile template: %w", err)
	}
	return t.Metadata(), nil
}

// Render compiles and renders a template with the provided parameters.
func (e *Engine) Render(tpl string, params any) (BoundSQL, error) {
	t, err := e.Compile(tpl)
	if err != nil {
		return BoundSQL{}, fmt.Errorf("compile template: %w", err)
	}
	return t.Render(params)
}

// Check compiles, renders, validates, and analyzes a template.
func (e *Engine) Check(tpl string, params any) (CheckReport, error) {
	return e.CheckNamed("", tpl, params)
}

// CheckNamed compiles, renders, validates, and analyzes a named template.
func (e *Engine) CheckNamed(name, tpl string, params any) (CheckReport, error) {
	t, err := e.CompileNamed(name, tpl)
	if err != nil {
		report := CheckReport{
			Name:           name,
			Dialect:        e.dialect.Name(),
			Stage:          CheckStageCompile,
			SampleProvided: params != nil,
			Err:            fmt.Errorf("compile template check: %w", err),
		}
		return report, report.Err
	}
	return t.Check(params)
}
