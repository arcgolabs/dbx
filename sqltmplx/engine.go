package sqltmplx

import (
	"fmt"

	"github.com/arcgolabs/dbx/dialect"
	"github.com/samber/hot"
	"github.com/samber/lo"
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
	lo.ForEach(opts, func(opt Option, _ int) {
		if opt != nil {
			opt(&cfg)
		}
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

// Render compiles and renders a template with the provided parameters.
func (e *Engine) Render(tpl string, params any) (BoundSQL, error) {
	t, err := e.Compile(tpl)
	if err != nil {
		return BoundSQL{}, fmt.Errorf("compile template: %w", err)
	}
	return t.Render(params)
}
