package sqltmplx

import (
	"fmt"
	"io/fs"
	"path"
	"strings"

	"github.com/arcgolabs/dbx/dialect"
	"github.com/arcgolabs/dbx/sqlstmt"
	"github.com/samber/hot"
)

// Registry loads and caches named SQL templates from a filesystem.
type Registry struct {
	engine *Engine
	fsys   fs.FS
	cache  *hot.HotCache[string, *Template]
}

// NewRegistry returns a template registry backed by the provided filesystem.
func NewRegistry(fsys fs.FS, d dialect.Contract, opts ...Option) *Registry {
	return &Registry{
		engine: New(d, opts...),
		fsys:   fsys,
		cache:  hot.NewHotCache[string, *Template](hot.LRU, 256).Build(),
	}
}

// Template loads or returns a cached template by name.
func (r *Registry) Template(name string) (*Template, error) {
	if r == nil || r.engine == nil || r.fsys == nil {
		return nil, sqlstmt.ErrNilStatement
	}

	normalized := normalizeTemplateName(name)
	if cached, ok := r.cache.Peek(normalized); ok {
		return cached, nil
	}

	content, err := fs.ReadFile(r.fsys, normalized)
	if err != nil {
		return nil, fmt.Errorf("read template %q: %w", normalized, err)
	}
	template, err := r.engine.CompileNamed(normalized, string(content))
	if err != nil {
		return nil, fmt.Errorf("compile template %q: %w", normalized, err)
	}

	if cached, ok := r.cache.Peek(normalized); ok {
		return cached, nil
	}
	r.cache.Set(normalized, template)
	return template, nil
}

// MustTemplate loads a template and panics on error.
func (r *Registry) MustTemplate(name string) *Template {
	template, err := r.Template(name)
	if err != nil {
		panic(err)
	}
	return template
}

// Statement loads or returns a cached statement template by name.
func (r *Registry) Statement(name string) (*Template, error) {
	return r.Template(name)
}

// MustStatement loads a statement template and panics on error.
func (r *Registry) MustStatement(name string) *Template {
	return r.MustTemplate(name)
}

func normalizeTemplateName(name string) string {
	normalized := path.Clean(strings.TrimSpace(name))
	return strings.TrimPrefix(normalized, "/")
}
