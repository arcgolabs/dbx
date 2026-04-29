package sqltmplx

import (
	"fmt"
	"io/fs"
	"path"
	"slices"
	"strings"

	collectionx "github.com/arcgolabs/collectionx/list"
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

// Preload loads and caches the named templates.
func (r *Registry) Preload(names ...string) (*collectionx.List[*Template], error) {
	if r == nil {
		return nil, sqlstmt.ErrNilStatement
	}
	templates := collectionx.NewListWithCapacity[*Template](len(names))
	for _, name := range names {
		template, err := r.Template(name)
		if err != nil {
			return nil, err
		}
		templates.Add(template)
	}
	return templates, nil
}

// Names returns sorted template paths from the registry filesystem.
func (r *Registry) Names() (*collectionx.List[string], error) {
	if r == nil || r.fsys == nil {
		return nil, sqlstmt.ErrNilStatement
	}

	names := collectionx.NewList[string]()
	if err := fs.WalkDir(r.fsys, ".", func(name string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || !strings.HasSuffix(strings.ToLower(name), ".sql") {
			return nil
		}
		names.Add(normalizeTemplateName(name))
		return nil
	}); err != nil {
		return nil, fmt.Errorf("walk templates: %w", err)
	}

	values := names.Values()
	slices.Sort(values)
	return collectionx.NewList(values...), nil
}

// PreloadAll loads and caches every .sql template from the registry filesystem.
func (r *Registry) PreloadAll() (*collectionx.List[*Template], error) {
	names, err := r.Names()
	if err != nil {
		return nil, err
	}
	return r.Preload(names.Values()...)
}

// Check loads a template, renders it with params, and collects any available SQL analysis.
func (r *Registry) Check(name string, params any) (CheckReport, error) {
	if r == nil {
		report := CheckReport{Stage: CheckStageLoad, Err: sqlstmt.ErrNilStatement}
		return report, report.Err
	}
	template, err := r.Template(name)
	if err != nil {
		report := CheckReport{
			Name:           normalizeTemplateName(name),
			Stage:          CheckStageLoad,
			SampleProvided: params != nil,
			Err:            err,
		}
		return report, err
	}
	return template.Check(params)
}

// CheckAll loads every .sql template from the registry and checks each using samples[name].
func (r *Registry) CheckAll(samples map[string]any) (*collectionx.List[CheckReport], error) {
	names, err := r.Names()
	if err != nil {
		return nil, err
	}

	reports := collectionx.NewListWithCapacity[CheckReport](names.Len())
	names.Range(func(_ int, name string) bool {
		report, checkErr := r.Check(name, samples[name])
		if checkErr != nil {
			report.Err = checkErr
		}
		reports.Add(report)
		return true
	})
	return reports, nil
}

func normalizeTemplateName(name string) string {
	normalized := path.Clean(strings.TrimSpace(name))
	return strings.TrimPrefix(normalized, "/")
}
