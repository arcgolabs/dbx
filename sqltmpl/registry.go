package sqltmpl

import (
	"errors"
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
	cache  *hot.HotCache[registryCacheKey, *Template]
}

type registryCacheKey struct {
	name    string
	dialect string
}

// NewRegistry returns a template registry backed by the provided filesystem.
func NewRegistry(fsys fs.FS, d dialect.Contract, opts ...Option) *Registry {
	return &Registry{
		engine: New(d, opts...),
		fsys:   fsys,
		cache:  hot.NewHotCache[registryCacheKey, *Template](hot.LRU, 256).Build(),
	}
}

// Template loads or returns a cached template by name.
func (r *Registry) Template(name string) (*Template, error) {
	return r.TemplateFor(name, registryDialect(r))
}

// TemplateFor loads or returns a cached template by name using d for dialect
// suffix resolution and template rendering.
func (r *Registry) TemplateFor(name string, d dialect.Contract) (*Template, error) {
	if err := r.validateTemplateRequest(d); err != nil {
		return nil, err
	}

	normalized := normalizeTemplateName(name)
	dialectName := sanitizeDialectTemplateSuffix(d.Name())
	cacheKey := registryCacheKey{name: normalized, dialect: dialectName}
	if cached, ok := r.cache.Peek(cacheKey); ok {
		return cached, nil
	}
	resolved, err := r.resolveTemplateName(normalized, dialectName)
	if err != nil {
		return nil, err
	}
	resolvedKey := registryCacheKey{name: resolved, dialect: dialectName}
	if cached, ok := r.cache.Peek(resolvedKey); ok {
		r.cache.Set(cacheKey, cached)
		return cached, nil
	}

	content, err := fs.ReadFile(r.fsys, resolved)
	if err != nil {
		return nil, fmt.Errorf("read template %q: %w", resolved, err)
	}
	template, err := compileTemplate(resolved, string(content), d, r.engine.cfg)
	if err != nil {
		return nil, fmt.Errorf("compile template %q: %w", resolved, err)
	}

	if cached, ok := r.cache.Peek(resolvedKey); ok {
		r.cache.Set(cacheKey, cached)
		return cached, nil
	}
	r.cache.Set(resolvedKey, template)
	r.cache.Set(cacheKey, template)
	return template, nil
}

func (r *Registry) validateTemplateRequest(d dialect.Contract) error {
	if r == nil || r.engine == nil || r.fsys == nil || d == nil {
		return sqlstmt.ErrNilStatement
	}
	return nil
}

// MustTemplate loads a template and panics on error.
func (r *Registry) MustTemplate(name string) *Template {
	template, err := r.Template(name)
	if err != nil {
		panic(err)
	}
	return template
}

// MustTemplateFor loads a template for d and panics on error.
func (r *Registry) MustTemplateFor(name string, d dialect.Contract) *Template {
	template, err := r.TemplateFor(name, d)
	if err != nil {
		panic(err)
	}
	return template
}

// Statement loads or returns a cached statement template by name.
func (r *Registry) Statement(name string) (*Template, error) {
	return r.Template(name)
}

// StatementFor loads or returns a cached statement template by name for d.
func (r *Registry) StatementFor(name string, d dialect.Contract) (*Template, error) {
	return r.TemplateFor(name, d)
}

// MustStatement loads a statement template and panics on error.
func (r *Registry) MustStatement(name string) *Template {
	return r.MustTemplate(name)
}

// MustStatementFor loads a statement template for d and panics on error.
func (r *Registry) MustStatementFor(name string, d dialect.Contract) *Template {
	return r.MustTemplateFor(name, d)
}

// Preload loads and caches the named templates.
func (r *Registry) Preload(names ...string) (*collectionx.List[*Template], error) {
	return r.PreloadFor(registryDialect(r), names...)
}

// PreloadFor loads and caches the named templates for d.
func (r *Registry) PreloadFor(d dialect.Contract, names ...string) (*collectionx.List[*Template], error) {
	if r == nil {
		return nil, sqlstmt.ErrNilStatement
	}
	templates := collectionx.NewListWithCapacity[*Template](len(names))
	for _, name := range names {
		template, err := r.TemplateFor(name, d)
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
	return r.PreloadAllFor(registryDialect(r))
}

// PreloadAllFor loads and caches every .sql template from the registry filesystem for d.
func (r *Registry) PreloadAllFor(d dialect.Contract) (*collectionx.List[*Template], error) {
	names, err := r.Names()
	if err != nil {
		return nil, err
	}
	return r.PreloadFor(d, names.Values()...)
}

// Check loads a template, renders it with params, and collects any available SQL analysis.
func (r *Registry) Check(name string, params any) (CheckReport, error) {
	return r.CheckFor(name, registryDialect(r), params)
}

// CheckFor loads a template for d, renders it with params, and collects SQL analysis.
func (r *Registry) CheckFor(name string, d dialect.Contract, params any) (CheckReport, error) {
	if r == nil {
		report := CheckReport{Stage: CheckStageLoad, Err: sqlstmt.ErrNilStatement}
		return report, report.Err
	}
	template, err := r.TemplateFor(name, d)
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
	return r.CheckAllFor(registryDialect(r), samples)
}

// CheckAllFor loads every .sql template from the registry and checks each for d.
func (r *Registry) CheckAllFor(d dialect.Contract, samples map[string]any) (*collectionx.List[CheckReport], error) {
	names, err := r.Names()
	if err != nil {
		return nil, err
	}

	reports := collectionx.NewListWithCapacity[CheckReport](names.Len())
	names.Range(func(_ int, name string) bool {
		report, checkErr := r.CheckFor(name, d, sampleForTemplate(samples, name))
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

func (r *Registry) resolveTemplateName(name, dialectName string) (string, error) {
	if isDialectTemplateName(name) {
		return name, nil
	}
	if dialectName == "" {
		return name, nil
	}
	for _, candidate := range dialectTemplateNames(name, dialectName) {
		if candidate == name {
			continue
		}
		if _, err := fs.Stat(r.fsys, candidate); err == nil {
			return candidate, nil
		} else if !errors.Is(err, fs.ErrNotExist) {
			return "", fmt.Errorf("stat template %q: %w", candidate, err)
		}
	}
	return name, nil
}

func registryDialect(r *Registry) dialect.Contract {
	if r == nil || r.engine == nil || r.engine.dialect == nil {
		return nil
	}
	return r.engine.dialect
}

func sampleForTemplate(samples map[string]any, name string) any {
	if len(samples) == 0 {
		return nil
	}
	if sample, ok := samples[name]; ok {
		return sample
	}
	if logical := logicalTemplateName(name); logical != name {
		return samples[logical]
	}
	return nil
}
