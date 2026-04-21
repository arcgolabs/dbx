package sqltmplx

import "github.com/arcgolabs/dbx/sqltmplx/validate"

// Option configures the template engine.
type Option func(*config)

const defaultTemplateCacheSize = 128

type config struct {
	validator         validate.Validator
	templateCacheSize int
}

func defaultConfig() config {
	return config{templateCacheSize: defaultTemplateCacheSize}
}

// WithValidator configures SQL validation for rendered templates.
func WithValidator(v validate.Validator) Option {
	return func(c *config) {
		c.validator = v
	}
}

// WithTemplateCacheSize configures the Engine's compiled-template LRU cache.
// A size <= 0 disables caching.
func WithTemplateCacheSize(size int) Option {
	return func(c *config) {
		c.templateCacheSize = size
	}
}
