package render

import (
	"strings"

	"github.com/arcgolabs/collectionx"
)

type paramLookup interface {
	LookupSQLTemplateParam(name string) (any, bool)
}

type paramEnv interface {
	SQLTemplateParamEnv() map[string]any
}

type overlayParam struct {
	name  string
	value any
}

type paramOverlay struct {
	base   any
	values collectionx.Map[string, overlayParam]
}

// WithParam overlays one top-level parameter onto an existing template parameter object.
func WithParam(params any, name string, value any) any {
	key := normalizeParamName(name)
	if key == "" {
		return params
	}
	values := collectionx.NewMap[string, overlayParam]()
	values.Set(key, overlayParam{name: strings.TrimSpace(name), value: value})
	return paramOverlay{base: params, values: values}
}

// WithParams overlays top-level parameters onto an existing template parameter object.
func WithParams(params any, values collectionx.Map[string, any]) any {
	filtered := collectionx.NewMapWithCapacity[string, overlayParam](values.Len())
	values.Range(func(name string, value any) bool {
		key := normalizeParamName(name)
		if key != "" {
			filtered.Set(key, overlayParam{name: strings.TrimSpace(name), value: value})
		}
		return true
	})
	if filtered.Len() == 0 {
		return params
	}
	return paramOverlay{base: params, values: filtered}
}

func (p paramOverlay) LookupSQLTemplateParam(name string) (any, bool) {
	if param, ok := p.values.Get(normalizeParamName(name)); ok {
		return param.value, true
	}
	return lookupOneValue(p.base, name)
}

func (p paramOverlay) SQLTemplateParamEnv() map[string]any {
	env := envMap(p.base)
	p.values.Range(func(key string, param overlayParam) bool {
		env[key] = param.value
		if param.name != "" {
			env[param.name] = param.value
		}
		return true
	})
	return env
}

func normalizeParamName(name string) string {
	return strings.ToLower(strings.TrimSpace(name))
}
