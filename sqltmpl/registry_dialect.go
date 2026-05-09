package sqltmpl

import (
	"path"
	"strings"

	collectionx "github.com/arcgolabs/collectionx/list"
)

var dialectTemplateSuffixAliases = map[string][]string{
	"postgres":   {"postgres", "postgresql"},
	"postgresql": {"postgresql", "postgres"},
	"sqlite":     {"sqlite", "sqlite3"},
	"sqlite3":    {"sqlite3", "sqlite"},
	"sqlserver":  {"sqlserver", "mssql"},
	"mssql":      {"mssql", "sqlserver"},
}

func dialectTemplateNames(name, dialectName string) []string {
	suffixes := dialectTemplateSuffixes(dialectName)
	if len(suffixes) == 0 {
		return []string{name}
	}
	dir, file := path.Split(name)
	ext := path.Ext(file)
	stem := strings.TrimSuffix(file, ext)
	if stem == "" {
		return []string{name}
	}
	return collectionx.FlatMapList[string, string](collectionx.NewList[string](suffixes...), func(_ int, suffix string) []string {
		return []string{
			dir + stem + "_" + suffix + ext,
			dir + stem + "__" + suffix + ext,
		}
	}).Values()
}

func isDialectTemplateName(name string) bool {
	_, file := path.Split(name)
	ext := path.Ext(file)
	stem := strings.TrimSuffix(file, ext)
	_, ok := splitDialectTemplateStem(stem)
	return ok
}

func sanitizeDialectTemplateSuffix(name string) string {
	return strings.ToLower(strings.TrimSpace(name))
}

func logicalTemplateName(name string) string {
	dir, file := path.Split(name)
	ext := path.Ext(file)
	stem := strings.TrimSuffix(file, ext)
	base, ok := splitDialectTemplateStem(stem)
	if !ok || base == "" {
		return name
	}
	return dir + base + ext
}

func splitDialectTemplateStem(stem string) (string, bool) {
	for _, separator := range []string{"__", "_"} {
		index := strings.LastIndex(stem, separator)
		if index <= 0 {
			continue
		}
		suffix := sanitizeDialectTemplateSuffix(stem[index+len(separator):])
		if !isKnownDialectTemplateSuffix(suffix) {
			continue
		}
		return stem[:index], true
	}
	return stem, false
}

func isKnownDialectTemplateSuffix(name string) bool {
	name = sanitizeDialectTemplateSuffix(name)
	if name == "mysql" {
		return true
	}
	_, ok := dialectTemplateSuffixAliases[name]
	return ok
}

func dialectTemplateSuffixes(name string) []string {
	name = sanitizeDialectTemplateSuffix(name)
	if name == "" {
		return nil
	}
	aliases, ok := dialectTemplateSuffixAliases[name]
	if ok {
		return aliases
	}
	return []string{name}
}
