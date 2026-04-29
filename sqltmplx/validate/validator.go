package validate

import (
	"strings"

	collectionx "github.com/arcgolabs/collectionx/mapping"
	"github.com/arcgolabs/dbx/dialect"
)

// Validator checks whether a SQL statement is valid for a dialect.
type Validator interface {
	Validate(sql string) error
}

// Analyzer extracts lightweight metadata from a SQL statement.
type Analyzer interface {
	Analyze(sql string) (*Analysis, error)
}

// SQLParser combines validation and analysis for a SQL dialect.
type SQLParser interface {
	Validator
	Analyzer
}

// Analysis describes the parsed SQL statement at a high level.
type Analysis struct {
	Dialect       string
	StatementType string
	NormalizedSQL string
	AST           any
}

// Func adapts a function into a [Validator].
type Func func(string) error

// Validate calls f with sql.
func (f Func) Validate(sql string) error { return f(sql) }

// Factory creates a SQL parser instance for a dialect.
type Factory func() SQLParser

var parserRegistry = collectionx.NewConcurrentMap[string, Factory]()

// NewSQLParser returns a registered parser for d or a no-op fallback parser.
func NewSQLParser(d dialect.Contract) SQLParser {
	name := strings.ToLower(strings.TrimSpace(d.Name()))
	if factory, ok := parserRegistry.Get(name); ok {
		return factory()
	}
	return &noopParser{dialect: d.Name()}
}

// Register adds a parser factory for dialectName.
func Register(dialectName string, factory Factory) {
	if factory == nil {
		return
	}
	parserRegistry.Set(strings.ToLower(strings.TrimSpace(dialectName)), factory)
}

type noopParser struct{ dialect string }

func (n *noopParser) Validate(_ string) error { return nil }

func (n *noopParser) Analyze(sql string) (*Analysis, error) {
	return &Analysis{
		Dialect:       n.dialect,
		StatementType: detectStatementType(sql),
		NormalizedSQL: sql,
		AST:           nil,
	}, nil
}
