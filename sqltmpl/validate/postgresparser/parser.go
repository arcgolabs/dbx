package postgresparser

import (
	"fmt"
	"strings"

	"github.com/arcgolabs/dbx/sqltmpl/validate"
	pgquery "github.com/wasilibs/go-pgquery"
)

func init() {
	validate.Register("postgres", New)
}

// Parser validates SQL using the PostgreSQL parser.
type Parser struct{}

// New creates a PostgreSQL SQL parser for sqltmpl validation.
func New() validate.SQLParser { return &Parser{} }

// Validate checks whether sqlText can be parsed as PostgreSQL SQL.
func (p *Parser) Validate(sqlText string) error {
	_, err := pgquery.Parse(sqlText)
	if err != nil {
		return fmt.Errorf("parse PostgreSQL SQL: %w", err)
	}

	return nil
}

// Analyze parses sqlText and returns normalized PostgreSQL metadata.
func (p *Parser) Analyze(sqlText string) (*validate.Analysis, error) {
	result, err := pgquery.Parse(sqlText)
	if err != nil {
		return nil, fmt.Errorf("parse PostgreSQL SQL: %w", err)
	}

	normalized, err := pgquery.Normalize(sqlText)
	if err != nil {
		normalized = normalizeWhitespace(sqlText)
	}

	return &validate.Analysis{
		Dialect:       "postgres",
		StatementType: detectStatementType(sqlText),
		NormalizedSQL: normalized,
		AST:           result,
	}, nil
}

func normalizeWhitespace(sqlText string) string {
	return strings.Join(strings.Fields(sqlText), " ")
}

func detectStatementType(sqlText string) string {
	sqlText = strings.TrimSpace(sqlText)
	if sqlText == "" {
		return "UNKNOWN"
	}
	parts := strings.Fields(sqlText)
	if len(parts) == 0 {
		return "UNKNOWN"
	}
	return strings.ToUpper(parts[0])
}
