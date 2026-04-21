package mysqlparser

import (
	"fmt"
	"strings"

	"github.com/arcgolabs/dbx/sqltmplx/validate"
	"vitess.io/vitess/go/vt/sqlparser"
)

func init() {
	validate.Register("mysql", New)
}

// Parser validates SQL using the Vitess MySQL parser.
type Parser struct {
	parser *sqlparser.Parser
}

// New creates a MySQL SQL parser for sqltmplx validation.
func New() validate.SQLParser {
	return &Parser{parser: sqlparser.NewTestParser()}
}

// Validate checks whether sqlText can be parsed as MySQL SQL.
func (p *Parser) Validate(sqlText string) error {
	_, err := p.parser.Parse(sqlText)
	if err != nil {
		return fmt.Errorf("parse MySQL SQL: %w", err)
	}

	return nil
}

// Analyze parses sqlText and returns normalized MySQL metadata.
func (p *Parser) Analyze(sqlText string) (*validate.Analysis, error) {
	stmt, err := p.parser.Parse(sqlText)
	if err != nil {
		return nil, fmt.Errorf("parse MySQL SQL: %w", err)
	}

	return &validate.Analysis{
		Dialect:       "mysql",
		StatementType: statementType(stmt),
		NormalizedSQL: normalizeWhitespace(sqlText),
		AST:           stmt,
	}, nil
}

func statementType(stmt sqlparser.Statement) string {
	switch stmt.(type) {
	case *sqlparser.Select:
		return "SELECT"
	case *sqlparser.Insert:
		return "INSERT"
	case *sqlparser.Update:
		return "UPDATE"
	case *sqlparser.Delete:
		return "DELETE"
	default:
		return detectStatementType(sqlparser.String(stmt))
	}
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
