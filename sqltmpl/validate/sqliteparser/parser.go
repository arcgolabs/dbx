package sqliteparser

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/arcgolabs/dbx/sqltmpl/validate"
	rqlitesql "github.com/rqlite/sql"
	// Register the modernc SQLite driver for in-memory syntax validation.
	_ "modernc.org/sqlite"
)

func init() {
	validate.Register("sqlite", New)
}

// Parser validates SQL using SQLite prepare and AST parsing.
type Parser struct{}

// New creates a SQLite SQL parser for sqltmpl validation.
func New() validate.SQLParser { return &Parser{} }

// Validate checks whether sqlText can be prepared as SQLite SQL.
func (p *Parser) Validate(sqlText string) (retErr error) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		return fmt.Errorf("open SQLite validator database: %w", err)
	}
	defer func() {
		retErr = errors.Join(retErr, closeSQLiteCloser(db, "SQLite validator database"))
	}()

	stmt, err := db.PrepareContext(context.Background(), sqlText)
	if err != nil {
		return fmt.Errorf("prepare SQLite SQL: %w", err)
	}
	defer func() {
		retErr = errors.Join(retErr, closeSQLiteCloser(stmt, "SQLite prepared statement"))
	}()

	return nil
}

// Analyze parses sqlText and returns normalized SQLite metadata.
func (p *Parser) Analyze(sqlText string) (*validate.Analysis, error) {
	parser := rqlitesql.NewParser(strings.NewReader(sqlText))
	astNode, err := parser.ParseStatement()
	if err != nil {
		return nil, fmt.Errorf("parse SQLite SQL: %w", err)
	}
	if err := p.Validate(sqlText); err != nil {
		return nil, fmt.Errorf("sqlite engine validation failed after AST parse: %w", err)
	}
	return &validate.Analysis{
		Dialect:       "sqlite",
		StatementType: detectStatementType(sqlText),
		NormalizedSQL: normalizeWhitespace(sqlText),
		AST:           astNode,
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

func closeSQLiteCloser(closer interface{ Close() error }, target string) error {
	if err := closer.Close(); err != nil {
		return fmt.Errorf("close %s: %w", target, err)
	}

	return nil
}
