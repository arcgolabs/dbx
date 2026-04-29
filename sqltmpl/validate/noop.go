package validate

// Noop is a no-op SQL parser used when no dialect-specific parser is registered.
type Noop struct{}

// Validate accepts every SQL statement.
func (Noop) Validate(string) error { return nil }

// Analyze returns a minimal statement analysis without dialect-specific parsing.
func (Noop) Analyze(sql string) (*Analysis, error) {
	return &Analysis{
		Dialect:       "",
		StatementType: detectStatementType(sql),
		NormalizedSQL: sql,
		AST:           nil,
	}, nil
}
