package validate_test

import (
	"testing"

	"github.com/arcgolabs/dbx/sqltmplx/validate"
)

func TestNewSQLParser(t *testing.T) {
	t.Run("fallback to noop when backend is not registered", func(t *testing.T) {
		parser := validate.NewSQLParser(testDialect{name: "validate_test_mysql"})
		if parser == nil {
			t.Fatal("parser should not be nil")
		}
		analysis, err := parser.Analyze("select 1")
		if err != nil {
			t.Fatalf("Analyze returned error: %v", err)
		}
		if analysis.Dialect != "validate_test_mysql" {
			t.Fatalf("unexpected dialect: %q", analysis.Dialect)
		}
	})

	t.Run("registered backend is selected by dialect name", func(t *testing.T) {
		validate.Register("validate_test_postgres", func() validate.SQLParser { return stubParser{} })

		parser := validate.NewSQLParser(testDialect{name: "validate_test_postgres"})
		if parser == nil {
			t.Fatal("parser should not be nil")
		}

		if _, ok := parser.(stubParser); !ok {
			t.Fatalf("unexpected parser type: %T", parser)
		}
	})
}

type testDialect struct {
	name string
}

func (d testDialect) BindVar(_ int) string { return "?" }
func (d testDialect) Name() string         { return d.name }

type stubParser struct{}

func (stubParser) Validate(string) error { return nil }

func (stubParser) Analyze(sql string) (*validate.Analysis, error) {
	return &validate.Analysis{
		Dialect:       "postgres",
		StatementType: "SELECT",
		NormalizedSQL: sql,
	}, nil
}
