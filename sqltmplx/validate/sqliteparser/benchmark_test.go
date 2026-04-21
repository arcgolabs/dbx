// Package sqliteparser_test contains benchmarks for sqliteparser.
package sqliteparser_test

import (
	"testing"

	"github.com/arcgolabs/dbx/sqltmplx/validate/sqliteparser"
)

func BenchmarkValidateSelect(b *testing.B) {
	parser := sqliteparser.New()
	query := "SELECT 1"

	for b.Loop() {
		if err := parser.Validate(query); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAnalyzeSelect(b *testing.B) {
	parser := sqliteparser.New()
	query := "SELECT 1"

	for b.Loop() {
		if _, err := parser.Analyze(query); err != nil {
			b.Fatal(err)
		}
	}
}
