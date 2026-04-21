// Package postgresparser_test contains benchmarks for postgresparser.
package postgresparser_test

import (
	"testing"

	"github.com/arcgolabs/dbx/sqltmplx/validate/postgresparser"
)

func BenchmarkValidateSelect(b *testing.B) {
	parser := postgresparser.New()
	query := "SELECT 1"

	for b.Loop() {
		if err := parser.Validate(query); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAnalyzeSelect(b *testing.B) {
	parser := postgresparser.New()
	query := "SELECT 1"

	for b.Loop() {
		if _, err := parser.Analyze(query); err != nil {
			b.Fatal(err)
		}
	}
}
