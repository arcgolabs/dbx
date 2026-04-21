// Package mysqlparser_test contains benchmarks for mysqlparser.
package mysqlparser_test

import (
	"testing"

	"github.com/arcgolabs/dbx/sqltmplx/validate/mysqlparser"
)

func BenchmarkValidateSelect(b *testing.B) {
	parser := mysqlparser.New()
	query := "SELECT 1"

	for b.Loop() {
		if err := parser.Validate(query); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAnalyzeSelect(b *testing.B) {
	parser := mysqlparser.New()
	query := "SELECT 1"

	for b.Loop() {
		if _, err := parser.Analyze(query); err != nil {
			b.Fatal(err)
		}
	}
}
