package render_test

import (
	"testing"

	"github.com/arcgolabs/dbx/dialect/postgres"
	"github.com/arcgolabs/dbx/sqltmplx/parse"
	"github.com/arcgolabs/dbx/sqltmplx/render"
)

type benchmarkNestedFilter struct {
	IDs []int `json:"ids"`
}

type benchmarkParams struct {
	Tenant string                `db:"tenant"`
	Status string                `json:"status"`
	Filter benchmarkNestedFilter `json:"filter"`
}

var (
	benchmarkLookupParams = benchmarkParams{
		Tenant: "acme",
		Status: "active",
		Filter: benchmarkNestedFilter{IDs: []int{1, 2, 3}},
	}
	benchmarkLookupNodes = []parse.Node{
		parse.TextNode{Text: "tenant = /* tenant */'acme' AND status = /* status */'active' AND id IN (/* filter.ids */(1, 2, 3))"},
	}
)

func BenchmarkRenderLookupStruct(b *testing.B) {
	for b.Loop() {
		if _, err := render.Render(benchmarkLookupNodes, benchmarkLookupParams, postgres.New()); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRenderLookupMap(b *testing.B) {
	params := map[string]any{
		"tenant": "acme",
		"status": "active",
		"filter": map[string]any{"ids": []int{1, 2, 3}},
	}

	for b.Loop() {
		if _, err := render.Render(benchmarkLookupNodes, params, postgres.New()); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRenderTextStruct(b *testing.B) {
	textNodes := []parse.Node{
		parse.TextNode{Text: "tenant = /* tenant */'acme' AND id IN (/* filter.ids */(1, 2, 3))"},
	}

	for b.Loop() {
		if _, err := render.Render(textNodes, benchmarkLookupParams, postgres.New()); err != nil {
			b.Fatal(err)
		}
	}
}
