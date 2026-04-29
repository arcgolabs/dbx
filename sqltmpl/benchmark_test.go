package sqltmpl_test

import (
	"testing"

	mysqlDialect "github.com/arcgolabs/dbx/dialect/mysql"
	sqltmpl "github.com/arcgolabs/dbx/sqltmpl"
)

var benchmarkTemplateText = `
SELECT id, tenant, status
FROM users
/*%where */
/*%if present(Tenant) */
  AND tenant = /* Tenant */'acme'
/*%end */
/*%if present(Status) */
  AND status = /* Status */'active'
/*%end */
/*%if !empty(IDs) */
  AND id IN (/* IDs */(1, 2, 3))
/*%end */
/*%end */
ORDER BY id DESC
`

type benchmarkQuery struct {
	Tenant string `db:"tenant"`
	Status string `json:"status"`
	IDs    []int  `json:"ids"`
}

func BenchmarkCompile(b *testing.B) {
	engine := sqltmpl.New(mysqlDialect.New(), sqltmpl.WithTemplateCacheSize(0))

	for b.Loop() {
		if _, err := engine.Compile(benchmarkTemplateText); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEngineRenderStruct(b *testing.B) {
	engine := sqltmpl.New(mysqlDialect.New())
	params := benchmarkQuery{Tenant: "acme", Status: "active", IDs: []int{1, 2, 3}}

	b.ResetTimer()
	for b.Loop() {
		if _, err := engine.Render(benchmarkTemplateText, params); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEngineRenderMap(b *testing.B) {
	engine := sqltmpl.New(mysqlDialect.New())
	params := map[string]any{"Tenant": "acme", "Status": "active", "IDs": []int{1, 2, 3}}

	b.ResetTimer()
	for b.Loop() {
		if _, err := engine.Render(benchmarkTemplateText, params); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTemplateRenderReuse(b *testing.B) {
	engine := sqltmpl.New(mysqlDialect.New())
	tpl, err := engine.Compile(benchmarkTemplateText)
	if err != nil {
		b.Fatal(err)
	}
	params := benchmarkQuery{Tenant: "acme", Status: "active", IDs: []int{1, 2, 3}}

	b.ResetTimer()
	for b.Loop() {
		if _, err := tpl.Render(params); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTemplateRenderReuseParallel(b *testing.B) {
	engine := sqltmpl.New(mysqlDialect.New())
	tpl, err := engine.Compile(benchmarkTemplateText)
	if err != nil {
		b.Fatal(err)
	}
	params := benchmarkQuery{Tenant: "acme", Status: "active", IDs: []int{1, 2, 3}}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := tpl.Render(params); err != nil {
				b.Fatal(err)
			}
		}
	})
}
