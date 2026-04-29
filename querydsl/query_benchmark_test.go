package querydsl_test

import (
	"github.com/arcgolabs/dbx/querydsl"
	"testing"

	basedialect "github.com/arcgolabs/dbx/dialect"
)

func BenchmarkBuildSelect(b *testing.B) {
	users := MustSchema("users", UserSchema{})
	query := Select(users.ID, users.Username, users.Email).
		From(users).
		Where(And(users.Status.Eq(1), Like(users.Username, "a%"))).
		OrderBy(users.ID.Desc()).
		Limit(20).
		Offset(10)

	benchmarkBuildAcrossDialects(b, query)
}

func BenchmarkBuildAggregateHaving(b *testing.B) {
	users := MustSchema("users", UserSchema{})
	roles := MustSchema("roles", RoleSchema{})
	query := Select(
		roles.Name,
		Count(users.ID).As("user_count"),
	).
		From(roles).
		LeftJoin(users).On(users.RoleID.EqColumn(roles.ID)).
		GroupBy(roles.Name).
		Having(Count(users.ID).Gt(1)).
		OrderBy(Count(users.ID).Desc())

	benchmarkBuildAcrossDialects(b, query)
}

func BenchmarkBuildInsertUpsertReturning(b *testing.B) {
	users := MustSchema("users", UserSchema{})
	query := InsertInto(users).
		Values(
			users.Username.Set("alice"),
			users.Email.Set("alice@example.com"),
			users.Status.Set(1),
			users.RoleID.Set(int64(1)),
		).
		OnConflict(users.Email).
		DoUpdateSet(users.Status.SetExcluded(), users.Username.SetExcluded()).
		Returning(users.ID, users.Username)

	benchmarkBuildWithDialects(b, query,
		benchmarkDialect{name: "sqlite", dialect: testSQLiteDialect{}},
		benchmarkDialect{name: "postgres", dialect: testPostgresDialect{}},
	)
}

func BenchmarkBuildCTEUnionCase(b *testing.B) {
	users := MustSchema("users", UserSchema{})
	activeUsers := View("active_users")
	activeID := querydsl.Col[int64](activeUsers, "id")
	activeName := querydsl.Col[string](activeUsers, "username")

	caseExpr := CaseWhen[string](users.Status.Eq(1), "active").
		Else("inactive").
		As("status_label")

	query := SelectFrom(activeUsers, activeID, activeName, caseExpr).
		With("active_users",
			SelectFrom(users, users.ID, users.Username).
				Where(users.Status.Eq(1)),
		).
		UnionAll(
			SelectFrom(users, users.ID, users.Username, caseExpr).
				Where(users.Status.Ne(1)),
		)

	benchmarkBuildAcrossDialects(b, query)
}

type benchmarkDialect struct {
	name    string
	dialect basedialect.Dialect
}

func benchmarkBuildAcrossDialects(b *testing.B, query querydsl.Builder) {
	b.Helper()
	benchmarkBuildWithDialects(b, query,
		benchmarkDialect{name: "sqlite", dialect: testSQLiteDialect{}},
		benchmarkDialect{name: "postgres", dialect: testPostgresDialect{}},
		benchmarkDialect{name: "mysql", dialect: testMySQLDialect{}},
	)
}

func benchmarkBuildWithDialects(b *testing.B, query querydsl.Builder, dialects ...benchmarkDialect) {
	b.Helper()
	for _, item := range dialects {
		b.Run(item.name, func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				if _, err := query.Build(item.dialect); err != nil {
					b.Fatalf("Build returned error: %v", err)
				}
			}
		})
	}
}
