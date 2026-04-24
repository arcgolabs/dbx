package relationload_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/arcgolabs/collectionx"
	"github.com/samber/mo"
)

func BenchmarkLoadBelongsTo(b *testing.B) {
	users := MustSchema("users", relationUserSchema{})
	roles := MustSchema("roles", relationRoleSchema{})
	items := collectionx.NewList[relationUser](
		relationUser{ID: 1, Name: "alice", RoleID: 2},
		relationUser{ID: 2, Name: "bob", RoleID: 4},
	)
	ddl := []string{relationTestSchemaDDL, `INSERT INTO "roles" ("id","name") VALUES (2,'admin')`}

	run := func(b *testing.B, sqlDB *sql.DB) {
		b.Helper()
		core := New(sqlDB, testSQLiteDialect{})
		sourceMapper := MustMapper[relationUser](users)
		targetMapper := MustMapper[relationRole](roles)
		loaded := make([]mo.Option[relationRole], items.Len())
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if err := LoadBelongsTo(context.Background(), core, items, users, sourceMapper, users.Role, roles, targetMapper, func(index int, user relationUser, value mo.Option[relationRole]) relationUser {
				loaded[index] = value
				return user
			}); err != nil {
				b.Fatalf("LoadBelongsTo returned error: %v", err)
			}
		}
	}

	b.Run("Memory", func(b *testing.B) {
		sqlDB, cleanup := OpenBenchmarkSQLiteMemory(b, ddl...)
		defer cleanup()
		run(b, sqlDB)
	})
	b.Run("IO", func(b *testing.B) {
		sqlDB, cleanup := OpenBenchmarkSQLite(b, ddl...)
		defer cleanup()
		run(b, sqlDB)
	})
}

func BenchmarkLoadHasMany(b *testing.B) {
	users := MustSchema("users", relationUserSchema{})
	posts := MustSchema("posts", relationPostSchema{})
	items := collectionx.NewList[relationUser](
		relationUser{ID: 1, Name: "alice"},
		relationUser{ID: 2, Name: "bob"},
	)
	ddl := []string{
		relationTestSchemaDDL,
		`INSERT INTO "roles" ("id","name") VALUES (1,'r')`,
		`INSERT INTO "users" ("id","name","role_id") VALUES (1,'alice',1),(2,'bob',1)`,
		`INSERT INTO "posts" ("id","user_id","title") VALUES (100,1,'first'),(101,1,'second'),(200,2,'third')`,
	}

	run := func(b *testing.B, sqlDB *sql.DB) {
		b.Helper()
		core := New(sqlDB, testSQLiteDialect{})
		sourceMapper := MustMapper[relationUser](users)
		targetMapper := MustMapper[relationPost](posts)
		loaded := make([][]relationPost, items.Len())
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if err := LoadHasMany(context.Background(), core, items, users, sourceMapper, users.Posts, posts, targetMapper, func(index int, user relationUser, value collectionx.List[relationPost]) relationUser {
				loaded[index] = value.Values()
				return user
			}); err != nil {
				b.Fatalf("LoadHasMany returned error: %v", err)
			}
		}
	}

	b.Run("Memory", func(b *testing.B) {
		sqlDB, cleanup := OpenBenchmarkSQLiteMemory(b, ddl...)
		defer cleanup()
		run(b, sqlDB)
	})
	b.Run("IO", func(b *testing.B) {
		sqlDB, cleanup := OpenBenchmarkSQLite(b, ddl...)
		defer cleanup()
		run(b, sqlDB)
	})
}

func BenchmarkLoadManyToMany(b *testing.B) {
	users := MustSchema("users", relationUserSchema{})
	tags := MustSchema("tags", relationTagSchema{})
	items := collectionx.NewList[relationUser](
		relationUser{ID: 1, Name: "alice"},
		relationUser{ID: 2, Name: "bob"},
	)
	ddl := []string{
		relationTestSchemaDDL,
		`INSERT INTO "roles" ("id","name") VALUES (1,'r')`,
		`INSERT INTO "users" ("id","name","role_id") VALUES (1,'alice',1),(2,'bob',1)`,
		`INSERT INTO "tags" ("id","name") VALUES (10,'red'),(11,'blue')`,
		`INSERT INTO "user_tags" ("user_id","tag_id") VALUES (1,10),(1,11),(2,11)`,
	}

	run := func(b *testing.B, sqlDB *sql.DB) {
		b.Helper()
		core := New(sqlDB, testSQLiteDialect{})
		sourceMapper := MustMapper[relationUser](users)
		targetMapper := MustMapper[relationTag](tags)
		loaded := make([][]relationTag, items.Len())
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if err := LoadManyToMany(context.Background(), core, items, users, sourceMapper, users.Tags, tags, targetMapper, func(index int, user relationUser, value collectionx.List[relationTag]) relationUser {
				loaded[index] = value.Values()
				return user
			}); err != nil {
				b.Fatalf("LoadManyToMany returned error: %v", err)
			}
		}
	}

	b.Run("Memory", func(b *testing.B) {
		sqlDB, cleanup := OpenBenchmarkSQLiteMemory(b, ddl...)
		defer cleanup()
		run(b, sqlDB)
	})
	b.Run("IO", func(b *testing.B) {
		sqlDB, cleanup := OpenBenchmarkSQLite(b, ddl...)
		defer cleanup()
		run(b, sqlDB)
	})
}
