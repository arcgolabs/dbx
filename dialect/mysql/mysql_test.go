package mysql_test

import (
	schemax "github.com/arcgolabs/dbx/schema"
	"reflect"
	"testing"

	"github.com/arcgolabs/collectionx"
	mysql "github.com/arcgolabs/dbx/dialect/mysql"
	"github.com/stretchr/testify/require"
)

func TestBuildCreateTable(t *testing.T) {
	bound, err := mysql.New().BuildCreateTable(schemax.TableSpec{
		Name: "users",
		Columns: collectionx.NewList[schemax.ColumnMeta](
			schemax.ColumnMeta{Name: "id", Table: "users", GoType: reflect.TypeFor[int64](), PrimaryKey: true, AutoIncrement: true},
			schemax.ColumnMeta{Name: "username", Table: "users", GoType: reflect.TypeFor[string]()},
			schemax.ColumnMeta{Name: "email_address", Table: "users", GoType: reflect.TypeFor[string]()},
			schemax.ColumnMeta{Name: "role_id", Table: "users", GoType: reflect.TypeFor[int64]()},
			schemax.ColumnMeta{Name: "status", Table: "users", GoType: reflect.TypeFor[int]()},
		),
		PrimaryKey: &schemax.PrimaryKeyMeta{
			Name:    "pk_users",
			Table:   "users",
			Columns: collectionx.NewList[string]("id"),
		},
		ForeignKeys: collectionx.NewList[schemax.ForeignKeyMeta](
			schemax.ForeignKeyMeta{
				Name:          "fk_users_role_id",
				Table:         "users",
				Columns:       collectionx.NewList[string]("role_id"),
				TargetTable:   "roles",
				TargetColumns: collectionx.NewList[string]("id"),
				OnDelete:      schemax.ReferentialCascade,
			},
		),
		Checks: collectionx.NewList[schemax.CheckMeta](
			schemax.CheckMeta{
				Name:       "ck_users_status",
				Table:      "users",
				Expression: "status >= 0",
			},
		),
	})
	require.NoError(t, err)

	expected := "CREATE TABLE IF NOT EXISTS `users` (`id` BIGINT AUTO_INCREMENT PRIMARY KEY, `username` TEXT NOT NULL, `email_address` TEXT NOT NULL, `role_id` BIGINT NOT NULL, `status` INT NOT NULL, CONSTRAINT `fk_users_role_id` FOREIGN KEY (`role_id`) REFERENCES `roles` (`id`) ON DELETE CASCADE, CONSTRAINT `ck_users_status` CHECK (status >= 0))"
	require.Equal(t, expected, bound.SQL)
}

func TestInspectTable(t *testing.T) {
	// InspectTable issues MySQL-specific information_schema queries; it cannot run against SQLite.
	t.Skip("InspectTable requires real mysql")
}
