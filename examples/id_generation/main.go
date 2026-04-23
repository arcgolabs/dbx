// Package main demonstrates dbx ID generation strategies.
package main

import (
	"context"
	"fmt"

	columnx "github.com/arcgolabs/dbx/column"
	"github.com/arcgolabs/dbx/idgen"
	mapperx "github.com/arcgolabs/dbx/mapper"
	schemax "github.com/arcgolabs/dbx/schema"
)

type snowflakeUser struct {
	ID   int64  `dbx:"id"`
	Name string `dbx:"name"`
}

type snowflakeUserSchema struct {
	schemax.Schema[snowflakeUser]
	ID   columnx.IDColumn[snowflakeUser, int64, idgen.IDSnowflake] `dbx:"id,pk"`
	Name columnx.Column[snowflakeUser, string]                     `dbx:"name"`
}

type uuidUser struct {
	ID   string `dbx:"id"`
	Name string `dbx:"name"`
}

type uuidUserSchema struct {
	schemax.Schema[uuidUser]
	ID   columnx.Column[uuidUser, string] `dbx:"id,pk"`
	Name columnx.Column[uuidUser, string] `dbx:"name"`
}

type strongTypedUser struct {
	ID   int64  `dbx:"id"`
	Name string `dbx:"name"`
}

type strongTypedUserSchema struct {
	schemax.Schema[strongTypedUser]
	ID   columnx.IDColumn[strongTypedUser, int64, idgen.IDSnowflake] `dbx:"id,pk"`
	Name columnx.Column[strongTypedUser, string]                     `dbx:"name"`
}

func main() {
	snowflakeSchema := schemax.MustSchema("snowflake_users", snowflakeUserSchema{})
	idGenerator, err := idgen.NewDefault(idgen.DefaultNodeID)
	if err != nil {
		panic(err)
	}
	snowflakeEntity := &snowflakeUser{Name: "alice"}
	snowflakeAssignments, err := mapperx.MustMapper[snowflakeUser](snowflakeSchema).InsertAssignmentsWithID(context.Background(), snowflakeSchema, snowflakeEntity, idGenerator)
	if err != nil {
		panic(err)
	}

	uuidSchema := schemax.MustSchema("uuid_users", uuidUserSchema{})
	uuidEntity := &uuidUser{Name: "bob"}
	uuidAssignments, err := mapperx.MustMapper[uuidUser](uuidSchema).InsertAssignmentsWithID(context.Background(), uuidSchema, uuidEntity, idGenerator)
	if err != nil {
		panic(err)
	}

	strongTypedSchema := schemax.MustSchema("strong_typed_users", strongTypedUserSchema{})
	strongTypedEntity := &strongTypedUser{Name: "carol"}
	strongTypedAssignments, err := mapperx.MustMapper[strongTypedUser](strongTypedSchema).InsertAssignmentsWithID(context.Background(), strongTypedSchema, strongTypedEntity, idGenerator)
	if err != nil {
		panic(err)
	}

	printLine("Snowflake by marker type:")
	printFormat("- strategy=%s generated_id=%d assignments=%d\n", snowflakeSchema.ID.Meta().IDStrategy, snowflakeEntity.ID, snowflakeAssignments.Len())

	printLine("UUID by default (string pk => uuidv7):")
	printFormat("- strategy=%s uuid_version=%s generated_id=%s assignments=%d\n", uuidSchema.ID.Meta().IDStrategy, uuidSchema.ID.Meta().UUIDVersion, uuidEntity.ID, uuidAssignments.Len())

	printLine("Snowflake by typed IDColumn marker:")
	printFormat("- strategy=%s generated_id=%d assignments=%d\n", strongTypedSchema.ID.Meta().IDStrategy, strongTypedEntity.ID, strongTypedAssignments.Len())
}

func printLine(text string) {
	if _, err := fmt.Println(text); err != nil {
		panic(err)
	}
}

func printFormat(format string, args ...any) {
	if _, err := fmt.Printf(format, args...); err != nil {
		panic(err)
	}
}
