---
title: 'Relations Tutorial'
linkTitle: 'tutorial-relations'
description: 'BelongsTo and batch relation loading in dbx'
weight: 14
---

## Relations Tutorial

This tutorial shows relation declaration and batch loading with `LoadBelongsTo`.

## When to Use

- You have normalized tables and want typed relation metadata.
- You want to avoid N+1 query patterns with batch relation loading.
- You want attach callbacks per source entity.

## Minimal Project Layout

```text
.
├── go.mod
└── main.go
```

## Complete Runnable Example

```go
package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"

	"github.com/arcgolabs/dbx"
	columnx "github.com/arcgolabs/dbx/column"
	"github.com/arcgolabs/dbx/dialect/sqlite"
	"github.com/arcgolabs/dbx/idgen"
	mapperx "github.com/arcgolabs/dbx/mapper"
	"github.com/arcgolabs/dbx/querydsl"
	relationx "github.com/arcgolabs/dbx/relation"
	"github.com/arcgolabs/dbx/relationload"
	"github.com/arcgolabs/dbx/schemamigrate"
	schemax "github.com/arcgolabs/dbx/schema"
	"github.com/samber/mo"

	_ "modernc.org/sqlite"
)

type Role struct {
	ID   int64  `dbx:"id"`
	Name string `dbx:"name"`
}

type User struct {
	ID       int64  `dbx:"id"`
	RoleID   int64  `dbx:"role_id"`
	Username string `dbx:"username"`
}

type RoleSchema struct {
	schemax.Schema[Role]
	ID   columnx.IDColumn[Role, int64, idgen.IDSnowflake] `dbx:"id,pk"`
	Name columnx.Column[Role, string]                   `dbx:"name,unique"`
}

type UserSchema struct {
	schemax.Schema[User]
	ID       columnx.IDColumn[User, int64, idgen.IDSnowflake] `dbx:"id,pk"`
	RoleID   columnx.Column[User, int64]                    `dbx:"role_id,ref=roles.id,ondelete=cascade,index"`
	Username columnx.Column[User, string]                   `dbx:"username,index"`
	Role     relationx.BelongsTo[User, Role]                  `rel:"table=roles,local=role_id,target=id"`
}

var Roles = schemax.MustSchema("roles", RoleSchema{})
var Users = schemax.MustSchema("users", UserSchema{})

func main() {
	ctx := context.Background()
	raw, err := sql.Open("sqlite", "file:dbx_relations.db?cache=shared")
	if err != nil {
		log.Fatal(err)
	}
	defer raw.Close()

	core, err := dbx.NewWithOptions(raw, sqlite.New())
	if err != nil {
		log.Fatal(err)
	}
	if _, err := schemamigrate.AutoMigrate(ctx, core, Roles, Users); err != nil {
		log.Fatal(err)
	}

	userMapper := mapperx.MustMapper[User](Users)
	loader := relationload.New[User, Role](core, Users, Roles)

items, err := dbx.QueryAll[User](ctx, core, querydsl.SelectFrom(Users, querydsl.AllColumns(Users).Values()...), userMapper)
	if err != nil {
		log.Fatal(err)
	}

	if err := loader.BelongsTo(
		ctx,
		items,
		Users.Role,
		func(index int, user User, role mo.Option[Role]) User {
			if role.IsPresent() {
				value, _ := role.Get()
				fmt.Printf("user=%s role=%s\n", user.Username, value.Name)
			}
			return user
		},
	); err != nil {
		log.Fatal(err)
	}
}
```

## Pitfalls

- Missing `rel` tag fields (`table`, `local`, `target`) breaks relation loading.
- Source key type and target key type must be compatible.
- Forgetting to migrate both source and target schemas causes runtime query errors.

## Verify

```bash
go test ./... -run Relation
go run .
```
