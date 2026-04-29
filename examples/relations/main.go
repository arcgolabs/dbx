// Package main demonstrates dbx relation joins and relation loaders.
package main

import (
	"context"
	"fmt"
	"strings"

	collectionx "github.com/arcgolabs/collectionx/list"
	"github.com/arcgolabs/dbx"
	"github.com/arcgolabs/dbx/examples/internal/shared"
	mapperx "github.com/arcgolabs/dbx/mapper"
	"github.com/arcgolabs/dbx/querydsl"
	relationx "github.com/arcgolabs/dbx/relation"
	"github.com/arcgolabs/dbx/relationload"
	schemax "github.com/arcgolabs/dbx/schema"
	"github.com/arcgolabs/dbx/schemamigrate"
	"github.com/arcgolabs/dbx/sqlstmt"
	"github.com/samber/mo"
)

type userRoleRow struct {
	ID       int64
	Username string
	RoleName string
}

type userRolePair struct {
	Username string
	RoleName string
}

func main() {
	ctx := context.Background()
	catalog := shared.NewCatalog()

	core, closeDB := openRelationsDB()
	defer closeOrPanic(closeDB)

	prepareRelationsData(ctx, core, catalog)

	belongsToSQL, belongsToRows := runBelongsToExample(ctx, core, catalog)
	printFormat("belongs-to SQL: %s\n", belongsToSQL)
	printUserRoleRows("users with role=admin:", belongsToRows)

	manyToManySQL, manyToManyRows := runManyToManyExample(ctx, core, catalog)
	printFormat("many-to-many SQL: %s\n", manyToManySQL)
	printUserRolePairs("alice roles:", manyToManyRows)

	printLoadedRelations("relation loaders:", loadRelations(ctx, core, catalog))
}

type loadedUserRelations struct {
	Username      string
	BelongsToRole string
	ManyToMany    *collectionx.List[string]
}

func openRelationsDB() (*dbx.DB, func() error) {
	core, closeDB, err := shared.OpenSQLite("dbx-relations", dbx.WithLogger(shared.NewLogger()), dbx.WithDebug(true))
	if err != nil {
		panic(err)
	}

	return core, closeDB
}

func prepareRelationsData(ctx context.Context, core *dbx.DB, catalog shared.Catalog) {
	_, err := schemamigrate.AutoMigrate(ctx, core, catalog.Roles, catalog.Users, catalog.UserRoles)
	if err != nil {
		panic(err)
	}
	err = shared.SeedDemoData(ctx, core, catalog)
	if err != nil {
		panic(err)
	}
}

func runBelongsToExample(ctx context.Context, core *dbx.DB, catalog shared.Catalog) (string, *collectionx.List[userRoleRow]) {
	users := schemax.Alias(catalog.Users, "u")
	roles := schemax.Alias(catalog.Roles, "r")

	query := querydsl.SelectFrom(users, users.ID, users.Username, roles.Name)
	_, err := relationx.Join(query, users, users.Role, roles)
	if err != nil {
		panic(err)
	}
	query = query.Where(roles.Name.Eq("admin")).OrderBy(users.ID.Asc())

	bound, err := dbx.Build(core, query)
	if err != nil {
		panic(err)
	}

	return bound.SQL, scanUserRoleRows(ctx, core, bound)
}

func runManyToManyExample(ctx context.Context, core *dbx.DB, catalog shared.Catalog) (string, *collectionx.List[userRolePair]) {
	users := schemax.Alias(catalog.Users, "u")
	roles := schemax.Alias(catalog.Roles, "r")

	query := querydsl.SelectFrom(users, users.Username, roles.Name)
	_, err := relationx.Join(query, users, users.Roles, roles)
	if err != nil {
		panic(err)
	}
	query = query.Where(users.Username.Eq("alice")).OrderBy(roles.Name.Asc())

	bound, err := dbx.Build(core, query)
	if err != nil {
		panic(err)
	}

	return bound.SQL, scanUserRolePairs(ctx, core, bound)
}

func scanUserRoleRows(ctx context.Context, core *dbx.DB, bound sqlstmt.Bound) *collectionx.List[userRoleRow] {
	rows, err := core.QueryBoundContext(ctx, bound)
	if err != nil {
		panic(err)
	}
	defer closeRowsOrPanic(rows.Close)

	out := collectionx.NewList[userRoleRow]()
	for rows.Next() {
		var row userRoleRow
		err = rows.Scan(&row.ID, &row.Username, &row.RoleName)
		if err != nil {
			panic(err)
		}
		out.Add(row)
	}
	err = rows.Err()
	if err != nil {
		panic(err)
	}

	return out
}

func scanUserRolePairs(ctx context.Context, core *dbx.DB, bound sqlstmt.Bound) *collectionx.List[userRolePair] {
	rows, err := core.QueryBoundContext(ctx, bound)
	if err != nil {
		panic(err)
	}
	defer closeRowsOrPanic(rows.Close)

	out := collectionx.NewList[userRolePair]()
	for rows.Next() {
		var row userRolePair
		err = rows.Scan(&row.Username, &row.RoleName)
		if err != nil {
			panic(err)
		}
		out.Add(row)
	}
	err = rows.Err()
	if err != nil {
		panic(err)
	}

	return out
}

func loadRelations(ctx context.Context, core *dbx.DB, catalog shared.Catalog) *collectionx.List[loadedUserRelations] {
	userMapper := mapperx.MustMapper[shared.User](catalog.Users)
	roleMapper := mapperx.MustMapper[shared.Role](catalog.Roles)
	usersToLoad, err := dbx.QueryAll[shared.User](
		ctx,
		core,
		querydsl.SelectFrom(catalog.Users, querydsl.AllColumns(catalog.Users).Values()...).OrderBy(catalog.Users.ID.Asc()),
		userMapper,
	)
	if err != nil {
		panic(err)
	}

	loadedRole := make([]mo.Option[shared.Role], usersToLoad.Len())
	err = relationload.LoadBelongsTo[shared.User, shared.Role](
		ctx,
		core,
		usersToLoad,
		catalog.Users,
		userMapper,
		catalog.Users.Role,
		catalog.Roles,
		roleMapper,
		func(index int, user shared.User, value mo.Option[shared.Role]) shared.User {
			loadedRole[index] = value
			return user
		},
	)
	if err != nil {
		panic(err)
	}

	loadedRoles := make([][]shared.Role, usersToLoad.Len())
	err = relationload.LoadManyToMany[shared.User, shared.Role](
		ctx,
		core,
		usersToLoad,
		catalog.Users,
		userMapper,
		catalog.Users.Roles,
		catalog.Roles,
		roleMapper,
		func(index int, user shared.User, value *collectionx.List[shared.Role]) shared.User {
			loadedRoles[index] = value.Values()
			return user
		},
	)
	if err != nil {
		panic(err)
	}

	results := collectionx.NewListWithCapacity[loadedUserRelations](usersToLoad.Len())
	usersToLoad.Range(func(index int, user shared.User) bool {
		results.Add(loadedUserRelations{
			Username:      user.Username,
			BelongsToRole: optionRoleName(loadedRole[index]),
			ManyToMany:    roleNames(loadedRoles[index]),
		})
		return true
	})

	return results
}

func optionRoleName(value mo.Option[shared.Role]) string {
	if value.IsPresent() {
		role, _ := value.Get()
		return role.Name
	}

	return "<none>"
}

func roleNames(roles []shared.Role) *collectionx.List[string] {
	names := collectionx.NewListWithCapacity[string](len(roles))
	for index := range roles {
		names.Add(roles[index].Name)
	}
	return names
}

func printUserRoleRows(title string, rows *collectionx.List[userRoleRow]) {
	printLine(title)
	rows.Range(func(_ int, row userRoleRow) bool {
		printFormat("- id=%d username=%s role=%s\n", row.ID, row.Username, row.RoleName)
		return true
	})
}

func printUserRolePairs(title string, rows *collectionx.List[userRolePair]) {
	printLine(title)
	rows.Range(func(_ int, row userRolePair) bool {
		printFormat("- username=%s role=%s\n", row.Username, row.RoleName)
		return true
	})
}

func printLoadedRelations(title string, rows *collectionx.List[loadedUserRelations]) {
	printLine(title)
	rows.Range(func(_ int, row loadedUserRelations) bool {
		printFormat(
			"- user=%s belongs-to role=%s many-to-many roles=%s\n",
			row.Username,
			row.BelongsToRole,
			strings.Join(row.ManyToMany.Values(), ","),
		)
		return true
	})
}

func closeRowsOrPanic(closeFn func() error) {
	if err := closeFn(); err != nil {
		panic(err)
	}
}

func closeOrPanic(closeFn func() error) {
	if err := closeFn(); err != nil {
		panic(err)
	}
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
