package shared

import (
	columnx "github.com/arcgolabs/dbx/column"
	relationx "github.com/arcgolabs/dbx/relation"
	schemax "github.com/arcgolabs/dbx/schema"
)

// Role represents an example role row.
type Role struct {
	ID   int64  `dbx:"id"`
	Name string `dbx:"name"`
}

// User represents an example user row.
type User struct {
	ID       int64  `dbx:"id"`
	Username string `dbx:"username"`
	Email    string `dbx:"email_address"`
	Status   int    `dbx:"status"`
	RoleID   int64  `dbx:"role_id"`
}

// UserRoleLink represents an example user-to-role join row.
type UserRoleLink struct {
	UserID int64 `dbx:"user_id"`
	RoleID int64 `dbx:"role_id"`
}

// UserSummary projects a subset of user columns for query examples.
type UserSummary struct {
	ID       int64  `dbx:"id"`
	Username string `dbx:"username"`
	Email    string `dbx:"email_address"`
}

// Catalog groups the example schemas used across dbx demos.
type Catalog struct {
	Roles     RoleSchema
	Users     UserSchema
	UserRoles UserRoleLinkSchema
}

// RoleSchema defines the roles table.
type RoleSchema struct {
	schemax.Schema[Role]
	ID   columnx.Column[Role, int64]  `dbx:"id,pk,auto"`
	Name columnx.Column[Role, string] `dbx:"name,unique"`
}

// UserSchema defines the users table and its relations.
type UserSchema struct {
	schemax.Schema[User]
	ID                columnx.Column[User, int64]      `dbx:"id,pk,auto"`
	Username          columnx.Column[User, string]     `dbx:"username"`
	Email             columnx.Column[User, string]     `dbx:"email_address,unique"`
	Status            columnx.Column[User, int]        `dbx:"status,default=1"`
	RoleID            columnx.Column[User, int64]      `dbx:"role_id,ref=roles.id,ondelete=cascade"`
	Role              relationx.BelongsTo[User, Role]  `rel:"table=roles,local=role_id,target=id"`
	Roles             relationx.ManyToMany[User, Role] `rel:"table=roles,target=id,join=user_roles,join_local=user_id,join_target=role_id"`
	UsernameStatusIdx schemax.Index[User]              `idx:"name=idx_users_username_status,columns=username|status"`
	StatusCheck       schemax.Check[User]              `check:"name=ck_users_status_nonnegative,expr=status >= 0"`
}

// UserRoleLinkSchema defines the user_roles join table.
type UserRoleLinkSchema struct {
	schemax.Schema[UserRoleLink]
	UserID columnx.Column[UserRoleLink, int64] `dbx:"user_id,ref=users.id,ondelete=cascade"`
	RoleID columnx.Column[UserRoleLink, int64] `dbx:"role_id,ref=roles.id,ondelete=cascade"`
	PK     schemax.CompositeKey[UserRoleLink]  `key:"name=pk_user_roles,columns=user_id|role_id"`
}

// NewCatalog constructs the shared example schema catalog.
func NewCatalog() Catalog {
	return Catalog{
		Roles:     schemax.MustSchema("roles", RoleSchema{}),
		Users:     schemax.MustSchema("users", UserSchema{}),
		UserRoles: schemax.MustSchema("user_roles", UserRoleLinkSchema{}),
	}
}
