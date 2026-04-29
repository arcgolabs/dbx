# API Direction

dbx is an ORM toolkit built on top of Go's `database/sql`. The public API should stay close to the Go ecosystem while giving application code a modern, generic-first, strongly typed model.

The project favors typed APIs at package boundaries and accepts reflection inside implementations when it keeps user code simpler. Reflection should be hidden behind schema binding, mapping, scanning, and template execution internals.

## Package Boundaries

The root package remains a thin runtime layer:

- database/session wrappers around `database/sql`
- bound SQL execution helpers
- cursor and row helpers
- hooks, observability, and runtime tracing
- small convenience functions that delegate to subpackages

Subpackages own the higher-level programming models:

- `schema`: table, column, relation, index, check, and migration metadata declarations
- `mapper`: entity-to-column mapping, codecs, scan plans, and mutation assignments
- `querydsl`: strongly typed SQL expression and builder APIs
- `repository`: entity-oriented persistence with specs, pagination, and relation loading
- `activerecord`: a thin Active Record facade over repository behavior
- `sqltmpl`: Doma-style SQL templates for handwritten SQL
- `migrate` and `schemamigrate`: versioned SQL/Go migration and schema-diff planning

Schema declaration and data mapping stay separate. Schema describes SQL structure. Mapper describes how Go values flow to and from rows. This separation is intentional and should be preserved.

## Querydsl Direction

`querydsl` is the strongest typed API surface. Its public API should make invalid SQL shapes harder to express:

- columns and result aliases should carry `T`
- column-to-column comparisons should require matching `T`
- string-only operators such as `LIKE` should require string expressions
- aggregates should infer or constrain their result type from typed input
- query builders can keep non-generic internal renderer interfaces, but public helpers should expose typed wrappers

Preferred usage:

```go
users := schemax.MustSchema("users", UserSchema{})

query := querydsl.SelectFrom(users, users.ID, users.Username).
	Where(querydsl.And(
		users.Status.Eq(1),
		querydsl.Like(users.Username, "a%"),
	)).
	OrderBy(users.ID.Desc())
```

Ad-hoc sources such as CTEs and SQL views should prefer struct-shaped source declarations:

```go
type ActiveUsersSource struct {
	querydsl.Table
	ID       querydsl.Column[int64]  `dbx:"id"`
	Username querydsl.Column[string] `dbx:"username"`
}

activeUsers := querydsl.MustSource("active_users", ActiveUsersSource{})

query := querydsl.SelectFrom(activeUsers, activeUsers.ID, activeUsers.Username).
	Where(activeUsers.ID.Gt(100))
```

## Repository And Active Record

Repository should hold repeated runtime context so public calls do not pass long parameter lists. Repeated values such as session, schema, mapper, relation metadata, and runtime caches should be captured by repository/store objects or small request structs.

Preferred direction:

- constructors bind schema and mapper once
- CRUD methods accept entity values and focused option/spec arguments
- relation loading should be exposed through repository/store-level helpers instead of long free-function parameter lists
- Active Record stays a thin convenience layer over repository and should not own separate persistence rules

Long functions like relation loading internals may keep explicit parameters internally, but public APIs should not require callers to assemble every dependency manually.

## Sqltmpl Direction

The Doma-style template package is `sqltmpl`.

The package owns:

- SQL template parsing
- parameter binding
- conditional blocks and spread parameters
- validation hooks
- typed execution helpers for handwritten SQL

The package should not duplicate querydsl. Querydsl builds SQL ASTs. Sqltmpl executes authored SQL files or strings with controlled parameter binding.

Backward compatibility with the previous template module name is not required for this repository while it is still personally maintained. Rename imports and module paths directly when performing the migration.

## Internal Implementation

Internal code should prefer maintainable collection and option APIs where they clarify intent:

- `collectionx/list`, `mapping`, and `set` for collection operations and stable data structures
- `lo` for small transformations when it reads better than local loops
- `mo` for optional values at public or semi-public boundaries

Do not force helpers into performance-sensitive paths where a direct loop is clearer. For SQL rendering, schema diffing, and mapping hot paths, clarity and allocation behavior should both be considered.

## Refactoring Order

1. Rename the template module to `sqltmpl` across modules, docs, examples, and workspace config.
2. Continue tightening `querydsl` typed expressions and reduce `any` in public helpers.
3. Simplify repository and active record public APIs by capturing repeated dependencies.
4. Replace internal hand-rolled map/list/set patterns with collectionx when it improves readability.
5. Keep docs and examples aligned with the preferred API after each API change.
