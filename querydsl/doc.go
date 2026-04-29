// Package querydsl provides dbx query AST types, predicates, and SQL rendering.
//
// Prefer schema-first queries with SelectFrom(schema, schema.Columns...) or
// From(schema).Select(...). Use View and Col for CTEs, SQL views, derived
// tables, and other ad-hoc query sources.
package querydsl
