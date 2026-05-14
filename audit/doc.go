// Package audit provides explicit schema-backed audit writing for dbx.
//
// It does not create audit tables implicitly and does not use database triggers.
// Callers declare revision and audit schemas through dbx/schema, then bind those
// schemas to entity columns with Entity.
package audit
