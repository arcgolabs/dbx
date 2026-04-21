package repository

import "errors"

// ErrNotFound indicates that the requested entity does not exist.
var ErrNotFound = errors.New("dbx/repository: not found")

// ErrNilMutation indicates that a mutation query was required but nil was provided.
var ErrNilMutation = errors.New("dbx/repository: mutation query is nil")

// ErrConflict indicates that the mutation violated a uniqueness or constraint rule.
var ErrConflict = errors.New("dbx/repository: conflict")

// ErrValidation indicates that repository input validation failed.
var ErrValidation = errors.New("dbx/repository: validation")

// ErrVersionConflict indicates that optimistic concurrency checks failed.
var ErrVersionConflict = errors.New("dbx/repository: version conflict")

// ValidationError describes repository validation failures.
type ValidationError struct {
	Message string
}

// Error implements the error interface.
func (e *ValidationError) Error() string {
	if e.Message == "" {
		return "dbx/repository: validation failed"
	}
	return "dbx/repository: validation failed: " + e.Message
}

// Unwrap returns the sentinel validation error.
func (e *ValidationError) Unwrap() error { return ErrValidation }

// ConflictError wraps a conflict-related mutation error.
type ConflictError struct{ Err error }

// Error implements the error interface.
func (e *ConflictError) Error() string { return e.Err.Error() }

// Unwrap returns the sentinel conflict error.
func (e *ConflictError) Unwrap() error { return ErrConflict }

// VersionConflictError wraps optimistic-lock conflicts.
type VersionConflictError struct{ Err error }

// Error implements the error interface.
func (e *VersionConflictError) Error() string { return e.Err.Error() }

// Unwrap returns the sentinel version conflict error.
func (e *VersionConflictError) Unwrap() error { return ErrVersionConflict }
