package mapper

import (
	"errors"
	"fmt"
)

var (
	ErrNilMapper          = errors.New("dbx/mapper: mapper is nil")
	ErrNilSession         = errors.New("dbx/mapper: session is nil")
	ErrNilEntity          = errors.New("dbx/mapper: entity is nil")
	ErrTooManyRows        = errors.New("dbx/mapper: query returned more than one row")
	ErrNoPrimaryKey       = errors.New("dbx/mapper: schema does not define a primary key")
	ErrUnmappedColumn     = errors.New("dbx/mapper: result column is not mapped to entity")
	ErrPrimaryKeyUnmapped = errors.New("dbx/mapper: primary key column is not mapped to entity")
	ErrUnsupportedEntity  = errors.New("dbx/mapper: entity type must be a struct")
)

// PrimaryKeyUnmappedError carries the column name when a primary key column
// is not mapped to the entity. Use errors.As to extract the column for programmatic handling.
type PrimaryKeyUnmappedError struct {
	Column string
}

func (e *PrimaryKeyUnmappedError) Error() string {
	if e.Column != "" {
		return fmt.Sprintf("dbx/mapper: primary key column %q is not mapped to entity", e.Column)
	}
	return "dbx/mapper: primary key column is not mapped to entity"
}

func (e *PrimaryKeyUnmappedError) Unwrap() error {
	return ErrPrimaryKeyUnmapped
}

// UnmappedColumnError carries the column name when a result column is not
// mapped to the entity. Use errors.As to extract the column for programmatic handling.
type UnmappedColumnError struct {
	Column string
}

func (e *UnmappedColumnError) Error() string {
	if e.Column != "" {
		return fmt.Sprintf("dbx/mapper: result column %q is not mapped to entity", e.Column)
	}
	return "dbx/mapper: result column is not mapped to entity"
}

func (e *UnmappedColumnError) Unwrap() error {
	return ErrUnmappedColumn
}
