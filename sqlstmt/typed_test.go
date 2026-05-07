package sqlstmt_test

import (
	"errors"
	"testing"

	collectionx "github.com/arcgolabs/collectionx/list"
	"github.com/arcgolabs/dbx/sqlstmt"
)

func TestNewTypedBindsConcreteParams(t *testing.T) {
	type params struct {
		Status int64
	}

	statement := sqlstmt.NewTyped("user.find_active", func(value params) (sqlstmt.Bound, error) {
		return sqlstmt.Bound{
			SQL:  `SELECT * FROM users WHERE status = ?`,
			Args: collectionx.NewList[any](value.Status),
		}, nil
	})

	bound, err := statement.Bind(params{Status: 1})
	if err != nil {
		t.Fatalf("Bind returned error: %v", err)
	}
	if bound.Name != "user.find_active" {
		t.Fatalf("expected statement name to be set, got %q", bound.Name)
	}
	if got := bound.Args.Values(); len(got) != 1 || got[0] != int64(1) {
		t.Fatalf("unexpected args: %#v", got)
	}
}

func TestNewTypedSourceRejectsWrongParamsType(t *testing.T) {
	type params struct {
		Status int64
	}

	statement := sqlstmt.NewTyped("user.find_active", func(value params) (sqlstmt.Bound, error) {
		return sqlstmt.Bound{}, nil
	})

	_, err := statement.Source().Bind("wrong")
	if err == nil {
		t.Fatal("expected wrong params type to fail")
	}
}

func TestNewTypedNilBinder(t *testing.T) {
	statement := sqlstmt.NewTyped[int64]("user.find_active", nil)

	_, err := statement.Bind(1)
	if !errors.Is(err, sqlstmt.ErrNilStatement) {
		t.Fatalf("expected ErrNilStatement, got %v", err)
	}
}
