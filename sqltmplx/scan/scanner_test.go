package scan_test

import (
	"testing"

	"github.com/arcgolabs/dbx/sqltmplx/scan"
	"github.com/stretchr/testify/require"
)

func TestScanList(t *testing.T) {
	t.Parallel()

	tokens, err := scan.ScanList("select id, name from users /* %where */name = /* name */'bob'/* %end */")
	require.NoError(t, err)
	require.Equal(t, []scan.Token{
		{Kind: scan.Text, Value: "select id, name from users "},
		{Kind: scan.Directive, Value: "%where"},
		{Kind: scan.Text, Value: "name = /* name */'bob'"},
		{Kind: scan.Directive, Value: "%end"},
	}, tokens.Values())
}
