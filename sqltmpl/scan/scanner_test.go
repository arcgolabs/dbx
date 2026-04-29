package scan_test

import (
	"testing"

	"github.com/arcgolabs/dbx/sqltmpl/scan"
	"github.com/stretchr/testify/require"
)

func TestScanList(t *testing.T) {
	t.Parallel()

	tokens, err := scan.ScanList("select id, name from users /* %where */name = /* name */'bob'/* %end */")
	require.NoError(t, err)
	require.Equal(t, []scan.Token{
		{
			Kind:  scan.Text,
			Value: "select id, name from users ",
			Span:  scan.Span{Start: scan.Position{Line: 1, Column: 1}, End: scan.Position{Offset: 27, Line: 1, Column: 28}},
		},
		{
			Kind:  scan.Directive,
			Value: "%where",
			Span:  scan.Span{Start: scan.Position{Offset: 27, Line: 1, Column: 28}, End: scan.Position{Offset: 39, Line: 1, Column: 40}},
		},
		{
			Kind:  scan.Text,
			Value: "name = /* name */'bob'",
			Span:  scan.Span{Start: scan.Position{Offset: 39, Line: 1, Column: 40}, End: scan.Position{Offset: 61, Line: 1, Column: 62}},
		},
		{
			Kind:  scan.Directive,
			Value: "%end",
			Span:  scan.Span{Start: scan.Position{Offset: 61, Line: 1, Column: 62}, End: scan.Position{Offset: 71, Line: 1, Column: 72}},
		},
	}, tokens.Values())
}
