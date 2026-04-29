package sqltmpl

import (
	"strings"

	collectionx "github.com/arcgolabs/collectionx/list"
	setx "github.com/arcgolabs/collectionx/set"
	"github.com/arcgolabs/dbx/sqltmpl/parse"
)

// TemplateMetadata describes static properties extracted from a compiled template.
type TemplateMetadata struct {
	StatementType    string
	Parameters       *collectionx.List[string]
	SpreadParameters *collectionx.List[string]
	Conditions       *collectionx.List[string]
	HasWhereBlock    bool
	HasSetBlock      bool
}

func buildTemplateMetadata(nodes *collectionx.List[parse.Node]) TemplateMetadata {
	metadata := TemplateMetadata{
		StatementType:    detectTemplateStatementType(templatePreviewSQL(nodes)),
		Parameters:       collectionx.NewList[string](),
		SpreadParameters: collectionx.NewList[string](),
		Conditions:       collectionx.NewList[string](),
	}

	seenParams := setx.NewSet[string]()
	seenSpread := setx.NewSet[string]()
	seenConditions := setx.NewSet[string]()
	collectTemplateMetadata(metadataCollector{
		metadata:       &metadata,
		seenParams:     seenParams,
		seenSpread:     seenSpread,
		seenConditions: seenConditions,
	}, nodes)
	return metadata
}

type metadataCollector struct {
	metadata       *TemplateMetadata
	seenParams     *setx.Set[string]
	seenSpread     *setx.Set[string]
	seenConditions *setx.Set[string]
}

func collectTemplateMetadata(collector metadataCollector, nodes *collectionx.List[parse.Node]) {
	nodes.Range(func(_ int, node parse.Node) bool {
		switch typed := node.(type) {
		case parse.ParamNode:
			addTemplateParameter(collector, typed.Name, typed.Spread)
		case *parse.IfNode:
			addTemplateCondition(collector, typed.RawExpr)
			collectTemplateMetadata(collector, typed.Body)
		case *parse.WhereNode:
			collector.metadata.HasWhereBlock = true
			collectTemplateMetadata(collector, typed.Body)
		case *parse.SetNode:
			collector.metadata.HasSetBlock = true
			collectTemplateMetadata(collector, typed.Body)
		}
		return true
	})
}

func addTemplateParameter(collector metadataCollector, name string, spread bool) {
	if name == "" {
		return
	}
	if !collector.seenParams.Contains(name) {
		collector.seenParams.Add(name)
		collector.metadata.Parameters.Add(name)
	}
	if spread && !collector.seenSpread.Contains(name) {
		collector.seenSpread.Add(name)
		collector.metadata.SpreadParameters.Add(name)
	}
}

func addTemplateCondition(collector metadataCollector, expr string) {
	expr = strings.TrimSpace(expr)
	if expr == "" || collector.seenConditions.Contains(expr) {
		return
	}
	collector.seenConditions.Add(expr)
	collector.metadata.Conditions.Add(expr)
}

func templatePreviewSQL(nodes *collectionx.List[parse.Node]) string {
	var builder strings.Builder
	writeTemplatePreview(&builder, nodes)
	return builder.String()
}

func writeTemplatePreview(builder *strings.Builder, nodes *collectionx.List[parse.Node]) {
	nodes.Range(func(_ int, node parse.Node) bool {
		switch typed := node.(type) {
		case parse.TextNode:
			writeMetadataString(builder, typed.Text)
		case parse.ParamNode:
			writeMetadataString(builder, " ? ")
		case *parse.IfNode:
			writeTemplatePreview(builder, typed.Body)
		case *parse.WhereNode:
			writeMetadataString(builder, " WHERE ")
			writeTemplatePreview(builder, typed.Body)
		case *parse.SetNode:
			writeMetadataString(builder, " SET ")
			writeTemplatePreview(builder, typed.Body)
		}
		return true
	})
}

func detectTemplateStatementType(sql string) string {
	sql = strings.TrimSpace(sql)
	if sql == "" {
		return "UNKNOWN"
	}
	parts := strings.Fields(sql)
	if len(parts) == 0 {
		return "UNKNOWN"
	}
	return strings.ToUpper(parts[0])
}

func cloneTemplateMetadata(metadata TemplateMetadata) TemplateMetadata {
	return TemplateMetadata{
		StatementType:    metadata.StatementType,
		Parameters:       metadata.Parameters.Clone(),
		SpreadParameters: metadata.SpreadParameters.Clone(),
		Conditions:       metadata.Conditions.Clone(),
		HasWhereBlock:    metadata.HasWhereBlock,
		HasSetBlock:      metadata.HasSetBlock,
	}
}

func writeMetadataString(builder *strings.Builder, value string) {
	if _, err := builder.WriteString(value); err != nil {
		panic(err)
	}
}
