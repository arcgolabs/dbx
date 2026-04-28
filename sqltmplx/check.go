package sqltmplx

import (
	"fmt"

	"github.com/arcgolabs/collectionx"
	"github.com/arcgolabs/dbx/dialect"
	"github.com/arcgolabs/dbx/sqlstmt"
	"github.com/arcgolabs/dbx/sqltmplx/validate"
)

// CheckStage identifies the phase where a template check finished or failed.
type CheckStage string

const (
	CheckStageCompile CheckStage = "compile"
	CheckStageLoad    CheckStage = "load"
	CheckStageRender  CheckStage = "render"
	CheckStageAnalyze CheckStage = "analyze"
	CheckStageOK      CheckStage = "ok"
)

// CheckReport describes a rendered template plus any available SQL analysis.
type CheckReport struct {
	Name           string
	Dialect        string
	Stage          CheckStage
	SampleProvided bool
	Metadata       TemplateMetadata
	SQL            string
	Args           collectionx.List[any]
	Analysis       *validate.Analysis
	Err            error
}

// Check renders the template, validates it, and collects any available SQL analysis.
func (t *Template) Check(params any) (CheckReport, error) {
	if t == nil {
		report := CheckReport{Stage: CheckStageRender, Err: sqlstmt.ErrNilStatement}
		return report, report.Err
	}

	report := CheckReport{
		Name:           t.name,
		Dialect:        t.dialect.Name(),
		Stage:          CheckStageRender,
		SampleProvided: params != nil,
		Metadata:       t.Metadata(),
	}
	bound, err := t.Render(params)
	if err != nil {
		report.Err = fmt.Errorf("render template check: %w", err)
		return report, report.Err
	}

	report.SQL = bound.Query
	report.Args = bound.Args.Clone()
	report.Stage = CheckStageAnalyze
	analysis, err := templateAnalyzer(t.validator, t.dialect).Analyze(bound.Query)
	if err != nil {
		report.Err = fmt.Errorf("analyze rendered sql: %w", err)
		return report, report.Err
	}
	report.Analysis = analysis
	report.Stage = CheckStageOK
	return report, nil
}

func templateAnalyzer(validator validate.Validator, d dialect.Contract) validate.Analyzer {
	if analyzer, ok := validator.(validate.Analyzer); ok && analyzer != nil {
		return analyzer
	}
	return validate.NewSQLParser(d)
}
