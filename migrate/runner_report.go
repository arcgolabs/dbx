package migrate

import (
	"fmt"

	collectionx "github.com/arcgolabs/collectionx/list"
	mappingx "github.com/arcgolabs/collectionx/mapping"
	"github.com/pressly/goose/v3"
)

func buildRunReport(
	applied *collectionx.List[AppliedRecord],
	metaByVersion *mappingx.Map[int64, AppliedRecord],
	results []*goose.MigrationResult,
) (RunReport, error) {
	reportApplied, err := collectionx.ReduceErrList[*goose.MigrationResult, *collectionx.List[AppliedRecord]](
		collectionx.NewList[*goose.MigrationResult](results...),
		collectionx.NewListWithCapacity[AppliedRecord](len(results)),
		func(items *collectionx.List[AppliedRecord], _ int, result *goose.MigrationResult) (*collectionx.List[AppliedRecord], error) {
			record, ok := metaByVersion.Get(result.Source.Version)
			if !ok {
				return items, nil
			}
			current, currentErr := appliedRecordForVersion(applied, record)
			if currentErr != nil {
				return nil, currentErr
			}
			items.Add(current)
			return items, nil
		},
	)
	if err != nil {
		return RunReport{}, fmt.Errorf("dbx/migrate: build run report: %w", err)
	}
	return RunReport{Applied: reportApplied}, nil
}
