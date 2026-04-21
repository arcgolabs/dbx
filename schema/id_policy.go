package schema

import (
	"fmt"
	"reflect"

	"github.com/arcgolabs/dbx/idgen"
)

func normalizeIDPolicy(meta ColumnMeta) (ColumnMeta, error) {
	if !meta.PrimaryKey {
		return meta, nil
	}

	columnType := normalizeIDColumnType(meta.GoType)
	meta = applyDefaultIDStrategy(meta, columnType)
	return validateIDStrategy(meta, columnType)
}

func normalizeIDColumnType(columnType reflect.Type) reflect.Type {
	for columnType != nil && columnType.Kind() == reflect.Pointer {
		columnType = columnType.Elem()
	}
	return columnType
}

func applyDefaultIDStrategy(meta ColumnMeta, columnType reflect.Type) ColumnMeta {
	if meta.IDStrategy != idgen.StrategyUnset {
		return meta
	}
	return inferIDStrategyFromType(meta, columnType)
}

func validateIDStrategy(meta ColumnMeta, columnType reflect.Type) (ColumnMeta, error) {
	if meta.IDStrategy == idgen.StrategyUnset {
		return meta, nil
	}
	if meta.IDStrategy == idgen.StrategyDBAuto {
		return handleDBAutoStrategy(meta)
	}
	if meta.IDStrategy == idgen.StrategySnowflake {
		return handleSnowflakeStrategy(meta, columnType)
	}
	if meta.IDStrategy == idgen.StrategyUUID {
		return handleUUIDStrategy(meta, columnType)
	}
	if meta.IDStrategy == idgen.StrategyULID || meta.IDStrategy == idgen.StrategyKSUID {
		return handleStringIDStrategy(meta, columnType)
	}
	return meta, fmt.Errorf("dbx: unsupported id strategy %q for column %s", meta.IDStrategy, meta.Name)
}

func inferIDStrategyFromType(meta ColumnMeta, columnType reflect.Type) ColumnMeta {
	switch {
	case columnType != nil && columnType.Kind() == reflect.Int64:
		meta.IDStrategy = idgen.StrategyDBAuto
	case columnType != nil && columnType.Kind() == reflect.String:
		meta.IDStrategy = idgen.StrategyUUID
		if meta.UUIDVersion == "" {
			meta.UUIDVersion = idgen.DefaultUUIDVersion
		}
	}
	return meta
}

func handleDBAutoStrategy(meta ColumnMeta) (ColumnMeta, error) {
	meta.AutoIncrement = true
	meta.UUIDVersion = ""
	return meta, nil
}

func handleSnowflakeStrategy(meta ColumnMeta, columnType reflect.Type) (ColumnMeta, error) {
	if columnType == nil || columnType.Kind() != reflect.Int64 {
		return meta, fmt.Errorf("dbx: snowflake id strategy only supports int64 primary keys, column %s", meta.Name)
	}
	meta.AutoIncrement = false
	meta.UUIDVersion = ""
	return meta, nil
}

func handleUUIDStrategy(meta ColumnMeta, columnType reflect.Type) (ColumnMeta, error) {
	if columnType == nil || columnType.Kind() != reflect.String {
		return meta, fmt.Errorf("dbx: uuid id strategy only supports string primary keys, column %s", meta.Name)
	}
	meta.AutoIncrement = false
	if meta.UUIDVersion == "" {
		meta.UUIDVersion = idgen.DefaultUUIDVersion
	}
	if meta.UUIDVersion != "v7" && meta.UUIDVersion != "v4" {
		return meta, fmt.Errorf("dbx: unsupported uuid version %q for column %s", meta.UUIDVersion, meta.Name)
	}
	return meta, nil
}

func handleStringIDStrategy(meta ColumnMeta, columnType reflect.Type) (ColumnMeta, error) {
	if columnType == nil || columnType.Kind() != reflect.String {
		return meta, fmt.Errorf("dbx: %s id strategy only supports string primary keys, column %s", meta.IDStrategy, meta.Name)
	}
	meta.AutoIncrement = false
	meta.UUIDVersion = ""
	return meta, nil
}
