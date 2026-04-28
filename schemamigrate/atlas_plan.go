package schemamigrate

import (
	"context"
	"hash/fnv"
	"strconv"
	"strings"

	"github.com/arcgolabs/dbx"
	"github.com/arcgolabs/dbx/querydsl"
	schemax "github.com/arcgolabs/dbx/schema"

	atlasmigrate "ariga.io/atlas/sql/migrate"
	atlasmysql "ariga.io/atlas/sql/mysql"
	atlaspostgres "ariga.io/atlas/sql/postgres"
	atlasschema "ariga.io/atlas/sql/schema"
	atlassqlite "ariga.io/atlas/sql/sqlite"
	"github.com/arcgolabs/collectionx"
	"github.com/samber/hot"
)

var compiledSchemaCache = hot.NewHotCache[string, *atlasCompiledSchema](hot.LRU, 128).Build()

type atlasCompiledSchema struct {
	schema    *atlasschema.Schema
	tables    collectionx.Map[string, *atlasCompiledTable]
	externals collectionx.Map[string, *atlasschema.Table]
	order     collectionx.List[string]
}

type atlasCompiledTable struct {
	spec              schemax.TableSpec
	table             *atlasschema.Table
	columnsByName     collectionx.Map[string, schemax.ColumnMeta]
	indexesByName     collectionx.Map[string, schemax.IndexMeta]
	indexesByKey      collectionx.Map[string, schemax.IndexMeta]
	foreignKeysByName collectionx.Map[string, schemax.ForeignKeyMeta]
	foreignKeysByKey  collectionx.Map[string, schemax.ForeignKeyMeta]
	checksByName      collectionx.Map[string, schemax.CheckMeta]
	checksByExpr      collectionx.Map[string, schemax.CheckMeta]
}

func schemaFingerprint(schemas []Resource) string {
	if len(schemas) == 0 {
		return ""
	}
	var buffer querydsl.Buffer
	collectionx.NewListWithCapacity[Resource](len(schemas), schemas...).Range(func(_ int, resource Resource) bool {
		writeSchemaFingerprint(&buffer, resource.Spec())
		return true
	})
	if err := buffer.Err("build schema fingerprint"); err != nil {
		return ""
	}
	return fingerprintString(buffer.String())
}

func writeSchemaFingerprint(buffer *querydsl.Buffer, spec schemax.TableSpec) {
	buffer.WriteString("T:")
	buffer.WriteString(spec.Name)
	buffer.WriteString("|")
	spec.Columns.Range(func(_ int, column schemax.ColumnMeta) bool {
		writeFingerprintColumn(buffer, column)
		return true
	})
	spec.Indexes.Range(func(_ int, index schemax.IndexMeta) bool {
		writeFingerprintIndex(buffer, index)
		return true
	})
	if spec.PrimaryKey != nil {
		buffer.WriteString("PK:")
		buffer.WriteString(columnsKey(spec.PrimaryKey.Columns))
		buffer.WriteString("|")
	}
	spec.ForeignKeys.Range(func(_ int, foreignKey schemax.ForeignKeyMeta) bool {
		buffer.WriteString("FK:")
		buffer.WriteString(foreignKeyKey(foreignKey))
		buffer.WriteString("|")
		return true
	})
	spec.Checks.Range(func(_ int, check schemax.CheckMeta) bool {
		buffer.WriteString("CK:")
		buffer.WriteString(check.Name)
		buffer.WriteString(":")
		buffer.WriteString(checkKey(check.Expression))
		buffer.WriteString("|")
		return true
	})
}

func writeFingerprintColumn(buffer *querydsl.Buffer, column schemax.ColumnMeta) {
	buffer.WriteString("C:")
	buffer.WriteString(column.Name)
	buffer.WriteString(":")
	buffer.WriteString(fingerprintColumnType(column))
	buffer.WriteString(":")
	buffer.WriteString(strconv.FormatBool(column.Nullable))
	buffer.WriteString(":")
	buffer.WriteString(column.DefaultValue)
	buffer.WriteString(":")
	buffer.WriteString(strconv.FormatBool(column.PrimaryKey))
	buffer.WriteString(":")
	buffer.WriteString(strconv.FormatBool(column.AutoIncrement))
	if column.References != nil {
		buffer.WriteString(":ref:")
		buffer.WriteString(column.References.TargetTable)
		buffer.WriteString(".")
		buffer.WriteString(column.References.TargetColumn)
	}
	buffer.WriteString("|")
}

func fingerprintColumnType(column schemax.ColumnMeta) string {
	if column.SQLType != "" {
		return column.SQLType
	}
	return InferTypeName(column)
}

func writeFingerprintIndex(buffer *querydsl.Buffer, index schemax.IndexMeta) {
	buffer.WriteString("I:")
	buffer.WriteString(index.Name)
	buffer.WriteString(":")
	buffer.WriteString(columnsKey(index.Columns))
	buffer.WriteString(":")
	buffer.WriteString(strconv.FormatBool(index.Unique))
	buffer.WriteString("|")
}

func fingerprintString(value string) string {
	h := fnv.New64a()
	if _, err := h.Write([]byte(value)); err != nil {
		return ""
	}
	return strconv.FormatUint(h.Sum64(), 16)
}

func planSchemaChangesWithAtlas(ctx context.Context, session dbx.Session, schemas ...Resource) (schemax.MigrationPlan, bool, error) {
	if len(schemas) == 0 {
		return schemax.MigrationPlan{}, true, nil
	}
	if err := validateAtlasPlanningSession(session); err != nil {
		return schemax.MigrationPlan{}, true, err
	}

	driver, ok, err := atlasDriverForSession(session)
	if err != nil || !ok {
		return schemax.MigrationPlan{}, ok, err
	}

	current, schemaName, err := atlasCurrentSchema(ctx, driver, session, schemas)
	if err != nil {
		return schemax.MigrationPlan{}, true, err
	}
	compiled, err := atlasCompiledSchemaForSession(session, driver, schemaName, schemas)
	if err != nil {
		return schemax.MigrationPlan{}, true, err
	}
	if current == nil {
		current = atlasschema.New(schemaName)
	}
	return atlasSchemaDiffPlan(ctx, driver, compiled, current)
}

func validateAtlasPlanningSession(session dbx.Session) error {
	if session == nil {
		return dbx.ErrNilDB
	}
	if session.Dialect() == nil {
		return dbx.ErrNilDialect
	}
	return nil
}

func atlasCurrentSchema(ctx context.Context, driver atlasmigrate.Driver, session dbx.Session, schemas []Resource) (*atlasschema.Schema, string, error) {
	tableNames := collectionx.MapList[Resource, string](collectionx.NewListWithCapacity[Resource](len(schemas), schemas...), func(_ int, schema Resource) string {
		return schema.TableName()
	})
	current, err := atlasInspectCurrentSchema(ctx, driver, tableNames.Values())
	if err != nil {
		return nil, "", err
	}
	schemaName := atlasDefaultSchemaName(session.Dialect().Name())
	if current != nil && strings.TrimSpace(current.Name) != "" {
		schemaName = current.Name
	}
	return current, schemaName, nil
}

func atlasCompiledSchemaForSession(session dbx.Session, driver atlasmigrate.Driver, schemaName string, schemas []Resource) (*atlasCompiledSchema, error) {
	dialectName := session.Dialect().Name()
	cacheKey := dialectName + ":" + schemaName + ":" + schemaFingerprint(schemas)
	if compiled, ok, err := compiledSchemaCache.Get(cacheKey); err != nil {
		return nil, wrapMigrateError("read compiled schema cache", err)
	} else if ok {
		return compiled, nil
	}

	compiled := compileAtlasSchema(dialectName, driver, schemaName, schemas)
	compiledSchemaCache.Set(cacheKey, compiled)
	return compiled, nil
}

func atlasSchemaDiffPlan(ctx context.Context, driver atlasmigrate.Driver, compiled *atlasCompiledSchema, current *atlasschema.Schema) (schemax.MigrationPlan, bool, error) {
	changes, err := driver.SchemaDiff(current, compiled.schema)
	if err != nil {
		return schemax.MigrationPlan{}, true, wrapMigrateError("diff atlas schema", err)
	}
	report := atlasReportFromChanges(changes, compiled, current)
	if len(changes) == 0 {
		return schemax.MigrationPlan{Actions: collectionx.NewList[schemax.MigrationAction](), Report: report}, true, nil
	}
	safeChanges, manualActions := atlasSplitChanges(changes)
	actions, err := atlasPlanActions(ctx, driver, safeChanges)
	if err != nil {
		return schemax.MigrationPlan{}, true, err
	}
	return schemax.MigrationPlan{
		Actions: collectionx.NewListWithCapacity[schemax.MigrationAction](len(actions)+len(manualActions), append(actions, manualActions...)...),
		Report:  report,
	}, true, nil
}

func atlasDriverForSession(session dbx.Session) (atlasmigrate.Driver, bool, error) {
	switch strings.ToLower(strings.TrimSpace(session.Dialect().Name())) {
	case "sqlite":
		driver, err := atlassqlite.Open(session)
		return driver, true, wrapMigrateError("open atlas sqlite driver", err)
	case "mysql":
		driver, err := atlasmysql.Open(session)
		return driver, true, wrapMigrateError("open atlas mysql driver", err)
	case "postgres":
		driver, err := atlaspostgres.Open(session)
		return driver, true, wrapMigrateError("open atlas postgres driver", err)
	default:
		return nil, false, nil
	}
}

func atlasInspectCurrentSchema(ctx context.Context, driver atlasmigrate.Driver, tables []string) (*atlasschema.Schema, error) {
	current, err := driver.InspectSchema(ctx, "", &atlasschema.InspectOptions{Mode: atlasschema.InspectTables, Tables: tables})
	if err != nil {
		if atlasschema.IsNotExistError(err) {
			var empty *atlasschema.Schema
			return empty, nil
		}
		return nil, wrapMigrateError("inspect current atlas schema", err)
	}
	return current, nil
}

func atlasDefaultSchemaName(dialectName string) string {
	switch strings.ToLower(strings.TrimSpace(dialectName)) {
	case "sqlite":
		return "main"
	case "postgres":
		return "public"
	default:
		return ""
	}
}
