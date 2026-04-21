package dbx_test

import (
	dbx "github.com/arcgolabs/dbx"
	codecx "github.com/arcgolabs/dbx/codec"
	"github.com/arcgolabs/dbx/idgen"
	mapperx "github.com/arcgolabs/dbx/mapper"
	"github.com/arcgolabs/dbx/paging"
	projectionx "github.com/arcgolabs/dbx/projection"
	"github.com/arcgolabs/dbx/querydsl"
	relationx "github.com/arcgolabs/dbx/relation"
	schemamigrate "github.com/arcgolabs/dbx/schemamigrate"
	"github.com/arcgolabs/dbx/sqlexec"
	"github.com/arcgolabs/dbx/sqlstmt"
)

var ErrIDGeneratorNodeIDConflict = dbx.ErrIDGeneratorNodeIDConflict
var ErrInvalidNodeID = idgen.ErrInvalidNodeID
var ErrMissingDialect = dbx.ErrMissingDialect
var ErrMissingDriver = dbx.ErrMissingDriver
var ErrMissingDSN = dbx.ErrMissingDSN
var ErrNilQuery = dbx.ErrNilQuery
var ErrPrimaryKeyUnmapped = mapperx.ErrPrimaryKeyUnmapped
var ErrRelationCardinality = dbx.ErrRelationCardinality
var ErrTooManyRows = mapperx.ErrTooManyRows
var ErrUnknownCodec = codecx.ErrUnknown
var ErrUnmappedColumn = mapperx.ErrUnmappedColumn

var And = querydsl.And
var AtlasSplitChangesForTest = schemamigrate.AtlasSplitChangesForTest
var AutoMigrate = schemamigrate.AutoMigrate
var Build = dbx.Build
var ClonePrimaryKeyMetaForTest = dbx.ClonePrimaryKeyMetaForTest
var ClonePrimaryKeyStateForTest = dbx.ClonePrimaryKeyStateForTest
var CloseRowsForTest = dbx.CloseRowsForTest
var CompileAtlasSchemaForTest = schemamigrate.CompileAtlasSchemaForTest
var CountAll = querydsl.CountAll
var DefaultOptions = dbx.DefaultOptions
var DefaultOptionsList = dbx.DefaultOptionsList
var DeleteFrom = querydsl.DeleteFrom
var ErrorRowForTest = dbx.ErrorRowForTest
var Exec = dbx.Exec
var Exists = querydsl.Exists
var IndexesForTest = dbx.IndexesForTest
var InferTypeNameForTest = dbx.InferTypeNameForTest
var InsertInto = querydsl.InsertInto
var JoinRelation = relationx.Join
var MustNewWithOptions = dbx.MustNewWithOptions
var MustNewWithOptionsList = dbx.MustNewWithOptionsList
var MustRegisterCodec = codecx.MustRegister
var MustSelectMapped = projectionx.MustSelect
var NamedTable = querydsl.NamedTable
var New = dbx.New
var NewDefaultIDGenerator = idgen.NewDefault
var NewKSUIDGenerator = idgen.NewKSUID
var NewPageRequest = paging.NewRequest
var NewSnowflakeGenerator = idgen.NewSnowflake
var NewSQLExecutorForTest = sqlexec.New
var NewStatement = sqlstmt.New
var NewULIDGenerator = idgen.NewULID
var NewUUIDGenerator = idgen.NewUUID
var NewWithOptions = dbx.NewWithOptions
var NewWithOptionsList = dbx.NewWithOptionsList
var Page = paging.Page
var PlanSchemaChanges = schemamigrate.PlanSchemaChanges
var ProductionOptions = dbx.ProductionOptions
var ProductionOptionsList = dbx.ProductionOptionsList
var ProjectionOf = projectionx.Of
var RowsIterErrorForTest = dbx.RowsIterErrorForTest
var Select = querydsl.Select
var SelectMapped = projectionx.Select
var TableSpecForTest = dbx.TableSpecForTest
var TestOptions = dbx.TestOptions
var TestOptionsList = dbx.TestOptionsList
var Update = querydsl.Update
var ValidateSchemas = schemamigrate.ValidateSchemas
var WithDebug = dbx.WithDebug
var WithHooks = dbx.WithHooks
var WithHooksList = dbx.WithHooksList
var WithIDGenerator = dbx.WithIDGenerator
var WithLogger = dbx.WithLogger
var WithMapperCodecs = mapperx.WithMapperCodecs
var WithNodeID = dbx.WithNodeID
