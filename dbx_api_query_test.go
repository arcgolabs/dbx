package dbx_test

import (
	"context"
	"fmt"

	collectionx "github.com/arcgolabs/collectionx/list"
	dbx "github.com/arcgolabs/dbx"
	codecx "github.com/arcgolabs/dbx/codec"
	columnx "github.com/arcgolabs/dbx/column"
	mapperx "github.com/arcgolabs/dbx/mapper"
	"github.com/arcgolabs/dbx/paging"
	"github.com/arcgolabs/dbx/querydsl"
	schemax "github.com/arcgolabs/dbx/schema"
	"github.com/arcgolabs/dbx/sqlexec"
	"github.com/arcgolabs/dbx/sqlstmt"
	"github.com/samber/mo"
)

func Alias[S querydsl.TableSource](schema S, alias string) S {
	return schemax.Alias(schema, alias)
}

func CaseWhen[T any](predicate querydsl.Predicate, value any) *CaseBuilder[T] {
	return querydsl.CaseWhen[T](predicate, value)
}

func Count[E any, T any](expr Column[E, T]) Aggregate[int64] {
	return querydsl.Count(expr)
}

func Like[E any](column Column[E, string], pattern string) querydsl.Predicate {
	return querydsl.Like(column, pattern)
}

func AllColumns(schema SchemaResource) *collectionx.List[querydsl.SelectItem] {
	return collectionx.MapList[schemax.ColumnMeta, querydsl.SelectItem](schema.Spec().Columns, func(_ int, column schemax.ColumnMeta) querydsl.SelectItem {
		return columnx.Named[any](schema, column.Name)
	})
}

func MustMapper[E any](schema SchemaResource) Mapper[E] {
	return mapperx.MustMapper[E](schema)
}

func MustSchema[S any](name string, schema S) S {
	return schemax.MustSchema(name, schema)
}

func MustStructMapper[E any]() StructMapper[E] {
	return mapperx.MustStructMapper[E]()
}

func NamedColumn[T any](source TableSource, name string) Column[struct{}, T] {
	return columnx.Named[T](source, name)
}

func NewCodec[T any](name string, decode func(any) (T, error), encode func(T) (any, error)) Codec {
	return codecx.New(name, decode, encode)
}

func NewStructMapper[E any]() (StructMapper[E], error) {
	mapper, err := mapperx.NewStructMapper[E]()
	if err != nil {
		var zero StructMapper[E]
		return zero, fmt.Errorf("new struct mapper: %w", err)
	}
	return mapper, nil
}

func NewStructMapperWithOptions[E any](opts ...MapperOption) (StructMapper[E], error) {
	mapper, err := mapperx.NewStructMapperWithOptions[E](opts...)
	if err != nil {
		var zero StructMapper[E]
		return zero, fmt.Errorf("new struct mapper with options: %w", err)
	}
	return mapper, nil
}

func NewPageResult[E any](items *collectionx.List[E], total int64, request PageRequest) PageResult[E] {
	return paging.NewResult[E](items, total, request)
}

func MapPageResult[E any, R any](result PageResult[E], mapper func(index int, item E) R) PageResult[R] {
	return paging.MapResult[E, R](result, mapper)
}

func QueryAll[E any](ctx context.Context, session Session, query querydsl.Builder, mapper RowsScanner[E]) (*collectionx.List[E], error) {
	items, err := dbx.QueryAll[E](ctx, session, query, mapper)
	if err != nil {
		return nil, fmt.Errorf("query all: %w", err)
	}
	return items, nil
}

func QueryAllList[E any](ctx context.Context, session Session, query querydsl.Builder, mapper RowsScanner[E]) (*collectionx.List[E], error) {
	items, err := dbx.QueryAllList[E](ctx, session, query, mapper)
	if err != nil {
		return nil, fmt.Errorf("query all list: %w", err)
	}
	return items, nil
}

func QueryAllBound[E any](ctx context.Context, session Session, bound sqlstmt.Bound, mapper RowsScanner[E]) (*collectionx.List[E], error) {
	items, err := dbx.QueryAllBound[E](ctx, session, bound, mapper)
	if err != nil {
		return nil, fmt.Errorf("query all bound: %w", err)
	}
	return items, nil
}

func QueryAllBoundList[E any](ctx context.Context, session Session, bound sqlstmt.Bound, mapper RowsScanner[E]) (*collectionx.List[E], error) {
	items, err := dbx.QueryAllBoundList[E](ctx, session, bound, mapper)
	if err != nil {
		return nil, fmt.Errorf("query all bound list: %w", err)
	}
	return items, nil
}

func QueryCursor[E any](ctx context.Context, session Session, query querydsl.Builder, mapper RowsScanner[E]) (Cursor[E], error) {
	cursor, err := dbx.QueryCursor[E](ctx, session, query, mapper)
	if err != nil {
		return nil, fmt.Errorf("query cursor: %w", err)
	}
	return cursor, nil
}

func QueryEach[E any](ctx context.Context, session Session, query querydsl.Builder, mapper RowsScanner[E]) func(func(E, error) bool) {
	return dbx.QueryEach[E](ctx, session, query, mapper)
}

func ResultColumn[T any](name string) Column[struct{}, T] {
	return columnx.Result[T](name)
}

func SQLCursor[E any](ctx context.Context, session Session, statement sqlstmt.Source, params any, mapper RowsScanner[E]) (Cursor[E], error) {
	cursor, err := dbx.SQLCursor[E](ctx, session, statement, params, mapper)
	if err != nil {
		return nil, fmt.Errorf("sql cursor: %w", err)
	}
	return cursor, nil
}

func SQLEach[E any](ctx context.Context, session Session, statement sqlstmt.Source, params any, mapper RowsScanner[E]) func(func(E, error) bool) {
	return dbx.SQLEach[E](ctx, session, statement, params, mapper)
}

func SQLFind[E any](ctx context.Context, session Session, statement sqlstmt.Source, params any, mapper RowsScanner[E]) (mo.Option[E], error) {
	item, err := sqlexec.Find[E](ctx, session, statement, params, mapper)
	if err != nil {
		return mo.None[E](), fmt.Errorf("sql find: %w", err)
	}
	return item, nil
}

func SQLGet[E any](ctx context.Context, session Session, statement sqlstmt.Source, params any, mapper RowsScanner[E]) (E, error) {
	item, err := sqlexec.Get[E](ctx, session, statement, params, mapper)
	if err != nil {
		var zero E
		return zero, fmt.Errorf("sql get: %w", err)
	}
	return item, nil
}

func SQLList[E any](ctx context.Context, session Session, statement sqlstmt.Source, params any, mapper RowsScanner[E]) (*collectionx.List[E], error) {
	items, err := sqlexec.List[E](ctx, session, statement, params, mapper)
	if err != nil {
		return nil, fmt.Errorf("sql list: %w", err)
	}
	return items, nil
}

func SQLQueryList[E any](ctx context.Context, session Session, statement sqlstmt.Source, params any, mapper RowsScanner[E]) (*collectionx.List[E], error) {
	items, err := sqlexec.QueryList[E](ctx, session, statement, params, mapper)
	if err != nil {
		return nil, fmt.Errorf("sql query list: %w", err)
	}
	return items, nil
}

func SQLScalar[T any](ctx context.Context, session Session, statement sqlstmt.Source, params any) (T, error) {
	item, err := sqlexec.Scalar[T](ctx, session, statement, params)
	if err != nil {
		var zero T
		return zero, fmt.Errorf("sql scalar: %w", err)
	}
	return item, nil
}

func SQLScalarOption[T any](ctx context.Context, session Session, statement sqlstmt.Source, params any) (mo.Option[T], error) {
	item, err := sqlexec.ScalarOption[T](ctx, session, statement, params)
	if err != nil {
		return mo.None[T](), fmt.Errorf("sql scalar option: %w", err)
	}
	return item, nil
}

func StructMapperScanPlanForTest[E any](mapper StructMapper[E], columns []string) error {
	if err := mapper.ScanPlan(columns); err != nil {
		return fmt.Errorf("struct mapper scan plan: %w", err)
	}
	return nil
}
