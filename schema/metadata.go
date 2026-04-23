package schema

import (
	"reflect"

	"github.com/arcgolabs/collectionx"
	"github.com/arcgolabs/dbx/idgen"
)

type ReferentialAction string

const (
	ReferentialNoAction   ReferentialAction = "NO ACTION"
	ReferentialRestrict   ReferentialAction = "RESTRICT"
	ReferentialCascade    ReferentialAction = "CASCADE"
	ReferentialSetNull    ReferentialAction = "SET NULL"
	ReferentialSetDefault ReferentialAction = "SET DEFAULT"
)

type ForeignKeyRef struct {
	TargetTable  string
	TargetColumn string
	OnDelete     ReferentialAction
	OnUpdate     ReferentialAction
}

type ColumnMeta struct {
	Name          string
	Table         string
	Alias         string
	FieldName     string
	GoType        reflect.Type
	SQLType       string
	PrimaryKey    bool
	AutoIncrement bool
	Nullable      bool
	Unique        bool
	Indexed       bool
	DefaultValue  string
	References    *ForeignKeyRef
	IDStrategy    idgen.Strategy
	UUIDVersion   string
}

type IndexMeta struct {
	Name    string
	Table   string
	Columns collectionx.List[string]
	Unique  bool
}

type PrimaryKeyMeta struct {
	Name    string
	Table   string
	Columns collectionx.List[string]
}

type ForeignKeyMeta struct {
	Name          string
	Table         string
	Columns       collectionx.List[string]
	TargetTable   string
	TargetColumns collectionx.List[string]
	OnDelete      ReferentialAction
	OnUpdate      ReferentialAction
}

type CheckMeta struct {
	Name       string
	Table      string
	Expression string
}

type RelationKind int

const (
	RelationBelongsTo RelationKind = iota
	RelationHasOne
	RelationHasMany
	RelationManyToMany
)

type RelationMeta struct {
	Name                string
	FieldName           string
	Kind                RelationKind
	SourceTable         string
	SourceAlias         string
	TargetTable         string
	LocalColumn         string
	TargetColumn        string
	ThroughTable        string
	ThroughLocalColumn  string
	ThroughTargetColumn string
	TargetType          reflect.Type
}
