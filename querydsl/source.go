package querydsl

type TableSource interface {
	TableName() string
	TableAlias() string
}
