package querydsl

type ComparisonOperator string

type LogicalOperator string

type JoinType string

type AggregateFunction string

const (
	OpEq    ComparisonOperator = "="
	OpNe    ComparisonOperator = "<>"
	OpGt    ComparisonOperator = ">"
	OpGe    ComparisonOperator = ">="
	OpLt    ComparisonOperator = "<"
	OpLe    ComparisonOperator = "<="
	OpIn    ComparisonOperator = "IN"
	OpLike  ComparisonOperator = "LIKE"
	OpIs    ComparisonOperator = "IS"
	OpIsNot ComparisonOperator = "IS NOT"
)

const (
	LogicalAnd LogicalOperator = "AND"
	LogicalOr  LogicalOperator = "OR"
)

const (
	InnerJoin JoinType = "INNER"
	LeftJoin  JoinType = "LEFT"
	RightJoin JoinType = "RIGHT"
)

const (
	AggCount AggregateFunction = "COUNT"
	AggSum   AggregateFunction = "SUM"
	AggAvg   AggregateFunction = "AVG"
	AggMin   AggregateFunction = "MIN"
	AggMax   AggregateFunction = "MAX"
)
