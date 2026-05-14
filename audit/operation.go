package audit

// Operation identifies the entity mutation recorded in an audit table.
type Operation string

const (
	OperationInsert Operation = "insert"
	OperationUpdate Operation = "update"
	OperationUpsert Operation = "upsert"
	OperationDelete Operation = "delete"
)
