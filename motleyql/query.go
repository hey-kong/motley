package motleyql

// Plan represents a parsed query
type Plan struct {
	Type         Type
	Fields       []string
	ModelZoo     string
	Conditions   []Condition
	OrderByItems []string
	Desc         bool
	Count        int
	Data         string
	Mode         string
}

// Type is the type of SQL query, e.g. SELECT
type Type int

const (
	// UnknownType is the zero value for a Type
	UnknownType Type = iota
	// Select represents a SELECT query
	Select
)

// TypeString is a string slice with the names of all types in order
var TypeString = []string{
	"UnknownType",
	"Select",
}

// DataType is the type of input data, e.g. Image
type DataType int

const (
	Image DataType = iota
	Text
	Speech
)

// DataTypeString is a string slice with the names of all data types in order
var DataTypeString = []string{
	"image",
	"text",
	"speech",
}

// Operator is between operands in a condition
type Operator int

const (
	// UnknownOperator is the zero value for an Operator
	UnknownOperator Operator = iota
	// Eq -> "="
	Eq
	// Ne -> "!="
	Ne
	// Gt -> ">"
	Gt
	// Lt -> "<"
	Lt
	// Gte -> ">="
	Gte
	// Lte -> "<="
	Lte
)

// OperatorString is a string slice with the names of all operators in order
var OperatorString = []string{
	"UnknownOperator",
	"Eq",
	"Ne",
	"Gt",
	"Lt",
	"Gte",
	"Lte",
}

// Condition is a single boolean condition in a WHERE clause
type Condition struct {
	// Operand1 is the left hand side operand
	Operand1 string
	// Operand1IsField determines if Operand1 is a literal or a field name
	Operand1IsField bool
	// Operator is e.g. "=", ">"
	Operator Operator
	// Operand1 is the right hand side operand
	Operand2 string
	// Operand2IsField determines if Operand2 is a literal or a field name
	Operand2IsField bool
}
