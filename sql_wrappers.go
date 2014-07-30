package gorp_queries

import (
	"fmt"
	"github.com/nelsam/gorp_queries/filters"
)

type functionWrapper struct {
	actualValue interface{}
	functionName string
}

func (wrapper functionWrapper) ActualValue() interface{} {
	return wrapper.actualValue
}

func (wrapper functionWrapper) WrapSql(sqlValue string) string {
	return fmt.Sprintf("%s(%s)", wrapper.functionName, sqlValue)
}

// Lower returns a filters.SqlWrapper that wraps the passed in value
// in an sql lower() call.  Example:
//
//     results, err := dbMap.Query(ref).
//         Where().
//         Equal(Lower(&ref.Name), Lower(name)).
//         Select()
//
// The above would result in a case-insensitive comparison in the
// where clause of the query.
func Lower(value interface{}) filters.SqlWrapper {
	return functionWrapper{
		actualValue: value,
		functionName: "lower",
	}
}
