package extensions

import (
	"fmt"
	"strings"

	"github.com/nelsam/gorq/filters"
)

func ILike(fieldPtr interface{}, pattern string) filters.Filter {
	return &filters.ComparisonFilter{
		Left:       fieldPtr,
		Comparison: " ilike ",
		Right:      pattern,
	}
}

type jsonObjectFieldTextWrapper struct {
	actualValue interface{}
	fields      []string
}

func (wrapper jsonObjectFieldTextWrapper) ActualValue() interface{} {
	return wrapper.actualValue
}

func (wrapper jsonObjectFieldTextWrapper) WrapSql(sqlValue string) string {
	objElementSelector := strings.Join(wrapper.fields, ",")
	return fmt.Sprintf("(%s::json#>>'{%s}')", sqlValue, objElementSelector)
}

type jsonObjectFieldWrapper struct {
	actualValue interface{}
	fields      []string
}

func (wrapper jsonObjectFieldWrapper) ActualValue() interface{} {
	return wrapper.actualValue
}

func (wrapper jsonObjectFieldWrapper) WrapSql(sqlValue string) string {
	objElementSelector := strings.Join(wrapper.fields, ",")
	return fmt.Sprintf("(%s::json#>'{%s}')", sqlValue, objElementSelector)
}

// JSONObjectFieldText returns a filters.SqlWrapper that wraps the
// passed in value in a JSON operator to query against elements
// (converted to text) in the json field.  It performs a type cast of
// the field to json, so columns which haven't been defined as json
// will work.
func JSONObjectFieldText(value interface{}, fields ...string) filters.SqlWrapper {
	return jsonObjectFieldTextWrapper{actualValue: value, fields: fields}
}

// JSONObjectField returns a filters.SqlWrapper that wraps the passed
// in value in a JSON operator to query against elements in the json
// field.  It performs a type cast of the field to json, so columns
// which haven't been defined as json will work.
func JSONObjectField(value interface{}, fields ...string) filters.SqlWrapper {
	return jsonObjectFieldWrapper{actualValue: value, fields: fields}
}
