package gorq

import (
	"bytes"
	"fmt"

	"github.com/nelsam/gorq/filters"
)

type functionWrapper struct {
	actualValue  interface{}
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
		actualValue:  value,
		functionName: "lower",
	}
}

// Count returns a filters.SqlWrapper that wraps the passed in value
// in an sql count() call.
func Count(value interface{}) filters.SqlWrapper {
	return functionWrapper{
		actualValue:  value,
		functionName: "COUNT",
	}
}

// whenValue represents a single "WHEN ... THEN ..." pair in a CASE
// WHEN clause.
type whenValue struct {
	when filters.Filter
	then interface{}
}

// A Case is a filters.MultiSqlWrapper for CASE WHEN ... THEN
// ... ELSE ... END logic.
type Case interface {
	filters.MultiSqlWrapper
	When(filters.Filter) Thener
	Else(interface{}) filters.MultiSqlWrapper
}

// Thener is a Case that is in a state where a WHEN expression has
// been supplied, so it needs a corresponding THEN expression.
type Thener interface {
	Then(interface{}) Case
}

// A CaseWhen is a type to hold details about a CASE WHEN expression.
type CaseWhen struct {
	whenValues []whenValue
	elseValue  interface{}
}

// ActualValues implements filters.MultiSqlWrapper.ActualValues.
func (c *CaseWhen) ActualValues() []interface{} {
	values := make([]interface{}, 0, len(c.whenValues)+1)
	for _, whenVal := range c.whenValues {
		values = append(values, whenVal.when.ActualValues()...)
		values = append(values, whenVal.then)
	}
	if c.elseValue != nil {
		values = append(values, c.elseValue)
	}
	return values
}

// WrapSql implements filters.MultiSqlWrapper.WrapSql.
func (c *CaseWhen) WrapSql(values ...string) string {
	buf := bytes.NewBufferString("CASE")
	var idx int
	for _, whenVal := range c.whenValues {
		buf.WriteString(" WHEN ")
		end := idx + len(whenVal.when.ActualValues())
		buf.WriteString(whenVal.when.Where(values[idx:end]...))
		idx = end
		buf.WriteString(" THEN ")
		buf.WriteString(values[idx])
		idx++
	}
	if c.elseValue != nil {
		buf.WriteString(" ELSE ")
		buf.WriteString(values[idx])
	}
	buf.WriteString(" END")
	return buf.String()
}

// When implements Case.When.
func (c *CaseWhen) When(filter filters.Filter) Thener {
	c.whenValues = append(c.whenValues, whenValue{
		when: filter,
	})
	return c
}

// Then implements Thener.Then.
func (c *CaseWhen) Then(value interface{}) Case {
	c.whenValues[len(c.whenValues)-1].then = value
	return c
}

// Else implements Case.Then.
func (c *CaseWhen) Else(value interface{}) filters.MultiSqlWrapper {
	c.elseValue = value
	return c
}

// When constructs a Case starting with a WHEN expression of the
// passed in filter, and returns the corresponding Thener.
//
// Example usage:
//
//     gorq.When(filters.Null(&foo.Bar)).Then(0).Else(&foo.Bar)
//
func When(comparison filters.Filter) Thener {
	return &CaseWhen{
		whenValues: []whenValue{{when: comparison}},
	}
}
