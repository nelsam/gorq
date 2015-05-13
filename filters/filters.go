package filters

import (
	"bytes"
	"strings"
)

// A TableAndColumnLocater takes a struct field reference and returns
// the column for that field, complete with table name.
type TableAndColumnLocater interface {
	// LocateColumnWithTable should do the same thing as LocateColumn,
	// but also include the table name.
	LocateTableAndColumn(fieldPtr interface{}) (string, error)
}

// A Filter is a type of MultiSqlWrapper, but is used explicitly in
// where clauses.  As such, its equivalent of WrapSql is named Where.
type Filter interface {
	// ActualValues returns a slice of all arguments in this filter.
	ActualValues() []interface{}

	// Where takes the string values that should be wrapped in the
	// query string, and returns the query string to use for this
	// filter.
	Where(...string) string
}

// A MultiFilter is a filter that can also accept additional filters.
type MultiFilter interface {
	Filter
	Add(filters ...Filter)
}

// A CombinedFilter is a filter that has more than one sub-filter.
// This is mainly for things like AND or OR operations.
type CombinedFilter struct {
	subFilters []Filter
}

func (filter *CombinedFilter) ActualValues() []interface{} {
	values := make([]interface{}, 0, len(filter.subFilters))
	for _, f := range filter.subFilters {
		values = append(values, f.ActualValues()...)
	}
	return values
}

func (filter *CombinedFilter) where(separator string, values ...string) string {
	buf := bytes.Buffer{}
	if len(filter.subFilters) > 1 {
		buf.WriteString("(")
	}
	index := 0
	for _, subFilter := range filter.subFilters {
		if index != 0 {
			buf.WriteString(separator)
		}
		end := index + len(subFilter.ActualValues())
		buf.WriteString(subFilter.Where(values[index:end]...))
		index = end
	}
	if len(filter.subFilters) > 1 {
		buf.WriteString(")")
	}
	return buf.String()
}

// Add adds one or more filters to the slice of sub-filters.
func (filter *CombinedFilter) Add(filters ...Filter) {
	filter.subFilters = append(filter.subFilters, filters...)
}

// An AndFilter is a CombinedFilter that will have its sub-filters
// joined using AND.
type AndFilter struct {
	CombinedFilter
}

func (filter *AndFilter) Where(values ...string) string {
	return filter.where(" and ", values...)
}

// An OrFilter is a CombinedFilter that will have its sub-filters
// joined using OR.
type OrFilter struct {
	CombinedFilter
}

func (filter *OrFilter) Where(values ...string) string {
	return filter.where(" or ", values...)
}

// An InFilter is a filter for value IN (list_of_values).
//
// TODO: InFilter should also support sub-selects, but it currently
// only supports lists of values.
type InFilter struct {
	expression interface{}
	valueList  []interface{}
}

func (filter *InFilter) ActualValues() []interface{} {
	values := make([]interface{}, 0, len(filter.valueList)+1)
	values = append(values, filter.expression)
	for _, v := range filter.valueList {
		values = append(values, v)
	}
	return values
}

func (filter *InFilter) Where(values ...string) string {
	return values[0] + " IN (" + strings.Join(values[1:], ", ") + ")"
}

// A JoinFilter is an AndFilter used for JOIN clauses and other forms
// of multi-table filters.
type JoinFilter struct {
	AndFilter
	QuotedJoinTable string
	Type            string
	QuotedAlias     string
}

// JoinClause on a JoinFilter will return the full join clause for use
// in a SELECT statement.
func (filter *JoinFilter) JoinClause(values ...string) string {
	join := filter.Type + " join " + filter.QuotedJoinTable
	if filter.QuotedAlias != "" && filter.QuotedAlias != "-" {
		join += " as " + filter.QuotedAlias
	}
	on := filter.AndFilter.Where(values...)
	if on != "" {
		join += " on " + on
	}
	return join
}

// A ComparisonFilter is a filter that compares two values.
type ComparisonFilter struct {
	Left       interface{}
	Comparison string
	Right      interface{}
}

func (filter *ComparisonFilter) ActualValues() []interface{} {
	return []interface{}{filter.Left, filter.Right}
}

func (filter *ComparisonFilter) Where(values ...string) string {
	return values[0] + filter.Comparison + values[1]
}

// A SingleFilter just deals with a single value, like with booleans
// or 'is not null' constraints.
type SingleFilter struct {
	field interface{}
}

func (filter *SingleFilter) ActualValues() []interface{} {
	return []interface{}{filter.field}
}

// A NotFilter is a filter that inverts another filter.
type NotFilter struct {
	filter Filter
}

func (filter *NotFilter) ActualValues() []interface{} {
	return filter.filter.ActualValues()
}

func (filter *NotFilter) Where(values ...string) string {
	return "not " + filter.filter.Where(values...)
}

// A NullFilter is a filter that compares a field to null
type NullFilter struct {
	SingleFilter
}

func (filter *NullFilter) Where(values ...string) string {
	return values[0] + " is null"
}

// A NotNullFilter is a filter that compares a field to null
type NotNullFilter struct {
	SingleFilter
}

func (filter *NotNullFilter) Where(values ...string) string {
	return values[0] + " is not null"
}

// A TrueFilter simply filters on a boolean column's truthiness.
type TrueFilter struct {
	SingleFilter
}

func (filter *TrueFilter) Where(values ...string) string {
	return values[0]
}

// Or returns a filter that will OR all passed in filters
func Or(filters ...Filter) Filter {
	return &OrFilter{CombinedFilter{filters}}
}

// And returns a filter that will AND all passed in filters
func And(filters ...Filter) Filter {
	return &AndFilter{CombinedFilter{filters}}
}

// Not returns a filter that will NOT the passed in filter
func Not(filter Filter) Filter {
	return &NotFilter{filter}
}

// Null returns a filter for fieldPtr IS NULL
func Null(fieldPtr interface{}) Filter {
	filter := &NullFilter{}
	filter.field = fieldPtr
	return filter
}

// NotNull returns a filter for fieldPtr IS NOT NULL
func NotNull(fieldPtr interface{}) Filter {
	filter := &NotNullFilter{}
	filter.field = fieldPtr
	return filter
}

// True returns a filter for fieldPtr's truthiness
func True(fieldPtr interface{}) Filter {
	filter := &TrueFilter{}
	filter.field = fieldPtr
	return filter
}

// False returns a filter for NOT fieldPtr
func False(fieldPtr interface{}) Filter {
	return Not(True(fieldPtr))
}

// In returns a filter for fieldPtr IN (values)
func In(fieldPtr interface{}, values ...interface{}) Filter {
	return &InFilter{
		expression: fieldPtr,
		valueList:  values,
	}
}

// Like returns a filter for fieldPtr LIKE pattern
func Like(fieldPtr interface{}, pattern string) Filter {
	return &ComparisonFilter{
		Left:       fieldPtr,
		Comparison: " like ",
		Right:      pattern,
	}
}

// Equal returns a filter for fieldPtr == value
func Equal(fieldPtr interface{}, value interface{}) Filter {
	return &ComparisonFilter{
		Left:       fieldPtr,
		Comparison: "=",
		Right:      value,
	}
}

// NotEqual returns a filter for fieldPtr != value
func NotEqual(fieldPtr interface{}, value interface{}) Filter {
	return &ComparisonFilter{
		Left:       fieldPtr,
		Comparison: "<>",
		Right:      value,
	}
}

// Less returns a filter for fieldPtr < value
func Less(fieldPtr interface{}, value interface{}) Filter {
	return &ComparisonFilter{
		Left:       fieldPtr,
		Comparison: "<",
		Right:      value,
	}
}

// LessOrEqual returns a filter for fieldPtr <= value
func LessOrEqual(fieldPtr interface{}, value interface{}) Filter {
	return &ComparisonFilter{
		Left:       fieldPtr,
		Comparison: "<=",
		Right:      value,
	}
}

// Greater returns a filter for fieldPtr > value
func Greater(fieldPtr interface{}, value interface{}) Filter {
	return &ComparisonFilter{
		Left:       fieldPtr,
		Comparison: ">",
		Right:      value,
	}
}

// GreaterOrEqual returns a filter for fieldPtr >= value
func GreaterOrEqual(fieldPtr interface{}, value interface{}) Filter {
	return &ComparisonFilter{
		Left:       fieldPtr,
		Comparison: ">=",
		Right:      value,
	}
}
