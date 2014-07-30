package filters

import (
	"bytes"
	"github.com/coopernurse/gorp"
	"reflect"
)

type SqlWriter interface {
	// ActualValue should return the value to be used as a value or
	// column in the SQL query.
	ActualValue() interface{}

	// WriteSql should take the generated string that is being used to
	// represent the ActualValue() in the query, and wrap it in
	// whatever SQL this SqlWriter needs to add to the query.
	WriteSql(string) string
}

// A TableAndColumnLocater takes a struct field reference and returns
// the column for that field, complete with table name.
type TableAndColumnLocater interface {
	// LocateColumnWithTable should do the same thing as LocateColumn,
	// but also include the table name.
	LocateTableAndColumn(fieldPtr interface{}) (string, error)
}

// A Filter is a type that can be used as a sub-section of a where
// clause.
type Filter interface {
	// Where should take a TableAndColumnLocater, a dialect, and the index
	// to start binding at, and return the string to be added to the
	// where clause, a slice of query arguments in the where clause,
	// and any errors encountered.
	Where(structMap TableAndColumnLocater, dialect gorp.Dialect, startBindIdx int) (string, []interface{}, error)
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

// joinFilters joins all of the sub-filters' where clauses into a
// single where clause.
func (filter *CombinedFilter) joinFilters(separator string, structMap TableAndColumnLocater, dialect gorp.Dialect, startBindIdx int) (string, []interface{}, error) {
	buffer := bytes.Buffer{}
	args := make([]interface{}, 0, len(filter.subFilters))
	if len(filter.subFilters) > 1 {
		buffer.WriteString("(")
	}
	for index, subFilter := range filter.subFilters {
		nextWhere, nextArgs, err := subFilter.Where(structMap, dialect, startBindIdx+len(args))
		if err != nil {
			return "", nil, err
		}
		args = append(args, nextArgs...)
		if index != 0 {
			buffer.WriteString(separator)
		}
		buffer.WriteString(nextWhere)
	}
	if len(filter.subFilters) > 1 {
		buffer.WriteString(")")
	}
	return buffer.String(), args, nil
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

func (filter *AndFilter) Where(structMap TableAndColumnLocater, dialect gorp.Dialect, startBindIdx int) (string, []interface{}, error) {
	return filter.joinFilters(" and ", structMap, dialect, startBindIdx)
}

// An OrFilter is a CombinedFilter that will have its sub-filters
// joined using OR.
type OrFilter struct {
	CombinedFilter
}

func (filter *OrFilter) Where(structMap TableAndColumnLocater, dialect gorp.Dialect, startBindIdx int) (string, []interface{}, error) {
	return filter.joinFilters(" or ", structMap, dialect, startBindIdx)
}

// A JoinFilter is an AndFilter used for JOIN clauses and other forms
// of multi-table filters.
type JoinFilter struct {
	AndFilter
	QuotedJoinTable string
}

// JoinClause on a JoinFilter will return the full join clause for use
// in a SELECT statement.
func (filter *JoinFilter) JoinClause(structMap TableAndColumnLocater, dialect gorp.Dialect, startBindIdx int) (string, []interface{}, error) {
	join := " inner join " + filter.QuotedJoinTable
	on, args, err := filter.AndFilter.Where(structMap, dialect, startBindIdx)
	if err != nil {
		return "", nil, err
	}
	if on != "" {
		join += " on " + on
	}
	return join, args, nil
}

// A ComparisonFilter is a filter that compares two values.
type ComparisonFilter struct {
	left       interface{}
	comparison string
	right      interface{}

	// Simply to make function definitions for helper functions
	// shorter
	structMap TableAndColumnLocater
	dialect gorp.Dialect
	sql bytes.Buffer
	args []interface{}
}

func (filter *ComparisonFilter) Where(structMap TableAndColumnLocater, dialect gorp.Dialect, startBindIdx int) (string, []interface{}, error) {
	filter.structMap = structMap
	filter.dialect = dialect
	filter.args = make([]interface{}, 0, 2)
	filter.sql = bytes.Buffer{}
	if err := filter.queryValue(filter.left, startBindIdx); err != nil {
		return "", nil, err
	}
	filter.sql.WriteString(filter.comparison)
	if err := filter.queryValue(filter.right, startBindIdx + len(filter.args)); err != nil {
		return "", nil, err
	}
	return filter.sql.String(), filter.args, nil
}

func (filter *ComparisonFilter) queryValue(columnOrValue interface{}, bindIdx int) (err error) {
	sqlWriter, isSqlWriter := columnOrValue.(SqlWriter)
	if isSqlWriter {
		columnOrValue = sqlWriter.ActualValue()
	}
	var sqlValue string
	if reflect.ValueOf(columnOrValue).Kind() == reflect.Ptr {
		sqlValue, err = filter.structMap.LocateTableAndColumn(columnOrValue)
		if err != nil {
			return err
		}
	} else {
		sqlValue = filter.dialect.BindVar(bindIdx)
		filter.args = append(filter.args, columnOrValue)
	}
	if isSqlWriter {
		sqlValue = sqlWriter.WriteSql(sqlValue)
	}
	filter.sql.WriteString(sqlValue)
	return
}

// A NotFilter is a filter that inverts another filter.
type NotFilter struct {
	filter Filter
}

func (filter *NotFilter) Where(structMap TableAndColumnLocater, dialect gorp.Dialect, startBindIdx int) (string, []interface{}, error) {
	whereStr, args, err := filter.filter.Where(structMap, dialect, startBindIdx)
	if err != nil {
		return "", nil, err
	}
	return "not " + whereStr, args, nil
}

// A NullFilter is a filter that compares a field to null
type NullFilter struct {
	addr interface{}
}

func (filter *NullFilter) Where(structMap TableAndColumnLocater, dialect gorp.Dialect, startBindIdx int) (string, []interface{}, error) {
	column, err := structMap.LocateTableAndColumn(filter.addr)
	if err != nil {
		return "", nil, err
	}
	return column + " is null", nil, nil
}

// A NotNullFilter is a filter that compares a field to null
type NotNullFilter struct {
	addr interface{}
}

func (filter *NotNullFilter) Where(structMap TableAndColumnLocater, dialect gorp.Dialect, startBindIdx int) (string, []interface{}, error) {
	column, err := structMap.LocateTableAndColumn(filter.addr)
	if err != nil {
		return "", nil, err
	}
	return column + " is not null", nil, nil
}

// A TrueFilter simply filters on a boolean column's truthiness.
type TrueFilter struct {
	addr interface{}
}

func (filter *TrueFilter) Where(structMap TableAndColumnLocater, dialect gorp.Dialect, startBindIdx int) (string, []interface{}, error) {
	column, err := structMap.LocateTableAndColumn(filter.addr)
	if err != nil {
		return "", nil, err
	}
	return column, nil, nil
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
	return &NullFilter{fieldPtr}
}

// NotNull returns a filter for fieldPtr IS NOT NULL
func NotNull(fieldPtr interface{}) Filter {
	return &NotNullFilter{fieldPtr}
}

// True returns a filter for fieldPtr's truthiness
func True(fieldPtr interface{}) Filter {
	return &TrueFilter{fieldPtr}
}

// False returns a filter for NOT fieldPtr
func False(fieldPtr interface{}) Filter {
	return Not(True(fieldPtr))
}

// Equal returns a filter for fieldPtr == value
func Equal(fieldPtr interface{}, value interface{}) Filter {
	return &ComparisonFilter{
		left: fieldPtr,
		comparison: "=",
		right: value,
	}
}

// NotEqual returns a filter for fieldPtr != value
func NotEqual(fieldPtr interface{}, value interface{}) Filter {
	return &ComparisonFilter{
		left: fieldPtr,
		comparison: "<>",
		right: value,
	}
}

// Less returns a filter for fieldPtr < value
func Less(fieldPtr interface{}, value interface{}) Filter {
	return &ComparisonFilter{
		left: fieldPtr,
		comparison: "<",
		right: value,
	}
}

// LessOrEqual returns a filter for fieldPtr <= value
func LessOrEqual(fieldPtr interface{}, value interface{}) Filter {
	return &ComparisonFilter{
		left: fieldPtr,
		comparison: "<=",
		right: value,
	}
}

// Greater returns a filter for fieldPtr > value
func Greater(fieldPtr interface{}, value interface{}) Filter {
	return &ComparisonFilter{
		left: fieldPtr,
		comparison: "=",
		right: value,
	}
}

// GreaterOrEqual returns a filter for fieldPtr >= value
func GreaterOrEqual(fieldPtr interface{}, value interface{}) Filter {
	return &ComparisonFilter{
		left: fieldPtr,
		comparison: "=",
		right: value,
	}
}
