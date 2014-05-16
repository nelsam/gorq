package gorp_queries

import (
	"bytes"
	"reflect"
	"github.com/coopernurse/gorp"
)

// A Filter is a type that can be used as a sub-section of a where
// clause.
type Filter interface {
	// Where should take a structColumnMap, a dialect, and the index
	// to start binding at, and return the string to be added to the
	// where clause, a slice of query arguments in the where clause,
	// and any errors encountered.
	Where(structMap structColumnMap, dialect gorp.Dialect, startBindIdx int) (string, []interface{}, error)
}

// A MultiFilter is a filter that can also accept additional filters.
type MultiFilter interface {
	Filter
	Add(filters ...Filter)
}

// A combinedFilter is a filter that has more than one sub-filter.
// This is mainly for things like AND or OR operations.
type combinedFilter struct {
	subFilters []Filter
}

// joinFilters joins all of the sub-filters' where clauses into a
// single where clause.
func (filter *combinedFilter) joinFilters(separator string, structMap structColumnMap, dialect gorp.Dialect, startBindIdx int) (string, []interface{}, error) {
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
func (filter *combinedFilter) Add(filters ...Filter) {
	filter.subFilters = append(filter.subFilters, filters...)
}

// An andFilter is a combinedFilter that will have its sub-filters
// joined using AND.
type andFilter struct {
	combinedFilter
}

func (filter *andFilter) Where(structMap structColumnMap, dialect gorp.Dialect, startBindIdx int) (string, []interface{}, error) {
	return filter.joinFilters(" and ", structMap, dialect, startBindIdx)
}

// An orFilter is a combinedFilter that will have its sub-filters
// joined using OR.
type orFilter struct {
	combinedFilter
}

func (filter *orFilter) Where(structMap structColumnMap, dialect gorp.Dialect, startBindIdx int) (string, []interface{}, error) {
	return filter.joinFilters(" or ", structMap, dialect, startBindIdx)
}

// A joinFilter is an andFilter used for ON clauses.  It contains the
// name of the table that this filter is for, to make generating a
// join clause simple.
type joinFilter struct {
	andFilter
	quotedJoinTable string
}

// JoinClause on a joinFilter will return the full join clause for use
// in a SELECT statement.
func (filter *joinFilter) JoinClause(structMap structColumnMap, dialect gorp.Dialect, startBindIdx int) (string, []interface{}, error) {
	join := " inner join " + filter.quotedJoinTable
	on, args, err := filter.andFilter.Where(structMap, dialect, startBindIdx)
	if err != nil {
		return "", nil, err
	}
	if on != "" {
		join += " on " + on
	}
	return join, args, nil
}

// A comparisonFilter is a filter that compares two values.
type comparisonFilter struct {
	left       interface{}
	comparison string
	right      interface{}
}

func (filter *comparisonFilter) Where(structMap structColumnMap, dialect gorp.Dialect, startBindIdx int) (string, []interface{}, error) {
	args := make([]interface{}, 0, 2)
	comparison := bytes.Buffer{}
	if reflect.ValueOf(filter.left).Kind() == reflect.Ptr {
		column, err := structMap.tableColumnForPointer(filter.left)
		if err != nil {
			return "", nil, err
		}
		comparison.WriteString(column)
	} else {
		bindVar := dialect.BindVar(startBindIdx + len(args))
		comparison.WriteString(bindVar)
		args = append(args, filter.left)
	}
	comparison.WriteString(filter.comparison)
	if reflect.ValueOf(filter.right).Kind() == reflect.Ptr {
		column, err := structMap.tableColumnForPointer(filter.right)
		if err != nil {
			return "", nil, err
		}
		comparison.WriteString(column)
	} else {
		bindVar := dialect.BindVar(startBindIdx + len(args))
		comparison.WriteString(bindVar)
		args = append(args, filter.right)
	}
	return comparison.String(), args, nil
}

// A notFilter is a filter that inverts another filter.
type notFilter struct {
	filter Filter
}

func (filter *notFilter) Where(structMap structColumnMap, dialect gorp.Dialect, startBindIdx int) (string, []interface{}, error) {
	whereStr, args, err := filter.filter.Where(structMap, dialect, startBindIdx)
	if err != nil {
		return "", nil, err
	}
	return "NOT " + whereStr, args, nil
}

// A nullFilter is a filter that compares a field to null
type nullFilter struct {
	addr interface{}
}

func (filter *nullFilter) Where(structMap structColumnMap, dialect gorp.Dialect, startBindIdx int) (string, []interface{}, error) {
	column, err := structMap.tableColumnForPointer(filter.addr)
	if err != nil {
		return "", nil, err
	}
	return column + " IS NULL", nil, nil
}

// A notNullFilter is a filter that compares a field to null
type notNullFilter struct {
	addr interface{}
}

func (filter *notNullFilter) Where(structMap structColumnMap, dialect gorp.Dialect, startBindIdx int) (string, []interface{}, error) {
	column, err := structMap.tableColumnForPointer(filter.addr)
	if err != nil {
		return "", nil, err
	}
	return column + " IS NOT NULL", nil, nil
}

// Or returns a filter that will OR all passed in filters
func Or(filters ...Filter) Filter {
	return &orFilter{combinedFilter{filters}}
}

// And returns a filter that will AND all passed in filters
func And(filters ...Filter) Filter {
	return &andFilter{combinedFilter{filters}}
}

// Not returns a filter that will NOT the passed in filter
func Not(filter Filter) Filter {
	return &notFilter{filter}
}

// Null returns a filter for fieldPtr IS NULL
func Null(fieldPtr interface{}) Filter {
	return &nullFilter{fieldPtr}
}

// NotNull returns a filter for fieldPtr IS NOT NULL
func NotNull(fieldPtr interface{}) Filter {
	return &notNullFilter{fieldPtr}
}

// Equal returns a filter for fieldPtr == value
func Equal(fieldPtr interface{}, value interface{}) Filter {
	return &comparisonFilter{fieldPtr, "=", value}
}

// NotEqual returns a filter for fieldPtr != value
func NotEqual(fieldPtr interface{}, value interface{}) Filter {
	return &comparisonFilter{fieldPtr, "<>", value}
}

// Less returns a filter for fieldPtr < value
func Less(fieldPtr interface{}, value interface{}) Filter {
	return &comparisonFilter{fieldPtr, "<", value}
}

// LessOrEqual returns a filter for fieldPtr <= value
func LessOrEqual(fieldPtr interface{}, value interface{}) Filter {
	return &comparisonFilter{fieldPtr, "<=", value}
}

// Greater returns a filter for fieldPtr > value
func Greater(fieldPtr interface{}, value interface{}) Filter {
	return &comparisonFilter{fieldPtr, "=", value}
}

// GreaterOrEqual returns a filter for fieldPtr >= value
func GreaterOrEqual(fieldPtr interface{}, value interface{}) Filter {
	return &comparisonFilter{fieldPtr, "=", value}
}
