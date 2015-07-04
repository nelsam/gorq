package plans

import (
	"errors"
	"reflect"
	"strings"

	"github.com/go-gorp/gorp"
	"github.com/nelsam/gorq/filters"
)

type order struct {
	fieldOrWrapper interface{}
	direction      string
}

func (o order) OrderBy(dialect gorp.Dialect, colMap structColumnMap, bindIdx int) (string, []interface{}, error) {
	var (
		wrapper      filters.SqlWrapper
		allFields    []interface{}
		multiWrapper filters.MultiSqlWrapper
	)
	switch t := o.fieldOrWrapper.(type) {
	case filters.SqlWrapper:
		wrapper = t
		allFields = []interface{}{wrapper.ActualValue()}
	case filters.MultiSqlWrapper:
		multiWrapper = t
		allFields = multiWrapper.ActualValues()
	default:
		allFields = []interface{}{o.fieldOrWrapper}
	}
	// OrderBy needs at least one reference to a column of some sort.
	fieldFound := false
	columnsAndFields := make([]string, 0, len(allFields))
	params := make([]interface{}, 0, len(allFields))
	for _, field := range allFields {
		if reflect.TypeOf(field).Kind() == reflect.Ptr {
			column, err := colMap.LocateTableAndColumn(field)
			if err != nil {
				return "", nil, err
			}
			columnsAndFields = append(columnsAndFields, column)
			fieldFound = true
		} else {
			columnsAndFields = append(columnsAndFields, dialect.BindVar(bindIdx))
			params = append(params, field)
			bindIdx++
		}
	}
	if !fieldFound {
		return "", nil, errors.New("OrderBy requires a pointer to a struct field or " +
			"a wrapper with at least one struct field pointer as an actual value.")
	}
	var orderStr string
	if wrapper != nil {
		orderStr = wrapper.WrapSql(columnsAndFields[0])
	} else if multiWrapper != nil {
		orderStr = multiWrapper.WrapSql(columnsAndFields...)
	} else {
		orderStr = columnsAndFields[0]
	}
	direction := strings.ToLower(o.direction)
	switch direction {
	case "asc", "desc":
		orderStr += " " + direction
	case "":
	default:
		return "", nil, errors.New(`gorp: Order by direction must be empty string, "asc", or "desc"`)
	}
	return orderStr, params, nil
}
