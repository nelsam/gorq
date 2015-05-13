package plans

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/outdoorsy/gorp"
)

type fieldColumnMap struct {
	// parent is a pointer to the parent struct, used when calling
	// joinFunc.
	parent interface{}

	// field should be the address (pointer value) of the field within
	// the struct being used to construct this query.
	field interface{}

	// column should be the column that matches the field that addr
	// points to.
	column *gorp.ColumnMap

	// alias is used in the query as an alias for this column.
	alias string

	// prefix is used in the query when sub-fields of this field are
	// queried.
	prefix string

	// selectTarget is the value that should be used for the select
	// clause for this field.  If it is equal to field, the table and
	// column will be used; otherwise, this can be used to select
	// other values (and even things like CASE WHEN ... THEN ... ELSE
	// ... END) that will be assigned to this field.
	selectTarget interface{}

	// quotedTable should be the pre-quoted table string for this
	// column.
	quotedTable string

	// quotedColumn should be the pre-quoted column string for this
	// column.
	quotedColumn string

	// doSelect contains whether or not this column should be included
	// in the fields requested in a select statement.
	doSelect bool

	// joinOp stores the JoinFunc (if any) related to this field.
	join JoinFunc
}

type structColumnMap []fieldColumnMap

// LocateColumn takes an interface value (which should be a
// pointer to one of the fields on the value that is being used as a
// reference for query construction) and returns the pre-quoted column
// name that should be used to reference that value in queries.
func (structMap structColumnMap) LocateColumn(fieldPtr interface{}) (string, error) {
	fieldMap, err := structMap.fieldMapForPointer(fieldPtr)
	if err != nil {
		return "", err
	}
	return fieldMap.quotedColumn, nil
}

// LocateTableAndColumn takes an interface value (which should be a
// pointer to one of the fields on the value that is being used as a
// reference for query construction) and returns the pre-quoted
// table.column name that should be used to reference that value in
// some types of queries (mostly where statements and select queries).
func (structMap structColumnMap) LocateTableAndColumn(fieldPtr interface{}) (string, error) {
	fieldMap, err := structMap.fieldMapForPointer(fieldPtr)
	if err != nil {
		return "", err
	}
	return fieldMap.quotedTable + "." + fieldMap.quotedColumn, nil
}

func (structMap structColumnMap) joinMapForPointer(fieldPtr interface{}) (*fieldColumnMap, error) {
	for _, fieldMap := range structMap {
		if fieldMap.field == fieldPtr {
			return &fieldMap, nil
		}
	}
	fieldPtrVal := reflect.ValueOf(fieldPtr)
	addr, value := fieldPtrVal.Pointer(), fieldPtrVal.Elem().Interface()
	return nil, fmt.Errorf("gorp: Cannot find a field matching the passed in pointer %d (value %v)", addr, value)
}

// fieldMapForPointer takes a pointer to a struct field and returns
// the fieldColumnMap for that struct field.
func (structMap structColumnMap) fieldMapForPointer(fieldPtr interface{}) (*fieldColumnMap, error) {
	m, err := structMap.joinMapForPointer(fieldPtr)
	if err != nil {
		return nil, err
	}
	if m.column.Transient && m.field == m.selectTarget {
		return nil, errors.New("gorp: Cannot run queries against transient columns")
	}
	return m, nil
}
