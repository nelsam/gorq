package plans

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/go-gorp/gorp"
)

type fieldColumnMap struct {
	// addr should be the address (pointer value) of the field within
	// the struct being used to construct this query.
	addr interface{}

	// column should be the column that matches the field that addr
	// points to.
	column *gorp.ColumnMap

	// quotedTable should be the pre-quoted table string for this
	// column.
	quotedTable string

	// quotedColumn should be the pre-quoted column string for this
	// column.
	quotedColumn string
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

// fieldMapForPointer takes a pointer to a struct field and returns
// the fieldColumnMap for that struct field.
func (structMap structColumnMap) fieldMapForPointer(fieldPtr interface{}) (*fieldColumnMap, error) {
	for _, fieldMap := range structMap {
		if fieldMap.addr == fieldPtr {
			if fieldMap.column.Transient {
				return nil, errors.New("gorp: Cannot run queries against transient columns")
			}
			return &fieldMap, nil
		}
	}
	fieldPtrVal := reflect.ValueOf(fieldPtr)
	addr, value := fieldPtrVal.Pointer(), fieldPtrVal.Elem().Interface()
	return nil, fmt.Errorf("gorp: Cannot find a field matching the passed in pointer %d (value %v)", addr, value)
}
