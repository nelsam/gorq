package plans

import (
	"errors"
	"reflect"

	"github.com/go-gorp/gorp"
	"github.com/nelsam/gorq/dialects"
	"github.com/nelsam/gorq/interfaces"
)

// Query generates a Query for a target model.  The target that is
// passed in must be a pointer to a struct, and will be used as a
// reference for query construction.
func Query(m *gorp.DbMap, exec gorp.SqlExecutor, target interface{}) interfaces.Query {
	// Handle non-standard dialects
	switch src := m.Dialect.(type) {
	case gorp.MySQLDialect:
		m.Dialect = dialects.MySQLDialect{src}
	case gorp.SqliteDialect:
		m.Dialect = dialects.SqliteDialect{src}
	default:
	}
	plan := &QueryPlan{
		dbMap:    m,
		executor: exec,
	}

	targetVal := reflect.ValueOf(target)
	if targetVal.Kind() != reflect.Ptr || targetVal.Elem().Kind() != reflect.Struct {
		plan.Errors = append(plan.Errors, errors.New("A query target must be a pointer to struct"))
	}
	targetTable, err := plan.mapTable(targetVal)
	if err != nil {
		plan.Errors = append(plan.Errors, err)
		return plan
	}
	plan.target = targetVal
	plan.table = targetTable
	return plan
}

func (plan *QueryPlan) mapTable(targetVal reflect.Value) (*gorp.TableMap, error) {
	if targetVal.Kind() != reflect.Ptr || targetVal.Elem().Kind() != reflect.Struct {
		return nil, errors.New("gorp: Cannot create query plan - target value must be a pointer to a struct")
	}

	if subQuery, ok := targetVal.Interface().(subQuery); ok {
		return plan.mapSubQuery(subQuery), nil
	}

	targetTable, err := plan.dbMap.TableFor(targetVal.Type().Elem(), false)
	if err != nil {
		return nil, err
	}

	if _, err = plan.mapColumns(targetTable, targetVal); err != nil {
		return nil, err
	}
	return targetTable, nil
}

// mapColumns creates a list of field addresses and column maps, to
// make looking up the column for a field address easier.  Note that
// it doesn't do any special handling for overridden fields, because
// passing the address of a field that has been overridden is
// difficult to do accidentally.
func (plan *QueryPlan) mapColumns(table *gorp.TableMap, value reflect.Value) (int, error) {
	value = value.Elem()
	valueType := value.Type()
	if plan.colMap == nil {
		plan.colMap = make(structColumnMap, 0, value.NumField())
	}
	queryableFields := 0
	quotedTableName := plan.dbMap.Dialect.QuotedTableForQuery(table.SchemaName, table.TableName)
	for i := 0; i < value.NumField(); i++ {
		fieldType := valueType.Field(i)
		fieldVal := value.Field(i)
		if fieldType.Anonymous {
			if fieldVal.Kind() != reflect.Ptr {
				fieldVal = fieldVal.Addr()
			} else if fieldVal.IsNil() {
				// embedded types must be initialized for querying
				fieldVal.Set(reflect.New(fieldVal.Type().Elem()))
			}
			count, _ := plan.mapColumns(table, fieldVal)
			queryableFields += count
		} else if fieldType.PkgPath == "" {
			col := table.ColMap(fieldType.Name)
			quotedCol := plan.dbMap.Dialect.QuoteField(col.ColumnName)
			fieldMap := fieldColumnMap{
				addr:         fieldVal.Addr().Interface(),
				column:       col,
				quotedTable:  quotedTableName,
				quotedColumn: quotedCol,
			}
			plan.colMap = append(plan.colMap, fieldMap)
			if !col.Transient {
				queryableFields++
			}
		}
	}
	if queryableFields == 0 {
		return 0, errors.New("No fields in the target struct are mappable.")
	}
	return queryableFields, nil
}
