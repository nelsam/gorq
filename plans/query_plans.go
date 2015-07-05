package plans

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/go-gorp/gorp"
	"github.com/nelsam/gorq/filters"
	"github.com/nelsam/gorq/interfaces"
)

// BindVarPlaceholder is used as a placeholder string for bindVar
// strings.  Wherever it is used in a query, it should be replaced by
// the correct bind variable for the dialect in use.
const BindVarPlaceholder = "%s"

// A QueryPlan is a Query.  It returns itself on most method calls;
// the one exception is Assign(), which returns an AssignQueryPlan (a type of
// QueryPlan that implements AssignQuery instead of Query).  The return
// types of the methods on this struct help prevent silly errors like
// trying to run a SELECT statement that tries to Assign() values - that
// type of nonsense will result in compile errors.
//
// QueryPlans must be prepared and executed using an allocated struct
// as reference.  Again, this is intended to catch stupid mistakes
// (like typos in column names) at compile time.  Unfortunately, it
// makes the syntax a little unintuitive; but I haven't been able to
// come up with a better way to do it.
//
// For details about what you need in order to generate a query with
// this logic, see DbMap.Query().
type QueryPlan struct {
	// Errors is a slice of error valuues encountered during query
	// construction.  This is to allow cascading method calls, e.g.
	//
	//     someModel := new(OurModel)
	//     results, err := dbMap.Query(someModel).
	//         Where().
	//         Greater(&someModel.CreatedAt, yesterday).
	//         Less(&someModel.CreatedAt, time.Now()).
	//         Order(&someModel.CreatedAt, gorp.Descending).
	//         Select()
	//
	// The first time that a method call returns an error (most likely
	// Select(), Insert(), Delete(), or Update()), this field will be
	// checked for errors that occurred during query construction, and
	// if it is non-empty, the first error in the list will be
	// returned immediately.
	Errors []error

	table          *gorp.TableMap
	dbMap          *gorp.DbMap
	quotedTable    string
	executor       gorp.SqlExecutor
	target         reflect.Value
	colMap         structColumnMap
	joins          []*filters.JoinFilter
	assignCols     []string
	assignBindVars []string
	assignArgs     []interface{}
	filters        filters.MultiFilter
	orderBy        []order
	groupBy        []string
	limit          int64
	offset         int64
}

// Extend returns an extended query, using extensions for the
// gorp.Dialect stored as your dbmap's Dialect field.  You will need
// to use a type assertion on the return value.  As an example,
// postgresql supports a form of joining tables for use in an update
// statement.  You can still only *assign* values on the main
// reference table, but you can use values from other joined tables
// both during assignment and in the where clause.  Here's what it
// would look like:
//
//     updateCount, err := dbMap.Query(ref).Extend().(extensions.Postgres).
//         Assign(&ref.Date, time.Now()).
//         Join(mapRef).On().
//         Equal(&mapRef.Foreign, &ref.Id).
//         Update()
//
// If you want to make your own extensions, just make sure to register
// the constructor using RegisterExtension().
func (plan *QueryPlan) Extend() interface{} {
	extendedQuery, err := LoadExtension(plan.dbMap.Dialect, plan)
	if err != nil {
		plan.Errors = append(plan.Errors, err)
		return nil
	}
	return extendedQuery
}

// Assign sets up an assignment operation to assign the passed in
// value to the passed in field pointer.  This is used for creating
// UPDATE or INSERT queries.
func (plan *QueryPlan) Assign(fieldPtr interface{}, value interface{}) interfaces.AssignQuery {
	assignPlan := &AssignQueryPlan{QueryPlan: plan}
	return assignPlan.Assign(fieldPtr, value)
}

func (plan *QueryPlan) storeJoin() {
	if lastJoinFilter, ok := plan.filters.(*filters.JoinFilter); ok {
		if plan.joins == nil {
			plan.joins = make([]*filters.JoinFilter, 0, 2)
		}
		plan.joins = append(plan.joins, lastJoinFilter)
		plan.filters = nil
	}
}

func (plan *QueryPlan) JoinType(joinType string, target interface{}) (joinPlan interfaces.JoinQuery) {
	joinPlan = &JoinQueryPlan{QueryPlan: plan}
	plan.storeJoin()
	table, err := plan.mapTable(reflect.ValueOf(target))
	if err != nil {
		plan.Errors = append(plan.Errors, err)
		// Add a filter just so the rest of the query methods won't panic
		plan.filters = &filters.JoinFilter{Type: joinType, QuotedJoinTable: "Error: no table found"}
		return
	}
	quotedTable := plan.dbMap.Dialect.QuotedTableForQuery(table.SchemaName, table.TableName)
	plan.filters = &filters.JoinFilter{Type: joinType, QuotedJoinTable: quotedTable}
	return
}

func (plan *QueryPlan) Join(target interface{}) interfaces.JoinQuery {
	return plan.JoinType("INNER", target)
}

func (plan *QueryPlan) LeftJoin(target interface{}) interfaces.JoinQuery {
	return plan.JoinType("LEFT OUTER", target)
}

func (plan *QueryPlan) On(filters ...filters.Filter) interfaces.JoinQuery {
	plan.filters.Add(filters...)
	return &JoinQueryPlan{QueryPlan: plan}
}

// Where stores any join filter and allocates a new and filter to use
// for WHERE clause creation.  If you pass filters to it, they will be
// passed to plan.Filter().
func (plan *QueryPlan) Where(filterSlice ...filters.Filter) interfaces.WhereQuery {
	plan.storeJoin()
	plan.filters = new(filters.AndFilter)
	plan.Filter(filterSlice...)
	return plan
}

// Filter will add a Filter to the list of filters on this query.  The
// default method of combining filters on a query is by AND - if you
// want OR, you can use the following syntax:
//
//     query.Filter(gorp.Or(gorp.Equal(&field.Id, id), gorp.Less(&field.Priority, 3)))
//
func (plan *QueryPlan) Filter(filters ...filters.Filter) interfaces.WhereQuery {
	plan.filters.Add(filters...)
	return plan
}

// In adds a column IN (values...) comparison to the where clause.
func (plan *QueryPlan) In(fieldPtr interface{}, values ...interface{}) interfaces.WhereQuery {
	return plan.Filter(filters.In(fieldPtr, values...))
}

// InSubQuery adds a column IN (subQuery) comparison to the where
// clause.
func (plan *QueryPlan) InSubQuery(fieldPtr interface{}, subQuery filters.SubQuery) interfaces.WhereQuery {
	return plan.Filter(filters.InSubQuery(fieldPtr, subQuery))
}

// Like adds a column LIKE pattern comparison to the where clause.
func (plan *QueryPlan) Like(fieldPtr interface{}, pattern string) interfaces.WhereQuery {
	return plan.Filter(filters.Like(fieldPtr, pattern))
}

// Equal adds a column = value comparison to the where clause.
func (plan *QueryPlan) Equal(fieldPtr interface{}, value interface{}) interfaces.WhereQuery {
	return plan.Filter(filters.Equal(fieldPtr, value))
}

// NotEqual adds a column != value comparison to the where clause.
func (plan *QueryPlan) NotEqual(fieldPtr interface{}, value interface{}) interfaces.WhereQuery {
	return plan.Filter(filters.NotEqual(fieldPtr, value))
}

// Less adds a column < value comparison to the where clause.
func (plan *QueryPlan) Less(fieldPtr interface{}, value interface{}) interfaces.WhereQuery {
	return plan.Filter(filters.Less(fieldPtr, value))
}

// LessOrEqual adds a column <= value comparison to the where clause.
func (plan *QueryPlan) LessOrEqual(fieldPtr interface{}, value interface{}) interfaces.WhereQuery {
	return plan.Filter(filters.LessOrEqual(fieldPtr, value))
}

// Greater adds a column > value comparison to the where clause.
func (plan *QueryPlan) Greater(fieldPtr interface{}, value interface{}) interfaces.WhereQuery {
	return plan.Filter(filters.Greater(fieldPtr, value))
}

// GreaterOrEqual adds a column >= value comparison to the where clause.
func (plan *QueryPlan) GreaterOrEqual(fieldPtr interface{}, value interface{}) interfaces.WhereQuery {
	return plan.Filter(filters.GreaterOrEqual(fieldPtr, value))
}

// Null adds a column IS NULL comparison to the where clause
func (plan *QueryPlan) Null(fieldPtr interface{}) interfaces.WhereQuery {
	return plan.Filter(filters.Null(fieldPtr))
}

// NotNull adds a column IS NOT NULL comparison to the where clause
func (plan *QueryPlan) NotNull(fieldPtr interface{}) interfaces.WhereQuery {
	return plan.Filter(filters.NotNull(fieldPtr))
}

// True adds a column comparison to the where clause (tests for
// column's truthiness)
func (plan *QueryPlan) True(fieldPtr interface{}) interfaces.WhereQuery {
	return plan.Filter(filters.True(fieldPtr))
}

// False adds a NOT column comparison to the where clause (tests for
// column's negated truthiness)
func (plan *QueryPlan) False(fieldPtr interface{}) interfaces.WhereQuery {
	return plan.Filter(filters.False(fieldPtr))
}

// OrderBy adds a column to the order by clause.  The direction is
// optional - you may pass in an empty string to order in the default
// direction for the given column.
func (plan *QueryPlan) OrderBy(fieldPtrOrWrapper interface{}, direction string) interfaces.SelectQuery {
	plan.orderBy = append(plan.orderBy, order{fieldPtrOrWrapper, direction})
	return plan
}

// DiscardOrderBy discards all entries in the order by clause.
func (plan *QueryPlan) DiscardOrderBy() interfaces.SelectQuery {
	plan.orderBy = []order{}
	return plan
}

// GroupBy adds a column to the group by clause.
func (plan *QueryPlan) GroupBy(fieldPtr interface{}) interfaces.SelectQuery {
	column, err := plan.colMap.LocateTableAndColumn(fieldPtr)
	if err != nil {
		plan.Errors = append(plan.Errors, err)
		return plan
	}
	plan.groupBy = append(plan.groupBy, column)
	return plan
}

// Limit sets the limit clause of the query.
func (plan *QueryPlan) Limit(limit int64) interfaces.SelectQuery {
	plan.limit = limit
	return plan
}

// DiscardLimit discards any previously set limit clause.
func (plan *QueryPlan) DiscardLimit() interfaces.SelectQuery {
	plan.limit = 0
	return plan
}

// Offset sets the offset clause of the query.
func (plan *QueryPlan) Offset(offset int64) interfaces.SelectQuery {
	plan.offset = offset
	return plan
}

// DiscardOffset discards any previously set offset clause.
func (plan *QueryPlan) DiscardOffset() interfaces.SelectQuery {
	plan.offset = 0
	return plan
}

// argOrColumn returns the string that should be used to represent a
// value in a query.  If the value is detected to be a field, an error
// will be returned if the field cannot be selected.  If the value is
// used as an argument, it will be appended to args and the returned
// string will be the bind value.
func (plan *QueryPlan) argOrColumn(value interface{}) (args []interface{}, sqlValue string, err error) {
	switch src := value.(type) {
	case filters.SqlWrapper:
		var wrapperVal string
		args, wrapperVal, err = plan.argOrColumn(value)
		if err != nil {
			return nil, "", err
		}
		return args, src.WrapSql(wrapperVal), nil
	case filters.MultiSqlWrapper:
		values := src.ActualValues()
		wrapperVals := make([]string, 0, len(values))
		args := make([]interface{}, 0, len(values))
		for _, val := range values {
			newArgs, wrapperVal, err := plan.argOrColumn(val)
			if err != nil {
				return nil, "", err
			}
			wrapperVals = append(wrapperVals, wrapperVal)
			args = append(args, newArgs...)
		}
		return args, src.WrapSql(wrapperVals...), nil
	default:
		if reflect.TypeOf(value).Kind() == reflect.Ptr {
			sqlValue, err = plan.colMap.LocateTableAndColumn(value)
		} else {
			sqlValue = BindVarPlaceholder
			args = append(args, value)
		}
	}
	return
}

// Truncate will run this query plan as a TRUNCATE TABLE statement.
func (plan *QueryPlan) Truncate() error {
	query := fmt.Sprintf("TRUNCATE TABLE %s", plan.QuotedTable())
	_, err := plan.dbMap.Exec(query)
	return err
}

func (plan *QueryPlan) bindVars(statement *Statement) []string {
	bindVars := make([]string, 0, len(statement.args))
	for i := range statement.args {
		bindVars = append(bindVars, plan.dbMap.Dialect.BindVar(i))
	}
	return bindVars
}

func (plan *QueryPlan) runSelect(target interface{}) ([]interface{}, error) {
	statement, err := plan.SelectStatement()
	if err != nil {
		return nil, err
	}
	bindVars := plan.bindVars(statement)
	return plan.executor.Select(target, statement.Query(bindVars...), statement.args...)
}

// Select will run this query plan as a SELECT statement.
func (plan *QueryPlan) Select() ([]interface{}, error) {
	target := plan.target.Interface()
	if subQuery, ok := target.(subQuery); ok {
		target = subQuery.getTarget().Interface()
	}
	return plan.runSelect(target)
}

// SelectToTarget will run this query plan as a SELECT statement, and
// append results directly to the passed in slice pointer.
func (plan *QueryPlan) SelectToTarget(target interface{}) error {
	targetType := reflect.TypeOf(target)
	if targetType.Kind() != reflect.Ptr || targetType.Elem().Kind() != reflect.Slice {
		return errors.New("SelectToTarget must be run with a pointer to a slice as its target")
	}
	_, err := plan.runSelect(target)
	return err
}

func (plan *QueryPlan) Count() (int64, error) {
	statement := new(Statement)
	statement.query.WriteString("SELECT COUNT(*)")
	if err := plan.addSelectSuffix(statement); err != nil {
		return -1, err
	}
	bindVars := plan.bindVars(statement)
	return plan.executor.SelectInt(statement.Query(bindVars...), statement.args...)
}

func (plan *QueryPlan) QuotedTable() string {
	if plan.quotedTable == "" {
		plan.quotedTable = plan.dbMap.Dialect.QuotedTableForQuery(plan.table.SchemaName, plan.table.TableName)
	}
	return plan.quotedTable
}

// Insert will run this query plan as an INSERT statement.
func (plan *QueryPlan) Insert() error {
	if len(plan.Errors) > 0 {
		return plan.Errors[0]
	}
	statement := new(Statement)
	statement.query.WriteString("INSERT INTO ")
	statement.query.WriteString(plan.dbMap.Dialect.QuotedTableForQuery(plan.table.SchemaName, plan.table.TableName))
	statement.query.WriteString(" (")
	for i, col := range plan.assignCols {
		if i > 0 {
			statement.query.WriteString(", ")
		}
		statement.query.WriteString(col)
	}
	statement.query.WriteString(") VALUES (")
	for i, bindVar := range plan.assignBindVars {
		if i > 0 {
			statement.query.WriteString(", ")
		}
		statement.query.WriteString(bindVar)
	}
	statement.query.WriteString(")")
	_, err := plan.executor.Exec(statement.query.String(), statement.args...)
	return err
}

// Update will run this query plan as an UPDATE statement.
func (plan *QueryPlan) Update() (int64, error) {
	if len(plan.Errors) > 0 {
		return -1, plan.Errors[0]
	}
	statement := &Statement{
		args: plan.assignArgs,
	}
	statement.query.WriteString("UPDATE ")
	statement.query.WriteString(plan.dbMap.Dialect.QuotedTableForQuery(plan.table.SchemaName, plan.table.TableName))
	statement.query.WriteString(" SET ")
	for i, col := range plan.assignCols {
		if i > 0 {
			statement.query.WriteString(", ")
		}
		statement.query.WriteString(col)
		statement.query.WriteString("=")
		statement.query.WriteString(BindVarPlaceholder)
	}
	if err := plan.addWhereClause(statement); err != nil {
		return -1, err
	}
	bindVars := plan.bindVars(statement)
	res, err := plan.executor.Exec(statement.Query(bindVars...), statement.args...)
	if err != nil {
		return -1, err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return -1, err
	}
	return rows, nil
}

// Delete will run this query plan as a DELETE statement.
func (plan *QueryPlan) Delete() (int64, error) {
	if len(plan.Errors) > 0 {
		return -1, plan.Errors[0]
	}
	statement := new(Statement)
	statement.query.WriteString("DELETE FROM ")
	statement.query.WriteString(plan.dbMap.Dialect.QuotedTableForQuery(plan.table.SchemaName, plan.table.TableName))
	if err := plan.addWhereClause(statement); err != nil {
		return -1, err
	}
	bindVars := plan.bindVars(statement)
	res, err := plan.executor.Exec(statement.Query(bindVars...), statement.args...)
	if err != nil {
		return -1, err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return -1, err
	}
	return rows, nil
}

// A JoinQueryPlan is a QueryPlan, except with some return values
// changed so that it will match the JoinQuery interface.
type JoinQueryPlan struct {
	*QueryPlan
}

func (plan *JoinQueryPlan) In(fieldPtr interface{}, values ...interface{}) interfaces.JoinQuery {
	plan.QueryPlan.In(fieldPtr, values...)
	return plan
}

func (plan *JoinQueryPlan) InSubQuery(fieldPtr interface{}, subQuery filters.SubQuery) interfaces.JoinQuery {
	plan.QueryPlan.InSubQuery(fieldPtr, subQuery)
	return plan
}

func (plan *JoinQueryPlan) Like(fieldPtr interface{}, pattern string) interfaces.JoinQuery {
	plan.QueryPlan.Like(fieldPtr, pattern)
	return plan
}

func (plan *JoinQueryPlan) Equal(fieldPtr interface{}, value interface{}) interfaces.JoinQuery {
	plan.QueryPlan.Equal(fieldPtr, value)
	return plan
}

func (plan *JoinQueryPlan) NotEqual(fieldPtr interface{}, value interface{}) interfaces.JoinQuery {
	plan.QueryPlan.NotEqual(fieldPtr, value)
	return plan
}

func (plan *JoinQueryPlan) Less(fieldPtr interface{}, value interface{}) interfaces.JoinQuery {
	plan.QueryPlan.Less(fieldPtr, value)
	return plan
}

func (plan *JoinQueryPlan) LessOrEqual(fieldPtr interface{}, value interface{}) interfaces.JoinQuery {
	plan.QueryPlan.LessOrEqual(fieldPtr, value)
	return plan
}

func (plan *JoinQueryPlan) Greater(fieldPtr interface{}, value interface{}) interfaces.JoinQuery {
	plan.QueryPlan.Greater(fieldPtr, value)
	return plan
}

func (plan *JoinQueryPlan) GreaterOrEqual(fieldPtr interface{}, value interface{}) interfaces.JoinQuery {
	plan.QueryPlan.GreaterOrEqual(fieldPtr, value)
	return plan
}

func (plan *JoinQueryPlan) Null(fieldPtr interface{}) interfaces.JoinQuery {
	plan.QueryPlan.Null(fieldPtr)
	return plan
}

func (plan *JoinQueryPlan) NotNull(fieldPtr interface{}) interfaces.JoinQuery {
	plan.QueryPlan.NotNull(fieldPtr)
	return plan
}

func (plan *JoinQueryPlan) True(fieldPtr interface{}) interfaces.JoinQuery {
	plan.QueryPlan.True(fieldPtr)
	return plan
}

func (plan *JoinQueryPlan) False(fieldPtr interface{}) interfaces.JoinQuery {
	plan.QueryPlan.False(fieldPtr)
	return plan
}

// An AssignQueryPlan is, for all intents and purposes, a QueryPlan.
// The only difference is the return type of Where() and all of the
// various where clause operations.  This is intended to be used for
// queries that have had Assign() called, to make it a compile error
// if you try to call Select() on a query that has had both Assign()
// and Where() called.
//
// All documentation for QueryPlan applies to AssignQueryPlan, too.
type AssignQueryPlan struct {
	*QueryPlan
}

func (plan *AssignQueryPlan) Assign(fieldPtr interface{}, value interface{}) interfaces.AssignQuery {
	column, err := plan.colMap.LocateColumn(fieldPtr)
	if err != nil {
		plan.Errors = append(plan.Errors, err)
		return plan
	}
	plan.assignCols = append(plan.assignCols, column)
	plan.assignBindVars = append(plan.assignBindVars, plan.dbMap.Dialect.BindVar(len(plan.assignBindVars)))
	plan.assignArgs = append(plan.assignArgs, value)
	return plan
}

func (plan *AssignQueryPlan) Where(filters ...filters.Filter) interfaces.UpdateQuery {
	plan.QueryPlan.Where(filters...)
	return plan
}

func (plan *AssignQueryPlan) Filter(filters ...filters.Filter) interfaces.UpdateQuery {
	plan.QueryPlan.Filter(filters...)
	return plan
}

func (plan *AssignQueryPlan) In(fieldPtr interface{}, values ...interface{}) interfaces.UpdateQuery {
	plan.QueryPlan.In(fieldPtr, values...)
	return plan
}

func (plan *AssignQueryPlan) InSubQuery(fieldPtr interface{}, subQuery filters.SubQuery) interfaces.UpdateQuery {
	plan.QueryPlan.InSubQuery(fieldPtr, subQuery)
	return plan
}

func (plan *AssignQueryPlan) Like(fieldPtr interface{}, pattern string) interfaces.UpdateQuery {
	plan.QueryPlan.Like(fieldPtr, pattern)
	return plan
}

func (plan *AssignQueryPlan) Equal(fieldPtr interface{}, value interface{}) interfaces.UpdateQuery {
	plan.QueryPlan.Equal(fieldPtr, value)
	return plan
}

func (plan *AssignQueryPlan) NotEqual(fieldPtr interface{}, value interface{}) interfaces.UpdateQuery {
	plan.QueryPlan.NotEqual(fieldPtr, value)
	return plan
}

func (plan *AssignQueryPlan) Less(fieldPtr interface{}, value interface{}) interfaces.UpdateQuery {
	plan.QueryPlan.Less(fieldPtr, value)
	return plan
}

func (plan *AssignQueryPlan) LessOrEqual(fieldPtr interface{}, value interface{}) interfaces.UpdateQuery {
	plan.QueryPlan.LessOrEqual(fieldPtr, value)
	return plan
}

func (plan *AssignQueryPlan) Greater(fieldPtr interface{}, value interface{}) interfaces.UpdateQuery {
	plan.QueryPlan.Greater(fieldPtr, value)
	return plan
}

func (plan *AssignQueryPlan) GreaterOrEqual(fieldPtr interface{}, value interface{}) interfaces.UpdateQuery {
	plan.QueryPlan.GreaterOrEqual(fieldPtr, value)
	return plan
}

func (plan *AssignQueryPlan) Null(fieldPtr interface{}) interfaces.UpdateQuery {
	plan.QueryPlan.Null(fieldPtr)
	return plan
}

func (plan *AssignQueryPlan) NotNull(fieldPtr interface{}) interfaces.UpdateQuery {
	plan.QueryPlan.NotNull(fieldPtr)
	return plan
}

func (plan *AssignQueryPlan) True(fieldPtr interface{}) interfaces.UpdateQuery {
	plan.QueryPlan.True(fieldPtr)
	return plan
}

func (plan *AssignQueryPlan) False(fieldPtr interface{}) interfaces.UpdateQuery {
	plan.QueryPlan.False(fieldPtr)
	return plan
}
