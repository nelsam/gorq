package plans

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"

	"github.com/outdoorsy/gorp"
	"github.com/outdoorsy/gorq/dialects"
	"github.com/outdoorsy/gorq/filters"
	"github.com/outdoorsy/gorq/interfaces"
)

type HasPostDirectUpdate interface {
	PostDirectUpdate(gorp.SqlExecutor) error
}

var bufPool = sync.Pool{
	New: func() interface{} {
		return &bytes.Buffer{}
	},
}

type tableAlias struct {
	*gorp.TableMap
	quotedFromClause string
	dialect          gorp.Dialect
}

func (t tableAlias) tableForFromClause() string {
	if t.quotedFromClause != "" {
		return t.quotedFromClause
	}
	return t.dialect.QuotedTableForQuery(t.SchemaName, t.TableName)
}

// subQuery is provided to use plan types as sub-queries in from/join
// clauses.
type subQuery interface {
	QuotedTable() string
	getTable() *gorp.TableMap
	getTarget() reflect.Value
	getColMap() structColumnMap
	errors() []error
	selectQuery() (string, error)
	getArgs() []interface{}
}

// UnmappedSubQuery is an interface that subqueries which do not have
// access to details about the table and struct field maps may
// implement.
type UnmappedSubQuery interface {
	Target() interface{}
	SelectQuery(table *gorp.TableMap, col *gorp.ColumnMap, tableAlias string, tablePrefix string) (query string, columns []string)
}

type JoinFunc func(parent, field interface{}) (joinType string, joinTarget, selectionField interface{}, constraints []filters.Filter)

type JoinOp struct {
	Table  *gorp.TableMap
	Column *gorp.ColumnMap
	Join   JoinFunc
}

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
	lastRefs       []filters.Filter
	assignCols     []string
	assignBindVars []string
	assignArgs     []interface{}
	filters        filters.MultiFilter
	orderBy        []order
	groupBy        []string
	limit          int64
	offset         int64
	args           []interface{}
	argLen         int
	argLock        sync.RWMutex
	tables         []*gorp.TableMap
	distinctFields []interface{}
	forUpdate      bool
	forUpdateOf    string
}

// Query generates a Query for a target model.  The target that is
// passed in must be a pointer to a struct, and will be used as a
// reference for query construction.
func Query(m *gorp.DbMap, exec gorp.SqlExecutor, target interface{}, joinOps ...JoinOp) interfaces.Query {
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
	targetTable, _, err := plan.mapTable(targetVal, joinOps...)
	if err != nil {
		plan.Errors = append(plan.Errors, err)
		return plan
	}
	plan.target = targetVal
	plan.table = targetTable.TableMap
	plan.quotedTable = targetTable.tableForFromClause()
	return plan
}

func (plan *QueryPlan) getTarget() reflect.Value {
	return plan.target
}

func (plan *QueryPlan) getColMap() structColumnMap {
	return plan.colMap
}

func (plan *QueryPlan) errors() []error {
	return plan.Errors
}

func (plan *QueryPlan) getArgs() []interface{} {
	plan.argLock.RLock()
	args := make([]interface{}, len(plan.args))
	// have to copy the args into a new array here as this array is manipulated
	// in code outside our control.
	for i, v := range plan.args {
		args[i] = v
	}
	plan.argLock.RUnlock()
	return args
}
func (plan *QueryPlan) appendArgs(args ...interface{}) {
	plan.argLock.Lock()
	plan.args = append(plan.args, args...)
	plan.argLen = len(plan.args)
	plan.argLock.Unlock()
}
func (plan *QueryPlan) resetArgs() {
	plan.argLock.Lock()
	plan.args = nil
	if len(plan.assignArgs) > 0 {
		plan.args = append(plan.args, plan.assignArgs...)
	}
	if subQuery, ok := plan.target.Interface().(subQuery); ok {
		plan.args = append(plan.args, subQuery.getArgs()...)
	}
	plan.argLen = len(plan.args)
	plan.argLock.Unlock()
}

func (plan *QueryPlan) getTable() *gorp.TableMap {
	return plan.table
}

func (plan *QueryPlan) mapSubQuery(q subQuery) *tableAlias {
	if len(q.errors()) != 0 {
		plan.Errors = append(plan.Errors, q.errors()...)
	}
	query, err := q.selectQuery()
	if err != nil {
		plan.Errors = append(plan.Errors, err)
	}
	alias := q.QuotedTable()
	quotedFromClause := fmt.Sprintf("(%s) as %s", query, alias)
	for _, m := range q.getColMap() {
		m.quotedTable = alias
		plan.colMap = append(plan.colMap, m)
	}
	return &tableAlias{TableMap: q.getTable(), dialect: plan.dbMap.Dialect, quotedFromClause: quotedFromClause}
}

func (plan *QueryPlan) mapTable(targetVal reflect.Value, joinOps ...JoinOp) (*tableAlias, string, error) {
	if targetVal.Kind() != reflect.Ptr {
		return nil, "", errors.New("All query targets must be pointer types")
	}

	if subQuery, ok := targetVal.Interface().(subQuery); ok {
		// This is one of our QueryPlan types, or an extended version
		// of one of them.
		return plan.mapSubQuery(subQuery), subQuery.QuotedTable(), nil
	}

	// UnmappedSubQuery types are for user-generated sub-queries, so
	// we still have to do a fair bit of mapping work.
	subQuery, isSubQuery := targetVal.Interface().(UnmappedSubQuery)
	if isSubQuery {
		targetVal = reflect.ValueOf(subQuery.Target())
	}

	var prefix, alias string
	if plan.table != nil {
		prefix = "-"
		alias = "-"
	}
	var (
		targetTable *gorp.TableMap
		joinColumn  *gorp.ColumnMap
	)
	parentMap, err := plan.colMap.joinMapForPointer(targetVal.Interface())
	if err == nil && parentMap.column.TargetTable() != nil {
		prefix, alias = parentMap.prefix, parentMap.alias
		joinColumn = parentMap.column
		targetTable = parentMap.column.TargetTable()
	}

	// targetVal could feasibly be a slice or array, to store
	// *-to-many results in.
	elemType := targetVal.Type().Elem()
	if elemType.Kind() == reflect.Slice || elemType.Kind() == reflect.Array {
		targetVal = targetVal.Elem()
		if targetVal.IsNil() {
			targetVal.Set(reflect.MakeSlice(elemType, 0, 1))
		}
		if targetVal.Len() == 0 {
			newElem := reflect.New(elemType.Elem()).Elem()
			if newElem.Kind() == reflect.Ptr {
				newElem.Set(reflect.New(newElem.Type().Elem()))
			}
			targetVal.Set(reflect.Append(targetVal, newElem))
		}
		targetVal = targetVal.Index(0)
		if targetVal.Kind() != reflect.Ptr {
			targetVal = targetVal.Addr()
		}
	}
	// It could also be a pointer to a pointer to a struct, if the
	// struct field is a pointer, itself.  This is *only* allowed when
	// the passed in value mapped to a field, though, so targetTable
	// must already be set.
	if targetTable != nil && elemType.Kind() == reflect.Ptr {
		targetVal = targetVal.Elem()
		if targetVal.IsNil() {
			targetVal.Set(reflect.New(targetVal.Type().Elem()))
		}
	}

	if targetVal.Elem().Kind() != reflect.Struct {
		return nil, "", errors.New("gorp: Cannot create query plan - no struct found to map to")
	}

	if targetTable == nil {
		targetTable, err = plan.dbMap.TableFor(targetVal.Type().Elem(), false)
		if err != nil {
			return nil, "", err
		}
	}
	plan.tables = append(plan.tables, targetTable)

	plan.lastRefs = make([]filters.Filter, 0, 2)

	if isSubQuery {
		// Get the columns from the sub-query's select statement.
		query, columns := subQuery.SelectQuery(targetTable, joinColumn, alias, prefix)
		for _, field := range plan.colMap {
			for i, colName := range columns {
				switch colName {
				case field.column.ColumnName, field.column.JoinAlias():
					field.alias = colName
					columns = append(columns[:i], columns[i+1:]...)
					break
				}
			}
		}
		return &tableAlias{TableMap: targetTable, dialect: plan.dbMap.Dialect, quotedFromClause: query}, alias, nil
	}
	if err = plan.mapColumns(parentMap, targetVal.Interface(), targetTable, targetVal, prefix, joinOps...); err != nil {
		return nil, "", err
	}
	return &tableAlias{TableMap: targetTable, dialect: plan.dbMap.Dialect}, alias, nil
}

// fieldByIndex is a copy of v.FieldByIndex, except that it will
// initialize nil pointers while descending the indexes.
func fieldByIndex(v reflect.Value, index []int) reflect.Value {
	for _, idx := range index {
		if v.Kind() == reflect.Ptr {
			if v.IsNil() {
				v.Set(reflect.New(v.Type().Elem()))
			}
			v = v.Elem()
		}
		switch v.Kind() {
		case reflect.Struct:
			v = v.Field(idx)
		default:
			panic("gorp: found unsupported type using fieldByIndex: " + v.Kind().String())
		}
	}
	return v
}

// fieldOrNilByIndex is like fieldByIndex, except that it performs no
// initialization.  If it finds a nil pointer, it just returns the nil
// pointer, even if it is not the field requested.
func fieldOrNilByIndex(v reflect.Value, index []int) reflect.Value {
	var f reflect.StructField
	t := v.Type()
	for _, idx := range index {
		if v.Kind() == reflect.Ptr {
			if v.IsNil() {
				return v
			}
			v = v.Elem()
			t = t.Elem()
		}
		v = v.Field(idx)
		f = t.Field(idx)
		t = f.Type
	}
	return v
}

type referenceFilter struct {
	clause string
}

func (filter *referenceFilter) ActualValues() []interface{} {
	return nil
}

func (filter *referenceFilter) Where(...string) string {
	return filter.clause
}

func reference(leftTable, leftCol, rightTable, rightCol string) filters.Filter {
	return &referenceFilter{
		clause: fmt.Sprintf("%s.%s = %s.%s", leftTable, leftCol, rightTable, rightCol),
	}
}

// mapColumns creates a list of field addresses and column maps, to
// make looking up the column for a field address easier.  Note that
// it doesn't do any special handling for overridden fields, because
// passing the address of a field that has been overridden is
// difficult to do accidentally.
func (plan *QueryPlan) mapColumns(parentMap *fieldColumnMap, parent interface{}, table *gorp.TableMap, value reflect.Value, prefix string, joinOps ...JoinOp) (err error) {
	value = value.Elem()
	if plan.colMap == nil {
		plan.colMap = make(structColumnMap, 0, value.NumField())
	}
	queryableFields := 0
	quotedTableName := plan.dbMap.Dialect.QuoteField(strings.TrimSuffix(prefix, "_"))
	if prefix == "" || prefix == "-" {
		quotedTableName = plan.dbMap.Dialect.QuotedTableForQuery(table.SchemaName, table.TableName)
	}
	for _, col := range table.Columns {
		shouldSelect := !col.Transient && prefix != "-"
		if value.Type().FieldByIndex(col.FieldIndex()).PkgPath != "" {
			// TODO: What about anonymous fields?
			// Don't map unexported fields
			continue
		}
		field := fieldByIndex(value, col.FieldIndex())
		alias := prefix + col.ColumnName
		colPrefix := prefix
		if col.JoinAlias() != "" {
			alias = prefix + col.JoinAlias()
			colPrefix = prefix + col.JoinPrefix()
		}
		fieldRef := field.Addr().Interface()
		quotedCol := plan.dbMap.Dialect.QuoteField(col.ColumnName)
		if prefix != "-" && prefix != "" {
			// This means we're mapping an embedded struct, so we can
			// sort of autodetect some reference columns.
			if len(col.ReferencedBy()) > 0 {
				// The way that foreign keys work, columns that are
				// referenced by other columns will have the same
				// field reference.
				fieldMap, err := plan.colMap.fieldMapForPointer(fieldRef)
				if err == nil {
					plan.lastRefs = append(plan.lastRefs, reference(fieldMap.quotedTable, fieldMap.quotedColumn, quotedTableName, quotedCol))
					shouldSelect = false
				}
			}
		}
		fieldMap := &fieldColumnMap{
			parentMap:    parentMap,
			parent:       parent,
			field:        fieldRef,
			selectTarget: fieldRef,
			column:       col,
			alias:        alias,
			prefix:       colPrefix,
			quotedTable:  quotedTableName,
			quotedColumn: quotedCol,
			doSelect:     shouldSelect,
		}
		for _, op := range joinOps {
			if op.Table == table && op.Column == col {
				fieldMap.join = op.Join
			}
		}
		for _, op := range joinOps {
			if table == op.Table && col == op.Column {
				fieldMap.join = op.Join
				break
			}
		}
		plan.colMap = append(plan.colMap, fieldMap)
		if !col.Transient {
			queryableFields++
		}
	}
	if queryableFields == 0 {
		return errors.New("No fields in the target struct are mappable.")
	}
	return
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

// Fields restricts the columns being selected in a select query to
// just those matching the passed in field pointers.
func (plan *QueryPlan) Fields(fields ...interface{}) interfaces.SelectionQuery {
	for _, field := range plan.colMap {
		field.doSelect = false
	}
	for _, field := range fields {
		plan.AddField(field)
	}
	return plan
}

// AddField adds a field to the select statement.  Some fields (for
// example, fields that are processed via JoinOp values passed to
// Query()) are not mapped in the query by default, and you may use
// AddField to request that they are selected.
//
// Note that fields handled by JoinOp values should *not* be
// explicitly joined to using methods like Join or LeftJoin, but added
// using AddField instead.
func (plan *QueryPlan) AddField(fieldPtr interface{}) interfaces.SelectionQuery {
	m, err := plan.colMap.joinMapForPointer(fieldPtr)
	if err != nil {
		plan.Errors = append(plan.Errors, err)
		return plan
	}
	m.doSelect = true
	if m.join != nil {
		joinType, joinTarget, joinField, constraints := m.join(m.parent, m.field)
		if joinTarget == nil {
			// This means to not bother joining.
			m.doSelect = false
			return plan
		}
		plan.JoinType(joinType, joinTarget).On(constraints...)
		m.selectTarget = joinField
	}
	return plan
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
	table, alias, err := plan.mapTable(reflect.ValueOf(target))

	if err != nil {
		plan.Errors = append(plan.Errors, err)
		// Add a filter just so the rest of the query methods won't panic
		plan.filters = &filters.JoinFilter{Type: joinType, QuotedJoinTable: "Error: no table found"}
		return
	}
	quotedTable := plan.dbMap.Dialect.QuotedTableForQuery(table.SchemaName, table.TableName)
	quotedAlias := ""
	if alias != "" && alias != "-" {
		quotedAlias = plan.dbMap.Dialect.QuoteField(alias)
	}
	plan.filters = &filters.JoinFilter{Type: joinType, QuotedJoinTable: quotedTable, QuotedAlias: quotedAlias}
	return
}

func (plan *QueryPlan) Join(target interface{}) interfaces.JoinQuery {
	return plan.JoinType("inner", target)
}

func (plan *QueryPlan) LeftJoin(target interface{}) interfaces.JoinQuery {
	return plan.JoinType("left outer", target)
}

func (plan *QueryPlan) On(filters ...filters.Filter) interfaces.JoinQuery {
	plan.filters.Add(filters...)
	return &JoinQueryPlan{QueryPlan: plan}
}

func (plan *QueryPlan) References() interfaces.JoinQuery {
	joinQuery := &JoinQueryPlan{QueryPlan: plan}
	return joinQuery.References()
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

// ConvertTo adds a type converted version of a ComparisonFilter to the where clause.
func (plan *QueryPlan) ConvertTo(filter filters.Filter, to string) interfaces.WhereQuery {
	return plan.Filter(filters.ConvertTo(filter, to))
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

func (plan *QueryPlan) whereClause() (string, error) {
	if plan.filters == nil {
		return "", nil
	}
	whereArgs := plan.filters.ActualValues()
	whereVals := make([]string, 0, len(whereArgs))
	for _, arg := range whereArgs {
		val, err := plan.argOrColumn(arg)
		if err != nil {
			return "", err
		}
		whereVals = append(whereVals, val)
	}
	where := plan.filters.Where(whereVals...)
	if where != "" {
		return " where " + where, nil
	}
	return "", nil
}

func (plan *QueryPlan) selectJoinClause() (string, error) {
	buffer := bufPool.Get().(*bytes.Buffer)
	buffer.Reset()
	for _, join := range plan.joins {
		buffer.WriteString(" ")
		joinArgs := join.ActualValues()
		joinVals := make([]string, 0, len(joinArgs))
		for _, arg := range joinArgs {
			val, err := plan.argOrColumn(arg)
			if err != nil {
				bufPool.Put(buffer)
				return "", err
			}
			joinVals = append(joinVals, val)
		}
		joinClause := join.JoinClause(joinVals...)
		buffer.WriteString(joinClause)
	}
	s := buffer.String()
	bufPool.Put(buffer)
	return s, nil
}

// Truncate will run this query plan as a TRUNCATE TABLE statement.
func (plan *QueryPlan) Truncate() error {
	query := fmt.Sprintf("truncate table %s", plan.QuotedTable())
	_, err := plan.dbMap.Exec(query)
	return err
}

// Select will run this query plan as a SELECT statement.
func (plan *QueryPlan) Select() ([]interface{}, error) {
	query, err := plan.selectQuery()
	if err != nil {
		return nil, err
	}

	target := plan.target.Interface()
	if subQuery, ok := target.(subQuery); ok {
		target = subQuery.getTarget().Interface()
	}
	res, err := plan.executor.Select(target, query, plan.getArgs()...)
	if err != nil {
		return nil, err
	}

	return res, nil
}

// Distinct will make this query return only DISTINCT results
func (plan *QueryPlan) Distinct(fields ...interface{}) {
	plan.distinctFields = fields
}

// ForUpdate will make this query select using "for update" row locking.
func (plan *QueryPlan) ForUpdate(of interface{}) {
	plan.forUpdate = true
	if of != nil {
		table, _, err := plan.mapTable(reflect.ValueOf(of))
		if err != nil {
			plan.Errors = append(plan.Errors, err)
			return
		}
		plan.forUpdateOf = plan.dbMap.Dialect.QuotedTableForQuery(table.SchemaName, table.TableName)
	}
}

// SelectToTarget will run this query plan as a SELECT statement, and
// append results directly to the passed in slice pointer.
func (plan *QueryPlan) SelectToTarget(target interface{}) error {
	targetVal := reflect.ValueOf(target)
	targetType := targetVal.Type()
	if targetType.Kind() != reflect.Ptr || targetType.Elem().Kind() != reflect.Slice {
		return errors.New("SelectToTarget must be run with a pointer to a slice as its target")
	}
	query, err := plan.selectQuery()
	if err != nil {
		return err
	}

	_, err = plan.executor.Select(target, query, plan.getArgs()...)
	if err != nil {
		return err
	}

	return err
}

func (plan *QueryPlan) Count() (int64, error) {
	plan.resetArgs()
	buffer := bufPool.Get().(*bytes.Buffer)
	buffer.Reset()
	buffer.WriteString("select count(*)")
	if err := plan.writeSelectSuffix(buffer); err != nil {
		bufPool.Put(buffer)
		return -1, err
	}
	s := buffer.String()
	bufPool.Put(buffer)
	return plan.executor.SelectInt(s, plan.getArgs()...)
}

func (plan *QueryPlan) QuotedTable() string {
	if plan.quotedTable == "" {
		plan.quotedTable = plan.dbMap.Dialect.QuotedTableForQuery(plan.table.SchemaName, plan.table.TableName)
	}
	return plan.quotedTable
}

func (plan *QueryPlan) selectQuery() (string, error) {
	plan.resetArgs()
	buffer := bufPool.Get().(*bytes.Buffer)
	buffer.Reset()
	if err := plan.writeSelectColumns(buffer); err != nil {
		bufPool.Put(buffer)
		return "", err
	}
	if err := plan.writeSelectSuffix(buffer); err != nil {
		bufPool.Put(buffer)
		return "", err
	}
	if plan.forUpdate {
		buffer.WriteString(" for update")
		if plan.forUpdateOf != "" {
			buffer.WriteString(" of ")
			buffer.WriteString(plan.forUpdateOf)
		}
	}
	s := buffer.String()
	bufPool.Put(buffer)
	return s, nil
}

func (plan *QueryPlan) ArgOrColumn(value interface{}) (sqlValue string, err error) {
	return plan.argOrColumn(value)
}

// argOrColumn returns the string that should be used to represent a
// value in a query.  If the value is detected to be a field, an error
// will be returned if the field cannot be selected.  If the value is
// used as an argument, it will be appended to args and the returned
// string will be the bind value.
func (plan *QueryPlan) argOrColumn(value interface{}) (sqlValue string, err error) {
	switch src := value.(type) {
	case filters.SqlWrapper:
		value = src.ActualValue()
		wrapperVal, err := plan.argOrColumn(value)
		if err != nil {
			return "", err
		}
		return src.WrapSql(wrapperVal), nil
	case filters.MultiSqlWrapper:
		values := src.ActualValues()
		wrapperVals := make([]string, 0, len(values))
		for _, val := range values {
			wrapperVal, err := plan.argOrColumn(val)
			if err != nil {
				return "", err
			}
			wrapperVals = append(wrapperVals, wrapperVal)
		}
		return src.WrapSql(wrapperVals...), nil
	default:
		if reflect.TypeOf(value).Kind() == reflect.Ptr {
			m, err := plan.colMap.fieldMapForPointer(value)
			if err != nil {
				return "", err
			}
			if m.selectTarget != m.field {
				return plan.argOrColumn(m.selectTarget)
			}
			sqlValue = m.quotedTable + "." + m.quotedColumn
		} else {
			sqlValue = plan.dbMap.Dialect.BindVar(len(plan.getArgs()))
			plan.appendArgs(value)
		}
	}
	return
}

func (plan *QueryPlan) writeSelectColumns(buffer *bytes.Buffer) error {
	if len(plan.Errors) > 0 {
		return plan.Errors[0]
	}
	buffer.WriteString("select ")
	if len(plan.distinctFields) != 0 {
		buffer.WriteString("distinct on (")
		var name string
		var err error
		name, err = plan.argOrColumn(plan.distinctFields[0])
		if err != nil {
			return err
		}
		buffer.WriteString(name)

		if len(plan.distinctFields) == 1 {
			buffer.WriteString(") ")
		} else {
			for _, df := range plan.distinctFields {
				var name string
				var err error
				name, err = plan.argOrColumn(df)
				if err != nil {
					return err
				}
				buffer.WriteString(",")
				buffer.WriteString(name)
			}
			buffer.WriteString(") ")
		}
	}
	for index, m := range plan.colMap {
		if m.doSelect {
			if index != 0 {
				buffer.WriteString(",")
			}
			var err error
			selectClause := m.quotedTable + "." + m.quotedColumn
			if m.selectTarget != m.field {
				switch src := m.selectTarget.(type) {
				case filters.SqlWrapper:
					actualValue := src.ActualValue()
					sqlValue, err := plan.argOrColumn(actualValue)
					if err != nil {
						return err
					}
					selectClause = src.WrapSql(sqlValue)
				case filters.MultiSqlWrapper:
					values := src.ActualValues()
					sqlValues := make([]string, 0, len(values))
					for _, v := range values {
						sqlValue, err := plan.argOrColumn(v)
						if err != nil {
							return err
						}
						sqlValues = append(sqlValues, sqlValue)
					}
					selectClause = src.WrapSql(sqlValues...)
				default:
					selectClause, err = plan.argOrColumn(m.field)
					if err != nil {
						return err
					}
				}
			}
			buffer.WriteString(selectClause)
			if m.alias != "" {
				buffer.WriteString(" AS ")
				buffer.WriteString(m.alias)
			}
		}
	}
	return nil
}

func (plan *QueryPlan) writeSelectSuffix(buffer *bytes.Buffer) error {
	plan.storeJoin()
	buffer.WriteString(" from ")
	buffer.WriteString(plan.QuotedTable())
	joinClause, err := plan.selectJoinClause()
	if err != nil {
		return err
	}
	buffer.WriteString(joinClause)
	whereClause, err := plan.whereClause()
	if err != nil {
		return err
	}
	buffer.WriteString(whereClause)
	for index, orderBy := range plan.orderBy {
		if index == 0 {
			buffer.WriteString(" order by ")
		} else {
			buffer.WriteString(", ")
		}
		orderStr, args, err := orderBy.OrderBy(plan.dbMap.Dialect, plan.colMap, plan.argLen)
		if err != nil {
			return err
		}
		buffer.WriteString(orderStr)
		plan.appendArgs(args...)
	}
	for index, groupBy := range plan.groupBy {
		if index == 0 {
			buffer.WriteString(" group by ")
		} else {
			buffer.WriteString(", ")
		}
		buffer.WriteString(groupBy)
	}
	// Nonstandard LIMIT clauses seem to have to come *before* the
	// offset clause.
	limiter, nonstandard := plan.dbMap.Dialect.(interfaces.NonstandardLimiter)
	if plan.limit > 0 && nonstandard {
		buffer.WriteString(" ")
		buffer.WriteString(limiter.Limit(plan.dbMap.Dialect.BindVar(plan.argLen)))
		plan.appendArgs(plan.limit)
	}
	if plan.offset > 0 {
		buffer.WriteString(" offset ")
		buffer.WriteString(plan.dbMap.Dialect.BindVar(plan.argLen))
		plan.appendArgs(plan.offset)
	}
	// Standard FETCH NEXT (n) ROWS ONLY must come after the offset.
	if plan.limit > 0 && !nonstandard {
		// Many dialects seem to ignore the SQL standard when it comes
		// to the limit clause.
		buffer.WriteString(" fetch next (")
		buffer.WriteString(plan.dbMap.Dialect.BindVar(plan.argLen))
		plan.appendArgs(plan.limit)
		buffer.WriteString(") rows only")
	}
	return nil
}

// Insert will run this query plan as an INSERT statement.
func (plan *QueryPlan) Insert() error {
	plan.resetArgs()
	if len(plan.Errors) > 0 {
		return plan.Errors[0]
	}
	buffer := bufPool.Get().(*bytes.Buffer)
	buffer.Reset()
	buffer.WriteString("insert into ")
	buffer.WriteString(plan.dbMap.Dialect.QuotedTableForQuery(plan.table.SchemaName, plan.table.TableName))
	buffer.WriteString(" (")
	for i, col := range plan.assignCols {
		if i > 0 {
			buffer.WriteString(", ")
		}
		buffer.WriteString(col)
	}
	buffer.WriteString(") values (")
	for i, bindVar := range plan.assignBindVars {
		if i > 0 {
			buffer.WriteString(", ")
		}
		buffer.WriteString(bindVar)
	}
	buffer.WriteString(")")
	s := buffer.String()
	bufPool.Put(buffer)
	_, err := plan.executor.Exec(s, plan.getArgs()...)

	return err
}

// joinFromAndWhereClause will return the from and where clauses for
// joined tables, for use in UPDATE and DELETE statements.
func (plan *QueryPlan) joinFromAndWhereClause() (from, where string, err error) {
	fromSlice := make([]string, 0, len(plan.joins))
	whereBuffer := bufPool.Get().(*bytes.Buffer)
	whereBuffer.Reset()
	for _, join := range plan.joins {
		fromSlice = append(fromSlice, join.QuotedJoinTable)
		whereArgs := join.ActualValues()
		whereVals := make([]string, 0, len(whereArgs))
		for _, arg := range whereArgs {
			val, err := plan.argOrColumn(arg)
			if err != nil {
				bufPool.Put(whereBuffer)
				return "", "", err
			}
			whereVals = append(whereVals, val)
		}
		whereClause := join.Where(whereVals...)
		if err != nil {
			bufPool.Put(whereBuffer)
			return "", "", err
		}
		whereBuffer.WriteString(whereClause)
	}
	s := whereBuffer.String()
	bufPool.Put(whereBuffer)
	return strings.Join(fromSlice, ", "), s, nil
}

// Update will run this query plan as an UPDATE statement.
func (plan *QueryPlan) Update() (int64, error) {
	plan.resetArgs()
	if len(plan.Errors) > 0 {
		return -1, plan.Errors[0]
	}
	buffer := bufPool.Get().(*bytes.Buffer)
	buffer.Reset()
	defer bufPool.Put(buffer)
	buffer.WriteString("update ")
	buffer.WriteString(plan.dbMap.Dialect.QuotedTableForQuery(plan.table.SchemaName, plan.table.TableName))
	buffer.WriteString(" set ")
	for i, col := range plan.assignCols {
		bindVar := plan.assignBindVars[i]
		if i > 0 {
			buffer.WriteString(", ")
		}
		buffer.WriteString(col)
		buffer.WriteString("=")
		buffer.WriteString(bindVar)
	}
	joinTables, joinWhereClause, err := plan.joinFromAndWhereClause()
	if err != nil {
		return -1, nil
	}
	if joinTables != "" {
		buffer.WriteString(" from ")
		buffer.WriteString(joinTables)
	}
	whereClause, err := plan.whereClause()
	if err != nil {
		return -1, err
	}
	if joinWhereClause != "" {
		if whereClause == "" {
			whereClause = " where "
		} else {
			whereClause += " and "
		}
		whereClause += joinWhereClause
	}
	buffer.WriteString(whereClause)
	res, err := plan.executor.Exec(buffer.String(), plan.getArgs()...)
	if err != nil {
		return -1, err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return -1, err
	}

	if v, ok := plan.target.Interface().(HasPostDirectUpdate); ok {
		err := v.PostDirectUpdate(plan.executor)
		if err != nil {
			return -1, err
		}
	}

	return rows, nil
}

// Delete will run this query plan as a DELETE statement.
func (plan *QueryPlan) Delete() (int64, error) {
	plan.resetArgs()
	if len(plan.Errors) > 0 {
		return -1, plan.Errors[0]
	}
	buffer := bufPool.Get().(*bytes.Buffer)
	buffer.Reset()
	defer bufPool.Put(buffer)
	buffer.WriteString("delete from ")
	buffer.WriteString(plan.dbMap.Dialect.QuotedTableForQuery(plan.table.SchemaName, plan.table.TableName))
	joinTables, joinWhereClause, err := plan.joinFromAndWhereClause()
	if err != nil {
		return -1, err
	}
	if joinTables != "" {
		buffer.WriteString(" using ")
		buffer.WriteString(joinTables)
	}
	whereClause, err := plan.whereClause()
	if err != nil {
		return -1, err
	}
	if joinWhereClause != "" {
		if whereClause == "" {
			whereClause = " where "
		} else {
			whereClause += " and "
		}
		whereClause += joinWhereClause
	}
	buffer.WriteString(whereClause)
	res, err := plan.executor.Exec(buffer.String(), plan.getArgs()...)
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

func (plan *JoinQueryPlan) References() interfaces.JoinQuery {
	if len(plan.lastRefs) == 0 {
		plan.Errors = append(plan.Errors, errors.New("No references found to join with"))
	}
	plan.QueryPlan.Filter(plan.lastRefs...)
	return plan
}

func (plan *JoinQueryPlan) In(fieldPtr interface{}, values ...interface{}) interfaces.JoinQuery {
	plan.QueryPlan.In(fieldPtr, values...)
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
	plan.assignBindVars = append(plan.assignBindVars, plan.dbMap.Dialect.BindVar(len(plan.assignArgs)))
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
