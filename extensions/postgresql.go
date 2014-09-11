package extensions

import (
	"bytes"

	"github.com/nelsam/gorp"
	"github.com/nelsam/gorp_queries/filters"
	"github.com/nelsam/gorp_queries/interfaces"
	"github.com/nelsam/gorp_queries/query_plans"
)

type pgJsonSelectWrapper struct {
	actualValue interface{}
	subElements []string
}

func (wrapper pgJsonSelectWrapper) ActualValue() interface{} {
	return wrapper.actualValue
}

func (wrapper pgJsonSelectWrapper) WrapSql(sqlValue string) string {
	buf := bytes.Buffer{}
	buf.WriteString(sqlValue)
	for idx, elem := range wrapper.subElements {
		isLast := idx == len(wrapper.subElements)-1
		buf.WriteString("::json->")
		if isLast {
			buf.WriteString(">")
		}
		buf.WriteString("'")
		buf.WriteString(elem)
		buf.WriteString("'")
	}
	return buf.String()
}

func PgJsonField(actualValue interface{}, subElements ...string) filters.SqlWrapper {
	return pgJsonSelectWrapper{
		actualValue: actualValue,
		subElements: subElements,
	}
}

type PostgresAssigner interface {
	Assign(fieldPtr interface{}, value interface{}) PostgresAssignQuery
}

type PostgresJoiner interface {
	Join(table interface{}) PostgresJoinQuery
}

type PostgresAssignJoiner interface {
	Join(table interface{}) PostgresAssignJoinQuery
}

// PostgresJoinQuery is an interfaces.JoinQuery, but with support for
// delete statements.  See interfaces.JoinQuery for documentation.
type PostgresJoinQuery interface {
	PostgresJoiner

	On(...filters.Filter) PostgresJoinQuery

	Equal(fieldPtr interface{}, value interface{}) PostgresJoinQuery
	NotEqual(fieldPtr interface{}, value interface{}) PostgresJoinQuery
	Less(fieldPtr interface{}, value interface{}) PostgresJoinQuery
	LessOrEqual(fieldPtr interface{}, value interface{}) PostgresJoinQuery
	Greater(fieldPtr interface{}, value interface{}) PostgresJoinQuery
	GreaterOrEqual(fieldPtr interface{}, value interface{}) PostgresJoinQuery
	NotNull(fieldPtr interface{}) PostgresJoinQuery
	Null(fieldPtr interface{}) PostgresJoinQuery

	// In postgres, a query joined to other tables can still be a
	// delete statement.
	interfaces.Wherer
	interfaces.Deleter
	interfaces.SelectManipulator
	interfaces.Selector
}

// PostgresAssignQuery is an interfaces.AssignQuery, but with support
// for joining to other tables in an update statement.
type PostgresAssignQuery interface {
	PostgresAssigner
	PostgresAssignJoiner
	interfaces.AssignWherer
	interfaces.Inserter
	interfaces.Updater
}

// PostgresAssignJoinQuery is an interfaces.AssignQuery that has been
// joined to other tables.
type PostgresAssignJoinQuery interface {
	PostgresAssignJoiner

	On(...filters.Filter) PostgresAssignJoinQuery

	Equal(fieldPtr interface{}, value interface{}) PostgresAssignJoinQuery
	NotEqual(fieldPtr interface{}, value interface{}) PostgresAssignJoinQuery
	Less(fieldPtr interface{}, value interface{}) PostgresAssignJoinQuery
	LessOrEqual(fieldPtr interface{}, value interface{}) PostgresAssignJoinQuery
	Greater(fieldPtr interface{}, value interface{}) PostgresAssignJoinQuery
	GreaterOrEqual(fieldPtr interface{}, value interface{}) PostgresAssignJoinQuery
	NotNull(fieldPtr interface{}) PostgresAssignJoinQuery
	Null(fieldPtr interface{}) PostgresAssignJoinQuery

	// Postgres doesn't support joining tables in an insert statement.
	interfaces.AssignWherer
	interfaces.Updater
}

// Postgres is a Query type that implements PostgreSQL extensions to
// standard SQL.  Most notably, you can "join" tables during DELETE
// and UPDATE queries, using "DELETE FROM ... USING ..." and "UPDATE
// ... FROM ..."
type Postgres interface {
	// A PostgresqlExtendedQuery can perform join operations during
	// DELETE and UPDATE queries.
	PostgresAssigner
	PostgresJoiner
	interfaces.Wherer
	interfaces.SelectManipulator
	interfaces.Deleter
	interfaces.Selector
}

// PostgresExtendedQueryPlan is a QueryPlan that supports some of
// postgresql's extensions to the SQL standard.
type PostgresExtendedQueryPlan struct {
	*query_plans.QueryPlan
}

func PostgresPlan(query *query_plans.QueryPlan) interface{} {
	return &PostgresExtendedQueryPlan{QueryPlan: query}
}

func (plan *PostgresExtendedQueryPlan) Assign(fieldPtr interface{}, value interface{}) PostgresAssignQuery {
	assignPlan := plan.QueryPlan.Assign(fieldPtr, value)
	return &PostgresExtendedAssignQueryPlan{AssignQueryPlan: assignPlan.(*query_plans.AssignQueryPlan)}
}

func (plan *PostgresExtendedQueryPlan) Join(table interface{}) PostgresJoinQuery {
	plan.QueryPlan.Join(table)
	return &PostgresExtendedJoinQueryPlan{plan}
}

func (plan *PostgresExtendedQueryPlan) On(filters ...filters.Filter) PostgresJoinQuery {
	plan.QueryPlan.On(filters...)
	return &PostgresExtendedJoinQueryPlan{plan}
}

type PostgresExtendedJoinQueryPlan struct {
	*PostgresExtendedQueryPlan
}

func (plan *PostgresExtendedJoinQueryPlan) Equal(fieldPtr interface{}, value interface{}) PostgresJoinQuery {
	plan.QueryPlan.Equal(fieldPtr, value)
	return plan
}

func (plan *PostgresExtendedJoinQueryPlan) NotEqual(fieldPtr interface{}, value interface{}) PostgresJoinQuery {
	plan.QueryPlan.NotEqual(fieldPtr, value)
	return plan
}

func (plan *PostgresExtendedJoinQueryPlan) Less(fieldPtr interface{}, value interface{}) PostgresJoinQuery {
	plan.QueryPlan.Less(fieldPtr, value)
	return plan
}

func (plan *PostgresExtendedJoinQueryPlan) LessOrEqual(fieldPtr interface{}, value interface{}) PostgresJoinQuery {
	plan.QueryPlan.LessOrEqual(fieldPtr, value)
	return plan
}

func (plan *PostgresExtendedJoinQueryPlan) Greater(fieldPtr interface{}, value interface{}) PostgresJoinQuery {
	plan.QueryPlan.Greater(fieldPtr, value)
	return plan
}

func (plan *PostgresExtendedJoinQueryPlan) GreaterOrEqual(fieldPtr interface{}, value interface{}) PostgresJoinQuery {
	plan.QueryPlan.GreaterOrEqual(fieldPtr, value)
	return plan
}

func (plan *PostgresExtendedJoinQueryPlan) Null(fieldPtr interface{}) PostgresJoinQuery {
	plan.QueryPlan.Null(fieldPtr)
	return plan
}

func (plan *PostgresExtendedJoinQueryPlan) NotNull(fieldPtr interface{}) PostgresJoinQuery {
	plan.QueryPlan.NotNull(fieldPtr)
	return plan
}

type PostgresExtendedAssignQueryPlan struct {
	*query_plans.AssignQueryPlan
}

func (plan *PostgresExtendedAssignQueryPlan) Assign(fieldPtr interface{}, value interface{}) PostgresAssignQuery {
	plan.QueryPlan.Assign(fieldPtr, value)
	return plan
}

func (plan *PostgresExtendedAssignQueryPlan) Join(table interface{}) PostgresAssignJoinQuery {
	plan.QueryPlan.Join(table)
	return &PostgresExtendedAssignJoinQueryPlan{plan}
}

type PostgresExtendedAssignJoinQueryPlan struct {
	*PostgresExtendedAssignQueryPlan
}

func (plan *PostgresExtendedAssignJoinQueryPlan) On(filters ...filters.Filter) PostgresAssignJoinQuery {
	plan.QueryPlan.On(filters...)
	return plan
}

func (plan *PostgresExtendedAssignJoinQueryPlan) Equal(fieldPtr interface{}, value interface{}) PostgresAssignJoinQuery {
	plan.QueryPlan.Equal(fieldPtr, value)
	return plan
}

func (plan *PostgresExtendedAssignJoinQueryPlan) NotEqual(fieldPtr interface{}, value interface{}) PostgresAssignJoinQuery {
	plan.QueryPlan.NotEqual(fieldPtr, value)
	return plan
}

func (plan *PostgresExtendedAssignJoinQueryPlan) Less(fieldPtr interface{}, value interface{}) PostgresAssignJoinQuery {
	plan.QueryPlan.Less(fieldPtr, value)
	return plan
}

func (plan *PostgresExtendedAssignJoinQueryPlan) LessOrEqual(fieldPtr interface{}, value interface{}) PostgresAssignJoinQuery {
	plan.QueryPlan.LessOrEqual(fieldPtr, value)
	return plan
}

func (plan *PostgresExtendedAssignJoinQueryPlan) Greater(fieldPtr interface{}, value interface{}) PostgresAssignJoinQuery {
	plan.QueryPlan.Greater(fieldPtr, value)
	return plan
}

func (plan *PostgresExtendedAssignJoinQueryPlan) GreaterOrEqual(fieldPtr interface{}, value interface{}) PostgresAssignJoinQuery {
	plan.QueryPlan.GreaterOrEqual(fieldPtr, value)
	return plan
}

func (plan *PostgresExtendedAssignJoinQueryPlan) Null(fieldPtr interface{}) PostgresAssignJoinQuery {
	plan.QueryPlan.Null(fieldPtr)
	return plan
}

func (plan *PostgresExtendedAssignJoinQueryPlan) NotNull(fieldPtr interface{}) PostgresAssignJoinQuery {
	plan.QueryPlan.NotNull(fieldPtr)
	return plan
}

func init() {
	query_plans.RegisterExtension(gorp.PostgresDialect{}, PostgresPlan)
}
