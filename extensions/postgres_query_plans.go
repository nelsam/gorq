package extensions

import (
	"github.com/outdoorsy/gorp"
	"github.com/outdoorsy/gorq/filters"
	"github.com/outdoorsy/gorq/interfaces"
	"github.com/outdoorsy/gorq/plans"
)

// PostgresAssigner is equivalent to interfaces.Assigner, but allows
// postgres-specific join-like operations during update queries.
type PostgresAssigner interface {
	Assign(fieldPtr interface{}, value interface{}) PostgresAssignQuery
}

// PostgresJoiner includes methods equivalent to interfaces.Joiner,
// except for the functions' return types, as well as some slightly
// different join types.
type PostgresJoiner interface {
	Join(table interface{}) PostgresJoinQuery
	LeftJoin(table interface{}) PostgresJoinQuery
}

// PostgresAssignJoiner is equivalent to interfaces.Joiner, except for
// the functions' return types.
type PostgresAssignJoiner interface {
	Join(table interface{}) PostgresAssignJoinQuery
}

// PostgresJoinQuery is an interfaces.JoinQuery, but with support for
// delete statements.  See interfaces.JoinQuery for main
// documentation.  It also allows some slightly different joins,
// documented in PostgresJoiner.
type PostgresJoinQuery interface {
	PostgresJoiner

	On(...filters.Filter) PostgresJoinQuery

	References() PostgresJoinQuery

	In(fieldPtr interface{}, values ...interface{}) PostgresJoinQuery
	Like(fieldPtr interface{}, pattern string) PostgresJoinQuery
	Equal(fieldPtr interface{}, value interface{}) PostgresJoinQuery
	NotEqual(fieldPtr interface{}, value interface{}) PostgresJoinQuery
	Less(fieldPtr interface{}, value interface{}) PostgresJoinQuery
	LessOrEqual(fieldPtr interface{}, value interface{}) PostgresJoinQuery
	Greater(fieldPtr interface{}, value interface{}) PostgresJoinQuery
	GreaterOrEqual(fieldPtr interface{}, value interface{}) PostgresJoinQuery
	NotNull(fieldPtr interface{}) PostgresJoinQuery
	Null(fieldPtr interface{}) PostgresJoinQuery
	True(fieldPtr interface{}) PostgresJoinQuery
	False(fieldPtr interface{}) PostgresJoinQuery

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

	References() PostgresAssignJoinQuery

	In(fieldPtr interface{}, values ...interface{}) PostgresAssignJoinQuery
	Like(fieldPtr interface{}, pattern string) PostgresAssignJoinQuery
	Equal(fieldPtr interface{}, value interface{}) PostgresAssignJoinQuery
	NotEqual(fieldPtr interface{}, value interface{}) PostgresAssignJoinQuery
	Less(fieldPtr interface{}, value interface{}) PostgresAssignJoinQuery
	LessOrEqual(fieldPtr interface{}, value interface{}) PostgresAssignJoinQuery
	Greater(fieldPtr interface{}, value interface{}) PostgresAssignJoinQuery
	GreaterOrEqual(fieldPtr interface{}, value interface{}) PostgresAssignJoinQuery
	NotNull(fieldPtr interface{}) PostgresAssignJoinQuery
	Null(fieldPtr interface{}) PostgresAssignJoinQuery
	True(fieldPtr interface{}) PostgresAssignJoinQuery
	False(fieldPtr interface{}) PostgresAssignJoinQuery

	// Postgres doesn't support joining tables in an insert statement.
	interfaces.AssignWherer
	interfaces.Updater
}

// Postgres is a Query type that implements PostgreSQL extensions to
// standard SQL.  Most notably, you can "join" tables during DELETE
// and UPDATE queries, using "DELETE FROM ... USING ..." and "UPDATE
// ... FROM ..."
type Postgres interface {
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
	*plans.QueryPlan
}

func PostgresPlan(query *plans.QueryPlan) interface{} {
	return &PostgresExtendedQueryPlan{QueryPlan: query}
}

func (plan *PostgresExtendedQueryPlan) Assign(fieldPtr interface{}, value interface{}) PostgresAssignQuery {
	assignPlan := plan.QueryPlan.Assign(fieldPtr, value)
	return &PostgresExtendedAssignQueryPlan{AssignQueryPlan: assignPlan.(*plans.AssignQueryPlan)}
}

func (plan *PostgresExtendedQueryPlan) Join(table interface{}) PostgresJoinQuery {
	plan.QueryPlan.Join(table)
	return &PostgresExtendedJoinQueryPlan{plan}
}

func (plan *PostgresExtendedQueryPlan) LeftJoin(table interface{}) PostgresJoinQuery {
	plan.QueryPlan.LeftJoin(table)
	return &PostgresExtendedJoinQueryPlan{plan}
}

type PostgresExtendedJoinQueryPlan struct {
	*PostgresExtendedQueryPlan
}

func (plan *PostgresExtendedJoinQueryPlan) On(filters ...filters.Filter) PostgresJoinQuery {
	plan.QueryPlan.On(filters...)
	return plan
}

func (plan *PostgresExtendedJoinQueryPlan) References() PostgresJoinQuery {
	plan.QueryPlan.References()
	return plan
}

func (plan *PostgresExtendedJoinQueryPlan) In(fieldPtr interface{}, values ...interface{}) PostgresJoinQuery {
	plan.QueryPlan.In(fieldPtr, values...)
	return plan
}

func (plan *PostgresExtendedJoinQueryPlan) Like(fieldPtr interface{}, pattern string) PostgresJoinQuery {
	plan.QueryPlan.Like(fieldPtr, pattern)
	return plan
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

func (plan *PostgresExtendedJoinQueryPlan) True(fieldPtr interface{}) PostgresJoinQuery {
	plan.QueryPlan.True(fieldPtr)
	return plan
}

func (plan *PostgresExtendedJoinQueryPlan) False(fieldPtr interface{}) PostgresJoinQuery {
	plan.QueryPlan.False(fieldPtr)
	return plan
}

type PostgresExtendedAssignQueryPlan struct {
	*plans.AssignQueryPlan
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

func (plan *PostgresExtendedAssignJoinQueryPlan) References() PostgresAssignJoinQuery {
	plan.QueryPlan.References()
	return plan
}

func (plan *PostgresExtendedAssignJoinQueryPlan) In(fieldPtr interface{}, values ...interface{}) PostgresAssignJoinQuery {
	plan.QueryPlan.In(fieldPtr, values...)
	return plan
}

func (plan *PostgresExtendedAssignJoinQueryPlan) Like(fieldPtr interface{}, pattern string) PostgresAssignJoinQuery {
	plan.QueryPlan.Like(fieldPtr, pattern)
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

func (plan *PostgresExtendedAssignJoinQueryPlan) True(fieldPtr interface{}) PostgresAssignJoinQuery {
	plan.QueryPlan.True(fieldPtr)
	return plan
}

func (plan *PostgresExtendedAssignJoinQueryPlan) False(fieldPtr interface{}) PostgresAssignJoinQuery {
	plan.QueryPlan.False(fieldPtr)
	return plan
}

func init() {
	plans.RegisterExtension(gorp.PostgresDialect{}, PostgresPlan)
}
