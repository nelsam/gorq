package interfaces

import (
	"github.com/nelsam/gorp_queries/filters"
)

// A NonstandardLimiter is a type of query dialect that doesn't
// support the SQL standard method of limiting query results (i.e.
// fetch (N) rows only).  It instead returns its own LIMIT clause.
type NonstandardLimiter interface {
	Limit(interface{}) string
}

// An Updater is a query that can execute UPDATE statements.
type Updater interface {
	// Update executes an update statement and returns the updated row
	// count and any errors encountered.
	Update() (rowsUpdated int64, err error)
}

// A Deleter is a query that can execute DELETE statements.
type Deleter interface {
	// Delete executes a delete statement and returns the deleted row
	// count and any errors encountered.
	Delete() (rowsDeleted int64, err error)
}

// An Inserter is a query that can execute INSERT statements.
type Inserter interface {
	// Insert executes an insert statement and returns any errors
	// encountered.
	Insert() error
}

// A Selector is a query that can execute SELECT statements.
type Selector interface {
	// Select executes the select statement and returns the resulting
	// rows and any errors encountered.  The resulting rows will be of
	// the same type as the type used as a reference for generating
	// the query.
	Select() (results []interface{}, err error)

	// SelectToTarget executes the select statement and returns any
	// errors encountered.  The resulting rows will be appended to the
	// passed in target, which must be a pointer to a slice.
	SelectToTarget(target interface{}) error

	// Count executes a select statement that just returns a count of
	// the number of rows that would be returned.
	Count() (int64, error)
}

// A SelectManipulator is a query that will return a list of results
// which can be manipulated.  Offset and Limit are rarely used without
// OrderBy, as the results can be unpredictable.  In Go terms, think
// of Offset and Limit as options to make the query return
// results[Offset:Offset+Limit].
type SelectManipulator interface {
	// OrderBy orders the resulting result list by a field of the
	// reference struct and a direction, which can be "asc" or "desc".
	OrderBy(fieldPtr interface{}, direction string) SelectQuery

	// GroupBy groups the result list by a field of the reference
	// struct.
	GroupBy(fieldPtr interface{}) SelectQuery

	// Limit limits the result list to a maximum length.
	Limit(int64) SelectQuery

	// Offset sets the starting point of the result list.
	Offset(int64) SelectQuery
}

// An Assigner is a query that can set columns to values.
type Assigner interface {
	// Assign assigns a value to a field of the reference struct.
	// Note that the first argument is always the target, and the
	// second argument is always the value that is going to be
	// assigned.
	Assign(fieldPtr interface{}, value interface{}) AssignQuery
}

// A Joiner is a query that can add tables as join clauses.
type Joiner interface {
	// Join adds a table to the query.  The return type (JoinQuery)
	// has methods for filtering how the new table relates to other
	// tables in the query.
	Join(table interface{}) JoinQuery
}

// A Wherer is a query that can execute statements with a WHERE
// clause.
type Wherer interface {
	// Where is used to assert that you're ready to start adding
	// filters to the where clause of the query.  You can pass a list
	// of filters (which will be combined in an AndFilter), or just
	// call it with no arguments for better readability than
	// .(WhereQuery).
	Where(...filters.Filter) WhereQuery
}

// An AssignWherer is a Wherer with an assigner return type.
type AssignWherer interface {
	// Where is the same as Wherer.Where(), save for the return type.
	Where(...filters.Filter) UpdateQuery
}

// A SelectQuery is a query that can only execute SELECT statements.
type SelectQuery interface {
	SelectManipulator
	Selector
}

// An UpdateQuery is a query that can only execute UPDATE statements.
type UpdateQuery interface {
	// Filter is used for queries that are more complex than a few
	// ANDed constraints.
	Filter(...filters.Filter) UpdateQuery

	// Equal, NotEqual, Less, LessOrEqual, Greater, GreaterOrEqual,
	// and NotNull are sugar to add filters to the where clause of the
	// query, which are combined in an AndFilter.  For example,
	// Equal(fieldPtr, value) is just sugar for
	// Filter(filters.Equal(fieldPtr, value)).
	In(fieldPtr interface{}, values ...interface{}) UpdateQuery
	Like(fieldPtr interface{}, pattern string) UpdateQuery
	Equal(fieldPtr interface{}, value interface{}) UpdateQuery
	NotEqual(fieldPtr interface{}, value interface{}) UpdateQuery
	Less(fieldPtr interface{}, value interface{}) UpdateQuery
	LessOrEqual(fieldPtr interface{}, value interface{}) UpdateQuery
	Greater(fieldPtr interface{}, value interface{}) UpdateQuery
	GreaterOrEqual(fieldPtr interface{}, value interface{}) UpdateQuery
	NotNull(fieldPtr interface{}) UpdateQuery
	Null(fieldPtr interface{}) UpdateQuery
	True(fieldPtr interface{}) UpdateQuery
	False(fieldPtr interface{}) UpdateQuery

	// An UpdateQuery has both assignments and a where clause, which
	// means that it must be an update statement.
	Updater
}

// An AssignQuery is a query that has assigned values.  It must be an
// insert or update statement.
type AssignQuery interface {
	Assigner
	AssignWherer
	Inserter
	Updater
}

// A JoinQuery is a query that uses join operations to compare values
// between tables.
type JoinQuery interface {
	Joiner

	// On for a JoinQuery is equivalent to WhereQuery.Filter, except
	// it is used in the join clause.
	On(...filters.Filter) JoinQuery

	// These methods are sugar for filtering a join, the same as the
	// methods on WhereQuery.  Equal(fieldPtr, value) is sugar for
	// On(filters.Equal(fieldPtr, value)).
	In(fieldPtr interface{}, values ...interface{}) JoinQuery
	Like(fieldPtr interface{}, pattern string) JoinQuery
	Equal(fieldPtr interface{}, value interface{}) JoinQuery
	NotEqual(fieldPtr interface{}, value interface{}) JoinQuery
	Less(fieldPtr interface{}, value interface{}) JoinQuery
	LessOrEqual(fieldPtr interface{}, value interface{}) JoinQuery
	Greater(fieldPtr interface{}, value interface{}) JoinQuery
	GreaterOrEqual(fieldPtr interface{}, value interface{}) JoinQuery
	NotNull(fieldPtr interface{}) JoinQuery
	Null(fieldPtr interface{}) JoinQuery
	True(fieldPtr interface{}) JoinQuery
	False(fieldPtr interface{}) JoinQuery

	Wherer

	// According to the SQL standard, the only statements that can
	// have any kind of multi-table clauses are select statements.
	// Most languages have extensions to support what are effectively
	// join operations on other statements, but since that is not part
	// of the standard, it is not supported here.
	SelectManipulator
	Selector
}

// A WhereQuery is a query that does not set any values, but may have
// a where clause.
type WhereQuery interface {
	// Filter is used to add filters to the where clause.  By default,
	// they are combined using an AndFilter.
	Filter(...filters.Filter) WhereQuery

	// Equal, NotEqual, Less, LessOrEqual, Greater, GreaterOrEqual,
	// and NotNull are sugar to add filters to the where clause of the
	// query, which are combined in an AndFilter.  For example,
	// Equal(fieldPtr, value) is just sugar for
	// Filter(filters.Equal(fieldPtr, value)).
	In(fieldPtr interface{}, values ...interface{}) WhereQuery
	Like(fieldPtr interface{}, pattern string) WhereQuery
	Equal(fieldPtr interface{}, value interface{}) WhereQuery
	NotEqual(fieldPtr interface{}, value interface{}) WhereQuery
	Less(fieldPtr interface{}, value interface{}) WhereQuery
	LessOrEqual(fieldPtr interface{}, value interface{}) WhereQuery
	Greater(fieldPtr interface{}, value interface{}) WhereQuery
	GreaterOrEqual(fieldPtr interface{}, value interface{}) WhereQuery
	NotNull(fieldPtr interface{}) WhereQuery
	Null(fieldPtr interface{}) WhereQuery
	True(fieldPtr interface{}) WhereQuery
	False(fieldPtr interface{}) WhereQuery

	// A WhereQuery is returned when Where() has been called before
	// Assign(), which means it must be a select or delete statement.
	SelectManipulator
	Deleter
	Selector
}

// A Query is the base query type - as methods are called, the type of
// query will gradually be restricted based on which types of queries
// are capable of performing the requested operations.
//
// For example, select and delete statements cannot assign values to
// columns, and insert statements cannot have a where clause.  Select
// statements are the only statements that are allowed to join with
// other tables.
//
// You can override this functionality using simple type assertions,
// but it is there to protect you from accidentally writing invalid
// queries.
//
// Also note that this design is mainly for restricting queries
// generated in one small block of code.  If you need to generate
// individual parts of the query in distinct sections of your code,
// you can safely ignore the return types until you call the final
// execution method.  Here are a few example queries:
//
//     // These are very creative model names.  I know.
//     ref := new(Model)
//     parentRef := new(Parent)
//
//     // Get the Model row related to the Parent specified by
//     // parentId.
//     results, err := dbMap.Query(ref).
//         Join(parentRef).
//         On(). // For readability.
//         Equal(&parentRef.Model, &ref.Id).
//         Where().
//         Equal(&parentRef.Id, parentId).
//         Select()
//
//     // Generate range filters to pass to Where()
//     range := []filters.Filter{
//         references.Less(&parentRef.ActiveDate, end),
//         references.Greater(&parentRef.ActiveDate, start),
//     }
//     // Get a list of Models within a certain date range.
//     results, err := dbMap.Query(ref).
//         Join(parentRef). // On() is unnecessary
//         Equal(&parentRef.Model, &ref.Id).
//         Where(range...).
//         OrderBy(&parentRef.ActiveDate, "ASC").
//         Select()
//     // Above, note that the return type of OrderBy no longer
//     // has methods for manipulating the where clause, so without a
//     // type assertion, you won't be able to manipulate the where
//     // clause beyond that point.  This is intended for readability
//     // restrictions in cascading method calls, to make the
//     // statement structure familiar to those who know standard SQL.
//
//     // Here, we use a query variable because our query will be
//     // generated using some if statements.  We also choose to
//     // directly pass a filter to On() for the join clause.
//     q := dbMap.Query(ref).
//         Join(parentRef).
//         On(filters.Equal(&parentRef.Model, &ref.Id)).
//         Where()
//     if parentType == PAST {
//         // Return values are only for cascading method calls - if
//         // we do it this way, we are free to continue manipulating
//         // the where clause after the call to OrderBy.
//         q.Less(&parentRef.ActiveDate, time.Now())
//         q.OrderBy(&parentRef.ActiveDate, "ASC")
//     }
//     if parentType != "" {
//         // Again, the type of q hasn't changed, so we can still add
//         // to the where clause.
//         q.Equal(&parentRef.Type, parentType)
//     }
//     results, err := q.Select()
//
// Note that many SQL languages have extensions to the SQL standard,
// and we provide some support for them in the extended package.
type Query interface {
	// Before calling any methods on a query, you can use Extend() to
	// use a registered extension query type.
	Extend() interface{}

	// A query that has had no methods called can both perform
	// assignments and still have a where clause.
	Assigner
	Joiner
	Wherer

	// Updates and inserts need at least one assignment, so they won't
	// be allowed until Assign has been called.  However, select and
	// delete statements can be called without any where clause, so
	// they are allowed here.
	//
	// We should probably have a configuration variable to determine
	// whether delete statements without a where clause are allowed,
	// to prevent people from just deleting everything in their table.
	// On the other hand, they should be checking the count they get
	// back to ensure they deleted exactly what they wanted to delete.
	SelectManipulator
	Deleter
	Selector
}
