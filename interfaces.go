package gorp_queries

// An Updater is a query that can execute UPDATE statements.
type Updater interface {
	Update() (rowsUpdated int64, err error)
}

// A Deleter is a query that can execute DELETE statements.
type Deleter interface {
	Delete() (rowsDeleted int64, err error)
}

// An Inserter is a query that can execute INSERT statements.
type Inserter interface {
	Insert() error
}

// A Selector is a query that can execute SELECT statements.
type Selector interface {
	// Execute the select statement, return the results as a slice of
	// the type that was used to create the query.
	Select() (results []interface{}, err error)

	// Execute the select statement, but use the passed in slice
	// pointer as the target to append to.
	SelectToTarget(target interface{}) error
}

// A SelectManipulator is a query that will return a list of results
// which can be manipulated.
type SelectManipulator interface {
	OrderBy(fieldPtr interface{}, direction string) SelectQuery
	GroupBy(fieldPtr interface{}) SelectQuery
	Limit(int64) SelectQuery
	Offset(int64) SelectQuery
}

// An Assigner is a query that can set columns to values.
type Assigner interface {
	Assign(fieldPtr interface{}, value interface{}) AssignQuery
}

// A Joiner is a query that can add tables as join clauses.
type Joiner interface {
	Join(table interface{}) JoinQuery
}

// An AssignJoiner is a Joiner with an assigner return type, for
// insert or update statements with a FROM clause.
type AssignJoiner interface {
	Join(table interface{}) AssignJoinQuery
}

// A Wherer is a query that can execute statements with a WHERE
// clause.
type Wherer interface {
	Where(...Filter) WhereQuery
}

// An AssignWherer is a Wherer with an assigner return type.
type AssignWherer interface{
	Where(...Filter) UpdateQuery
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
	Filter(...Filter) UpdateQuery

	// Equal, NotEqual, Less, LessOrEqual, Greater, GreaterOrEqual,
	// and NotNull are all what you would expect.  Use them for adding
	// constraints to a query.  More than one constraint will be ANDed
	// together.
	Equal(fieldPtr interface{}, value interface{}) UpdateQuery
	NotEqual(fieldPtr interface{}, value interface{}) UpdateQuery
	Less(fieldPtr interface{}, value interface{}) UpdateQuery
	LessOrEqual(fieldPtr interface{}, value interface{}) UpdateQuery
	Greater(fieldPtr interface{}, value interface{}) UpdateQuery
	GreaterOrEqual(fieldPtr interface{}, value interface{}) UpdateQuery
	NotNull(fieldPtr interface{}) UpdateQuery
	Null(fieldPtr interface{}) UpdateQuery

	// An UpdateQuery has both assignments and a where clause, which
	// means the only query type it could be is an UPDATE statement.
	Updater
}

// An AssignQuery is a query that may set values.
type AssignQuery interface {
	Assigner
	AssignJoiner
	AssignWherer
	Inserter
	Updater
}

// An AssignJoinQuery is a clone of JoinQuery, but for UPDATE and
// INSERT statements instead of DELETE and SELECT.
type AssignJoinQuery interface {
	AssignJoiner

	On(...Filter) AssignJoinQuery

	Equal(fieldPtr interface{}, value interface{}) AssignJoinQuery
	NotEqual(fieldPtr interface{}, value interface{}) AssignJoinQuery
	Less(fieldPtr interface{}, value interface{}) AssignJoinQuery
	LessOrEqual(fieldPtr interface{}, value interface{}) AssignJoinQuery
	Greater(fieldPtr interface{}, value interface{}) AssignJoinQuery
	GreaterOrEqual(fieldPtr interface{}, value interface{}) AssignJoinQuery
	NotNull(fieldPtr interface{}) AssignJoinQuery
	Null(fieldPtr interface{}) AssignJoinQuery

	AssignWherer
	Updater
}

// A JoinQuery is a query that uses join operations to compare values
// between tables.
type JoinQuery interface {
	Joiner

	// On for a JoinQuery is equivalent to Filter for a WhereQuery.
	On(...Filter) JoinQuery

	// These methods should be roughly equivalent to those of a
	// WhereQuery, except they add to the ON clause instead of the
	// WHERE clause.
	Equal(fieldPtr interface{}, value interface{}) JoinQuery
	NotEqual(fieldPtr interface{}, value interface{}) JoinQuery
	Less(fieldPtr interface{}, value interface{}) JoinQuery
	LessOrEqual(fieldPtr interface{}, value interface{}) JoinQuery
	Greater(fieldPtr interface{}, value interface{}) JoinQuery
	GreaterOrEqual(fieldPtr interface{}, value interface{}) JoinQuery
	NotNull(fieldPtr interface{}) JoinQuery
	Null(fieldPtr interface{}) JoinQuery

	Wherer
	Deleter
	Selector
}

// A WhereQuery is a query that does not set any values, but may have
// a where clause.
type WhereQuery interface {
	// Filter is used for queries that are more complex than a few
	// ANDed constraints.
	Filter(...Filter) WhereQuery

	// Equal, NotEqual, Less, LessOrEqual, Greater, GreaterOrEqual,
	// and NotNull are all what you would expect.  Use them for adding
	// constraints to a query.  More than one constraint will be ANDed
	// together.
	Equal(fieldPtr interface{}, value interface{}) WhereQuery
	NotEqual(fieldPtr interface{}, value interface{}) WhereQuery
	Less(fieldPtr interface{}, value interface{}) WhereQuery
	LessOrEqual(fieldPtr interface{}, value interface{}) WhereQuery
	Greater(fieldPtr interface{}, value interface{}) WhereQuery
	GreaterOrEqual(fieldPtr interface{}, value interface{}) WhereQuery
	NotNull(fieldPtr interface{}) WhereQuery
	Null(fieldPtr interface{}) WhereQuery

	// A WhereQuery should be used when a where clause was requested
	// right off the bat, which means there have been no calls to
	// Assign.  Only delete and select statements can have a where
	// clause without doing assignment.
	SelectManipulator
	Deleter
	Selector
}

// A Query is the base query type - as methods are called, the type of
// query will gradually be restricted based on which types of queries
// are capable of performing the requested operations.
//
// For example, UPDATE statements may both set values and have a where
// clause, but SELECT and DELETE statements cannot set values, and
// INSERT statements cannot have a WHERE clause.  SELECT statements
// are the only types that can have a GROUP BY, ORDER BY, or LIMIT
// clause.
//
// Because of this design, the following would actually be a compile
// error:
//
//     t := new(myType)
//     q, err := dbmap.Query(t).
//         Assign(&t.Foo, "test").
//         Where().
//         Less(&t.Created, time.Now()).
//         Insert()
//
// Since the return value from Assign() is an AssignQuery, the return value
// from Where() will be an UpdateQuery, which doesn't have an Insert()
// method.
type Query interface {
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
