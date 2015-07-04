package filters

type SqlWrapper interface {
	// ActualValue should return the value to be used as a value or
	// column in the SQL query.
	ActualValue() interface{}

	// WrapSql should take the generated string that is being used to
	// represent the ActualValue() in the query, and wrap it in
	// whatever SQL this SqlWrapper needs to add to the query.
	WrapSql(string) string
}

// TODO: Add support for this in filters.  Currently used only for
// OrderBy.
type MultiSqlWrapper interface {
	// ActualValues should return the values to be used as values or
	// columns in the SQL query.
	ActualValues() []interface{}

	// WrapSql should take the generated strings that are being used
	// to represent the ActualValues() in the query, and wrap them in
	// whatever SQL this SqlWrapper needs to add to the query.
	WrapSql(...string) string
}
