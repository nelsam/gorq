package plans

type order struct {
	fieldOrWrapper interface{}
	direction      string
}

// ActualValue returns the actual value requested as the order
// target.  OrderBy expects the passed in sqlValue to in some way
// represent the value returned by ActualValue.
func (o order) ActualValue() interface{} {
	return o.fieldOrWrapper
}

// OrderBy returns the string that should follow " ORDER BY " in a
// query, using sqlValue as the value to order by.
func (o order) OrderBy(sqlValue string) string {
	if o.direction != "" {
		sqlValue += " " + o.direction
	}
	return sqlValue
}
