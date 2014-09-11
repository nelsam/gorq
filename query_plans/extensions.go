package query_plans

import (
	"github.com/nelsam/gorp"
	"errors"
)

type extensionMap struct {
	dialect gorp.Dialect
	constructor func(*QueryPlan) interface{}
}

var (
	extensions []extensionMap
	ExtensionNotFound = errors.New("No extension has been registered for the requested dialect")
)

// RegisterExtendedSQL registers a query constructor to a
// gorp.Dialect.  This is for returning the correct query type when
// Extend() is called on a query.  The constructor will be passed a
// pointer to the QueryPlan that Extend() was called on.
func RegisterExtension(dialect gorp.Dialect, queryConstructor func(*QueryPlan) interface{}) {
	extensions = append(extensions, extensionMap{dialect: dialect, constructor: queryConstructor})
}

func LoadExtension(dialect gorp.Dialect, query *QueryPlan) (interface{}, error) {
	for _, extension := range extensions {
		if extension.dialect == dialect {
			return extension.constructor(query), nil
		}
	}
	return nil, ExtensionNotFound
}
