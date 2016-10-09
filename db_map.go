package gorq

import (
	"github.com/go-gorp/gorp"
	"github.com/nelsam/gorq/interfaces"
	"github.com/nelsam/gorq/plans"
)

// DbMap embeds "github.com/go-gorp/gorp".DbMap and adds query
// methods to it.
type DbMap struct {
	gorp.DbMap
}

// Query returns a Query type, which can be used to generate and run
// queries using Go values instead of SQL literals.  The main
// advantage to this method over e.g. dbMap.Select() is that most
// silly mistakes will get caught at compile time instead of run time.
// This method makes use of struct fields instead of column names, and
// ensures some basic sanity in query structure by way of the
// interface types that are used as return values.
//
// The target must be the pointer to a struct value, which will be
// used as the reference for all query construction methods.  The
// struct's type must have been registered with AddTable.
//
// The struct's field addresses will be used to look up column names,
// so you have to use the same struct value for all method calls.
//
// When passing fields to be used for column lookup, always pass the
// address of the field that you want to add to the query within the
// reference struct that you passed to Query().  Here is a short
// example of how you might run a query using this method:
//
//     // Allocate memory for a struct type to use as reference
//     queryType := Model{}
//
//     // Use the memory addresses of the model and its fields as
//     // references to look up column names
//     results, err := dbMap.Query(&queryType).
//         Where().
//         Equal(&queryType.Name, "foo").
//         Greater(&queryType.StartDate, time.Now()).
//         Select()
//
// See the interfaces package for details on what the query types are
// capable of.
func (m *DbMap) Query(target interface{}) interfaces.Query {
	gorpMap := &m.DbMap
	return plans.Query(gorpMap, gorpMap, target)
}

// Begin acts just like "github.com/go-gorp/gorp".DbMap.Begin,
// except that its return type is gorq.Transaction.
func (m *DbMap) Begin() (*Transaction, error) {
	t, err := m.DbMap.Begin()
	if err != nil {
		return nil, err
	}
	return &Transaction{Transaction: *t, dbmap: m}, nil
}

// Transaction embeds "github.com/go-gorp/gorp".Transaction and
// adds query methods to it.
type Transaction struct {
	gorp.Transaction
	dbmap *DbMap
}

// Query runs a query within a transaction.  See DbMap.Query for full
// documentation.
func (t *Transaction) Query(target interface{}) interfaces.Query {
	return plans.Query(&t.dbmap.DbMap, &t.Transaction, target)
}
