package gorq

import (
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/outdoorsy/gorp"
	"github.com/outdoorsy/gorq/interfaces"
	"github.com/outdoorsy/gorq/plans"
)

// SqlExecutor is any type that can execute SQL statements.  Gorq's
// SqlExecutor matches that of gorp, but has some additional methods.
type SqlExecutor interface {
	gorp.SqlExecutor

	// Query should return a Query type that will perform queries
	// against target.
	Query(target interface{}) interfaces.Query
}

// DbMap embeds "github.com/outdoorsy/gorp".DbMap and adds query
// methods to it.
type DbMap struct {
	gorp.DbMap
	joinOps []plans.JoinOp
}

func (m *DbMap) JoinOp(target, fieldPtrOrName interface{}, op plans.JoinFunc) error {
	var (
		err   error
		newOp = plans.JoinOp{}
	)
	if newOp.Table, err = m.TableFor(reflect.TypeOf(target), false); err != nil {
		return err
	}
	if newOp.Column = newOp.Table.ColMap(fieldPtrOrName); newOp.Column == nil {
		return errors.New("No column found for the passed in field name or pointer")
	}
	newOp.Join = op
	m.joinOps = append(m.joinOps, newOp)
	return nil
}

// Returns the []plan.JoinOp for the DbMap. Useful for creating a
// plans.Query with a gorp.SqlExecutor.
func (m *DbMap) JoinOps() []plans.JoinOp {
	return m.joinOps
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
	return plans.Query(gorpMap, gorpMap, target, m.joinOps...)
}

// Begin acts just like "github.com/outdoorsy/gorp".DbMap.Begin,
// except that its return type is gorq.Transaction.
func (m *DbMap) Begin(timeout time.Duration) (*Transaction, error) {
	t, err := m.DbMap.Begin()
	if err != nil {
		return nil, err
	}

	// we don't want any transaction to run longer than 60 seconds
	if timeout != 0 {
		_, err = t.Exec(fmt.Sprintf("SET statement_timeout=%d;", int64(timeout/time.Millisecond)))
		if err != nil {
			return nil, err
		}
	}

	return &Transaction{Transaction: *t, dbmap: m}, nil
}

func (m *DbMap) table(target interface{}) *gorp.TableMap {
	t := reflect.TypeOf(target)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	table, err := m.TableFor(t, false)
	if err != nil {
		return nil
	}
	return table
}

// Transaction embeds "github.com/outdoorsy/gorp".Transaction and
// adds query methods to it.
type Transaction struct {
	gorp.Transaction
	dbmap *DbMap
}

// Query runs a query within a transaction.  See DbMap.Query for full
// documentation.
func (t *Transaction) Query(target interface{}) interfaces.Query {
	return plans.Query(&t.dbmap.DbMap, &t.Transaction, target, t.dbmap.joinOps...)
}

// DbMap is used to get a reference to the underlying dbmap the Transaction is using to do its work.
//
// In some cases, we have a Transaction, but need access to the DbMap in order to do work outside
// the transaction or on another goroutine. This allows us to extract it immediately without having
// to pass the original DbMap alongside.
func (t *Transaction) DbMap() *DbMap {
	return t.dbmap
}

func GorpToGorq(exec gorp.SqlExecutor, m *DbMap) SqlExecutor {
	switch e := exec.(type) {
	case *gorp.Transaction:
		return &Transaction{Transaction: *e, dbmap: m}
	case *gorp.DbMap:
		return &DbMap{DbMap: *e}
	}
	panic("unable to convert gorp to gorq")
}
