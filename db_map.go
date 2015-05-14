package gorq

import (
	"fmt"
	"reflect"

	"github.com/memcachier/mc"
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

// Invalidator is a type that allows for creating a relationship
// between two objects such that the first will be invalidated
// in cache if any of the others change
type Invalidator struct {
	dbMap    *DbMap
	original interface{}
}

// InvalidateOnChange associates the object from the previous call with
// the provided targets, invalidating the first if any of the others change.
func (i *Invalidator) InvalidateOnChange(targets ...interface{}) {
	if i.dbMap.invalidate == nil {
		i.dbMap.invalidate = map[string][]interface{}{}
	}
	for _, target := range targets {
		i.dbMap.invalidate[fullTypePath(target)] = append(i.dbMap.invalidate[fullTypePath(target)], i.original)
	}
}

func fullTypePath(object interface{}) string {
	t := reflect.TypeOf(object)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return t.PkgPath() + "." + t.Name()
}

// DbMap embeds "github.com/outdoorsy/gorp".DbMap and adds query
// methods to it.
type DbMap struct {
	gorp.DbMap
	MemCache   *mc.Conn
	cacheable  map[string]bool
	invalidate map[string][]interface{}
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
	fmt.Println("new query for ", fullTypePath(target), m.cacheable[fullTypePath(target)])
	return plans.Query(
		gorpMap,
		gorpMap,
		target,
		m.MemCache,
		m.cacheable[fullTypePath(target)],
		m.invalidate[fullTypePath(target)],
	)
}

// Begin acts just like "github.com/outdoorsy/gorp".DbMap.Begin,
// except that its return type is gorq.Transaction.
func (m *DbMap) Begin() (*Transaction, error) {
	t, err := m.DbMap.Begin()
	if err != nil {
		return nil, err
	}
	return &Transaction{Transaction: *t, dbmap: m}, nil
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
	return plans.Query(
		&t.dbmap.DbMap,
		&t.Transaction,
		target,
		t.dbmap.MemCache,
		t.dbmap.cacheable[fullTypePath(target)],
		t.dbmap.invalidate[fullTypePath(target)],
	)
}

// SetCacheable instructs gorq to cache the result of a query for this type in memcache
func (m *DbMap) SetCacheable(target interface{}, cacheable bool) *Invalidator {
	if m.cacheable == nil {
		m.cacheable = map[string]bool{}
	}
	fmt.Println("adding cacheable:", fullTypePath(target), cacheable)
	m.cacheable[fullTypePath(target)] = cacheable
	return &Invalidator{
		original: target,
		dbMap:    m,
	}
}
