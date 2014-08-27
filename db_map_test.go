package gorp_queries

import (
	"reflect"
	"testing"

	"github.com/coopernurse/gorp"
	"github.com/nelsam/gorp_queries/interfaces"
	"github.com/nelsam/gorp_queries/query_plans"
	"github.com/stretchr/testify/suite"
)

type ValidStruct struct {
	ExportedValue string
}

type QueryTestSuite struct {
	suite.Suite
	Exec     SqlExecutor
	TypeName string
}

func (suite *QueryTestSuite) getQueryFor(structType interface{}) *query_plans.QueryPlan {
	var ptr, val interface{}
	valueOfStruct := reflect.ValueOf(structType)
	if valueOfStruct.Kind() == reflect.Ptr {
		ptr = structType
		val = valueOfStruct.Elem().Interface()
	} else {
		ptr = reflect.New(valueOfStruct.Type()).Interface()
		val = structType
	}

	q := suite.Exec.Query(val)
	suite.Implements((*interfaces.Query)(nil), q)
	if plan, ok := q.(*query_plans.QueryPlan); suite.True(ok) {
		suite.NotEqual(0, len(plan.Errors),
			"%s.Query(ref) should error if ref is not a pointer to a struct", suite.TypeName)
	}

	q = suite.Exec.Query(ptr)
	if plan, ok := q.(*query_plans.QueryPlan); suite.True(ok) {
		return plan
	}
	return nil
}

func (suite *QueryTestSuite) TestDbMapQuery_ValidStruct() {
	q := suite.getQueryFor(ValidStruct{})
	suite.Equal(0, len(q.Errors),
		"%s.Query(ref) should not generate errors if ref is a pointer to a struct with exported fields", suite.TypeName)
}

type DbMapTestSuite struct {
	QueryTestSuite
}

func TestDbMapSuite(t *testing.T) {
	suite.Run(t, new(DbMapTestSuite))
}

func (suite *DbMapTestSuite) SetupSuite() {
	suite.TypeName = "DbMap"
	dbMap := new(DbMap)
	dbMap.Dialect = gorp.SqliteDialect{}
	dbMap.AddTable(ValidStruct{})
	suite.Exec = dbMap
}

// TODO: Set up a proper DB connection, most likely with sqlite3, so
// that we can test Begin().  Probably not a huge deal, though, since
// we'd really just be checking the return type.

type TransactionTestSuite struct {
	DbMapTestSuite
}

func TestTransactionSuite(t *testing.T) {
	suite.Run(t, new(TransactionTestSuite))
}

func (suite *TransactionTestSuite) SetupSuite() {
	suite.DbMapTestSuite.SetupSuite()
	suite.TypeName = "Transaction"
	dbMap := suite.Exec.(*DbMap)
	trans := new(Transaction)
	trans.dbmap = dbMap
	suite.Exec = trans
}
