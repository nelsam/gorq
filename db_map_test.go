package gorp_queries

import (
	"database/sql"
	"reflect"
	"testing"

	"github.com/nelsam/gorp"
	_ "github.com/mattn/go-sqlite3"
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

func (suite *QueryTestSuite) SetupSuite() {
	dbMap := new(DbMap)
	dbMap.Dialect = gorp.SqliteDialect{}
	connection, err := sql.Open("sqlite3", "/tmp/gorptest.bin")
	if !suite.NoError(err) {
		suite.T().FailNow()
	}
	dbMap.Db = connection
	dbMap.AddTable(ValidStruct{})
	suite.Exec = dbMap
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
	suite.QueryTestSuite.SetupSuite()
	suite.TypeName = "DbMap"
}

func (suite *DbMapTestSuite) TestBegin() {
	tx, err := suite.Exec.(*DbMap).Begin()
	if suite.NoError(err) {
		suite.IsType((*Transaction)(nil), tx)
	}
}

type TransactionTestSuite struct {
	QueryTestSuite
}

func TestTransactionSuite(t *testing.T) {
	suite.Run(t, new(TransactionTestSuite))
}

func (suite *TransactionTestSuite) SetupSuite() {
	suite.QueryTestSuite.SetupSuite()
	suite.TypeName = "Transaction"
	dbMap := suite.Exec.(*DbMap)
	trans := new(Transaction)
	trans.dbmap = dbMap
	suite.Exec = trans
}
