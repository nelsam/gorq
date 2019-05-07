package gorq

import (
	"context"
	"database/sql"
	"reflect"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/outdoorsy/gorp"
	"github.com/outdoorsy/gorq/interfaces"
	"github.com/outdoorsy/gorq/plans"
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

func (suite *QueryTestSuite) getQueryFor(structType interface{}) *plans.QueryPlan {
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
	if plan, ok := q.(*plans.QueryPlan); suite.True(ok) {
		suite.NotEqual(0, len(plan.Errors),
			"%s.Query(ref) should error if ref is not a pointer to a struct", suite.TypeName)
	}

	q = suite.Exec.Query(ptr)
	if plan, ok := q.(*plans.QueryPlan); suite.True(ok) {
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
	// TODO: come back around and fix this some day
	suite.T().Skip("Skipping due to lack of support for execution timeouts in Sqlite")

	tx, err := suite.Exec.(*DbMap).Begin(1 * time.Second)
	if suite.NoError(err) {
		suite.IsType((*Transaction)(nil), tx)
	}
}

func (suite *DbMapTestSuite) TestAttachContext() {
	suite.T().Run("DbMap Attach Context", func(t *testing.T) {
		ctx := context.Background()
		withCtx := suite.Exec.AttachContext(ctx)

		dbmWithCtx := withCtx.(*DbMap)
		dbmWoutCtx := suite.Exec.(*DbMap)
		suite.NotEqual(dbmWoutCtx, dbmWithCtx)
		suite.NotEqual(dbmWithCtx.DbMap, dbmWoutCtx.DbMap)
	})

	suite.T().Run("Transaction AttachContext", func(t *testing.T) {
		t.Skip("Transactions not supported for Sqlite")
		ctx := context.Background()
		woutCtx, err := suite.Exec.(*DbMap).Begin(1 * time.Second)
		if err != nil {
			t.Error(err)
			t.FailNow()
		}
		withCtx := woutCtx.AttachContext(ctx).(*Transaction)

		suite.NotEqual(woutCtx, withCtx)
		suite.NotEqual(woutCtx.Transaction, withCtx.Transaction)
	})
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
