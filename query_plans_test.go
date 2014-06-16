package gorp_queries

import (
	//"log"
	//"os"
	"testing"
	"github.com/coopernurse/gorp"
	"github.com/nelsam/gorp_queries/filters"
	//"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
	_ "github.com/ziutek/mymysql/godrv"
	"database/sql"
)

type Invoice struct {
	Id       int64
	Created  int64
	Updated  int64
	Memo     string
	PersonId int64
	IsPaid   bool
}

type OverriddenInvoice struct {
	Invoice
	Id string
}

type QueryLanguageTestSuite struct {
	suite.Suite
	Map *DbMap
	CanTest bool
}

func runQueryLanguageSuite(t *testing.T, dialect gorp.Dialect, connection *sql.DB) {
	dbMap := new(DbMap)
	dbMap.Dialect = dialect
	dbMap.Db = connection
	testSuite := &QueryLanguageTestSuite{Map: dbMap}
	suite.Run(t, testSuite)
}

func TestQueryLanguagePostgres(t *testing.T) {
	dialect := gorp.PostgresDialect{}
	connection, err := sql.Open("postgres", "user=gorptest password=gorptest dbname=gorptest sslmode=disable")
	if err != nil {
		t.Errorf("Could not connect to postgres: %s", err)
		return
	}
	runQueryLanguageSuite(t, dialect, connection)
}

func TestQueryLanguageMyMySql(t *testing.T) {
	dialect := gorp.MySQLDialect{"InnoDB", "UTF8"}
	connection, err := sql.Open("mymysql", "gorptest/gorptest/gorptest")
	if err != nil {
		t.Errorf("Could not connect to mysql (using mysql bindings): %s", err)
		return
	}
	runQueryLanguageSuite(t, dialect, connection)
}

func TestQueryLanguageMySql(t *testing.T) {
	dialect := gorp.MySQLDialect{"InnoDB", "UTF8"}
	connection, err := sql.Open("mysql", "gorptest:gorptest@/gorptest")
	if err != nil {
		t.Errorf("Could not connect to mysql (using native mysql): %s", err)
		return
	}
	runQueryLanguageSuite(t, dialect, connection)
}

func TestQueryLanguageSqlite(t *testing.T) {
	dialect := gorp.SqliteDialect{}
	connection, err := sql.Open("sqlite3", "/tmp/gorptest.bin")
	if err != nil {
		t.Errorf("Could not connect to sqlite: %s", err)
		return
	}
	runQueryLanguageSuite(t, dialect, connection)
}

func (suite *QueryLanguageTestSuite) SetupTest() {
	suite.Map.AddTable(OverriddenInvoice{}).SetKeys(false, "Id")
	err := suite.Map.CreateTablesIfNotExists()
	if err == nil {
		suite.CanTest = true
	} else {
		suite.T().Errorf("Cannot run tests: %s", err)
	}
}

func (suite *QueryLanguageTestSuite) TearDownTest() {
	suite.Map.Exec("drop table OverriddenInvoice")
}

func (suite *QueryLanguageTestSuite) TearDownSuite() {
	suite.Map.Exec("drop table OverriddenInvoice")
	suite.Map.Db.Close()
}

func (suite *QueryLanguageTestSuite) TestQueryLanguage() {
	if !suite.CanTest {
		return
	}

	dbmap := suite.Map

	emptyInv := new(OverriddenInvoice)

	err := dbmap.Query(emptyInv).
		Assign(&emptyInv.Id, "1").
		Assign(&emptyInv.Created, 1).
		Assign(&emptyInv.Updated, 1).
		Assign(&emptyInv.Memo, "test_memo").
		Assign(&emptyInv.PersonId, 1).
		Assign(&emptyInv.IsPaid, false).
		Insert()
	if err != nil {
		suite.T().Errorf("Failed to insert: %s", err)
		suite.T().FailNow()
	}

	err = dbmap.Query(emptyInv).
		Assign(&emptyInv.Id, "2").
		Assign(&emptyInv.Created, 2).
		Assign(&emptyInv.Updated, 2).
		Assign(&emptyInv.Memo, "another_test_memo").
		Assign(&emptyInv.PersonId, 2).
		Assign(&emptyInv.IsPaid, false).
		Insert()
	if err != nil {
		suite.T().Errorf("Failed to insert: %s", err)
		suite.T().FailNow()
	}

	err = dbmap.Query(emptyInv).
		Assign(&emptyInv.Id, "3").
		Assign(&emptyInv.Created, 1).
		Assign(&emptyInv.Updated, 3).
		Assign(&emptyInv.Memo, "test_memo").
		Assign(&emptyInv.PersonId, 1).
		Assign(&emptyInv.IsPaid, false).
		Insert()
	if err != nil {
		suite.T().Errorf("Failed to insert: %s", err)
		suite.T().FailNow()
	}

	err = dbmap.Query(emptyInv).
		Assign(&emptyInv.Id, "4").
		Assign(&emptyInv.Created, 2).
		Assign(&emptyInv.Updated, 1).
		Assign(&emptyInv.Memo, "another_test_memo").
		Assign(&emptyInv.PersonId, 1).
		Assign(&emptyInv.IsPaid, false).
		Insert()
	if err != nil {
		suite.T().Errorf("Failed to insert: %s", err)
		suite.T().FailNow()
	}

	invTest, err := dbmap.Query(emptyInv).
		Where().
		Equal(&emptyInv.IsPaid, true).
		Limit(1).
		Select()
	if err != nil {
		suite.T().Errorf("Failed to select: %s", err)
		suite.T().FailNow()
	}
	if len(invTest) != 0 {
		suite.T().Errorf("Expected zero paid invoices")
		suite.T().FailNow()
	}

	count, err := dbmap.Query(emptyInv).
		Assign(&emptyInv.IsPaid, true).
		Where().
		Equal(&emptyInv.Id, "4").
		Update()
	if err != nil {
		suite.T().Errorf("Failed to update: %s", err)
		suite.T().FailNow()
	}
	if count != 1 {
		suite.T().Errorf("Expected to update one invoice")
		suite.T().FailNow()
	}

	invTest, err = dbmap.Query(emptyInv).
		Where().
		Equal(&emptyInv.IsPaid, true).
		Select()
	if err != nil {
		suite.T().Errorf("Failed to select: %s", err)
		suite.T().FailNow()
	}
	if len(invTest) != 1 {
		suite.T().Errorf("Expected one paid invoice")
		suite.T().FailNow()
	}

	invTest, err = dbmap.Query(emptyInv).
		Where().
		Greater(&emptyInv.Updated, 1).
		Select()
	if err != nil {
		suite.T().Errorf("Failed to select: %s", err)
		suite.T().FailNow()
	}
	if len(invTest) != 2 {
		suite.T().Errorf("Expected two inv")
		suite.T().FailNow()
	}

	invTest, err = dbmap.Query(emptyInv).
		Where().
		Greater(&emptyInv.Updated, 1).
		Offset(1).
		Limit(1).
		Select()
	if err != nil {
		suite.T().Errorf("Failed to select: %s", err)
		suite.T().FailNow()
	}
	if len(invTest) != 1 {
		suite.T().Errorf("Expected two inv")
		suite.T().FailNow()
	}

	invTest, err = dbmap.Query(emptyInv).
		Where().
		Equal(&emptyInv.IsPaid, true).
		Select()
	if err != nil {
		suite.T().Errorf("Failed to select: %s", err)
		suite.T().FailNow()
	}
	if len(invTest) != 1 {
		suite.T().Errorf("Expected one inv")
		suite.T().FailNow()
	}

	invTest, err = dbmap.Query(emptyInv).
		Where().
		Equal(&emptyInv.IsPaid, false).
		Select()
	if err != nil {
		suite.T().Errorf("Failed to select: %s", err)
		suite.T().FailNow()
	}
	if len(invTest) != 3 {
		suite.T().Errorf("Expected three inv")
		suite.T().FailNow()
	}

	invTest, err = dbmap.Query(emptyInv).
		Where().
		Equal(&emptyInv.IsPaid, false).
		Equal(&emptyInv.Created, 2).
		Select()
	if err != nil {
		suite.T().Errorf("Failed to select: %s", err)
		suite.T().FailNow()
	}
	if len(invTest) != 1 {
		suite.T().Errorf("Expected one inv")
		suite.T().FailNow()
	}

	invTest, err = dbmap.Query(emptyInv).
		Where().
		Filter(filters.Or(filters.Equal(&emptyInv.Memo, "another_test_memo"), filters.Equal(&emptyInv.Updated, 3))).
		Select()
	if err != nil {
		suite.T().Errorf("Failed to select: %s", err)
		suite.T().FailNow()
	}
	if len(invTest) != 3 {
		suite.T().Errorf("Expected three invoices for ORed query")
		suite.T().FailNow()
	}

	count, err = dbmap.Query(emptyInv).
		Where().
		Equal(&emptyInv.IsPaid, true).
		Delete()
	if err != nil {
		suite.T().Errorf("Failed to delete: %s", err)
		suite.T().FailNow()
	}
	if count != 1 {
		suite.T().Errorf("Expected one invoice to be deleted")
		suite.T().FailNow()
	}

	invTest, err = dbmap.Query(emptyInv).
		Where().
		Equal(&emptyInv.IsPaid, true).
		Select()
	if err != nil {
		suite.T().Errorf("Failed to select: %s", err)
		suite.T().FailNow()
	}
	if len(invTest) != 0 {
		suite.T().Errorf("Expected no paid invoices after deleting all paid invoices")
		suite.T().FailNow()
	}
}

// func BenchmarkSqlQuerySelect(b *testing.B) {
// 	b.StopTimer()
// 	dbmap := newDbMap()
// 	dbmap.Exec("drop table if exists OverriddenInvoice")
// 	dbmap.TraceOff()
// 	dbmap.AddTable(OverriddenInvoice{}).SetKeys(false, "Id")
// 	err := dbmap.CreateTablesIfNotExists()
// 	if err != nil {
// 		panic(err)
// 	}
// 	defer dropAndClose(dbmap)

// 	inv := &OverriddenInvoice{
// 		Id: "1",
// 		Invoice: Invoice{
// 			Created:  1,
// 			Updated:  1,
// 			Memo:     "test_memo",
// 			PersonId: 1,
// 			IsPaid:   false,
// 		},
// 	}
// 	err = dbmap.Insert(inv)
// 	if err != nil {
// 		panic(err)
// 	}
// 	b.StartTimer()
// 	for i := 0; i < b.N; i++ {
// 		q := "SELECT * FROM overriddeninvoice WHERE memo = $1"
// 		_, err = dbmap.Select(inv, q, "test_memo")
// 		if err != nil {
// 			panic(err)
// 		}
// 	}
// }

// func BenchmarkGorpQuerySelect(b *testing.B) {
// 	b.StopTimer()
// 	dbmap := newDbMap()
// 	dbmap.Exec("drop table if exists OverriddenInvoice")
// 	dbmap.TraceOff()
// 	dbmap.AddTable(OverriddenInvoice{}).SetKeys(false, "Id")
// 	err := dbmap.CreateTablesIfNotExists()
// 	if err != nil {
// 		panic(err)
// 	}
// 	defer dropAndClose(dbmap)

// 	inv := &OverriddenInvoice{
// 		Id: "1",
// 		Invoice: Invoice{
// 			Created:  1,
// 			Updated:  1,
// 			Memo:     "test_memo",
// 			PersonId: 1,
// 			IsPaid:   false,
// 		},
// 	}
// 	err = dbmap.Insert(inv)
// 	if err != nil {
// 		panic(err)
// 	}
// 	b.StartTimer()
// 	for i := 0; i < b.N; i++ {
// 		t := new(OverriddenInvoice)
// 		_, err := dbmap.Query(t).
// 			Where().
// 			Equal(&t.Memo, "test_memo").
// 			Select()
// 		if err != nil {
// 			panic(err)
// 		}
// 	}
// }
