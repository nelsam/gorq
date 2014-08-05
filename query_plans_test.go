package gorp_queries

import (
	"testing"
	"github.com/coopernurse/gorp"
	"github.com/nelsam/gorp_queries/filters"
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

var testInvoices = []OverriddenInvoice{
	OverriddenInvoice{
		Id: "1",
		Invoice: Invoice{
			Created: 1,
			Updated: 1,
			Memo: "test_memo",
			PersonId: 1,
			IsPaid: false,
		},
	},
	OverriddenInvoice{
		Id: "2",
		Invoice: Invoice{
			Created: 2,
			Updated: 2,
			Memo: "another_test_memo",
			PersonId: 2,
			IsPaid: false,
		},
	},
	OverriddenInvoice{
		Id: "3",
		Invoice: Invoice{
			Created: 1,
			Updated: 3,
			Memo: "test_memo",
			PersonId: 1,
			IsPaid: false,
		},
	},
	OverriddenInvoice{
		Id: "4",
		Invoice: Invoice{
			Created: 2,
			Updated: 1,
			Memo: "another_test_memo",
			PersonId: 1,
			IsPaid: true,
		},
	},
	OverriddenInvoice{
		Id: "5",
		Invoice: Invoice{
			Created: 1,
			Updated: 3,
			Memo: "test_memo",
			PersonId: 1,
			IsPaid: false,
		},
	},
}

type QueryLanguageTestSuite struct {
	suite.Suite
	Map *DbMap
	Ref *OverriddenInvoice
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
	suite.Ref = new(OverriddenInvoice)
	suite.Map.AddTable(OverriddenInvoice{}).SetKeys(false, "Id")
	err := suite.Map.CreateTablesIfNotExists()
	if !suite.NoError(err) {
		suite.T().FailNow()
	}
	suite.insertInvoices()
}

func (suite *QueryLanguageTestSuite) TearDownTest() {
	suite.Map.Exec("drop table OverriddenInvoice")
}

func (suite *QueryLanguageTestSuite) TearDownSuite() {
	suite.Map.Db.Close()
}

// insertInvoices() runs some insert queries to ensure that there is
// data available for the other queries.  If any error occurs, it will
// call suite.T().FailNow() to skip testing the rest of the suite.
func (suite *QueryLanguageTestSuite) insertInvoices() {
	for _, inv := range testInvoices {
		err := suite.Map.Query(suite.Ref).
			Assign(&suite.Ref.Id, inv.Id).
			Assign(&suite.Ref.Created, inv.Created).
			Assign(&suite.Ref.Updated, inv.Updated).
			Assign(&suite.Ref.Memo, inv.Memo).
			Assign(&suite.Ref.PersonId, inv.PersonId).
			Assign(&suite.Ref.IsPaid, inv.IsPaid).
			Insert()
		if !suite.NoError(err) {
			suite.T().FailNow()
		}
	}
}

func (suite *QueryLanguageTestSuite) TestQueryLanguage_SelectSimple() {
	invTest, err := suite.Map.Query(suite.Ref).Select()
	if suite.NoError(err) {
		suite.Equal(len(invTest), len(testInvoices))
	}
}

func (suite *QueryLanguageTestSuite) TestQueryLanguage_CountSimple() {
	count, err := suite.Map.Query(suite.Ref).Count()
	if suite.NoError(err) {
		suite.Equal(count, len(testInvoices))
	}
}

func (suite *QueryLanguageTestSuite) TestQueryLanguage_SelectEqual() {
	isPaidCount := 0
	for _, inv := range testInvoices {
		if inv.IsPaid {
			isPaidCount++
		}
	}
	if isPaidCount == 0 {
		panic("Cannot continue test without at least one paid invoice.")
	}
	invTest, err := suite.Map.Query(suite.Ref).
		Where().
		True(&suite.Ref.IsPaid).
		Limit(1).
		Select()
	if suite.NoError(err) {
		suite.Equal(len(invTest), 1)
	}
}

func (suite *QueryLanguageTestSuite) TestQueryLanguage_Update() {
	var (
		targetInv OverriddenInvoice
		isPaidCount int64
	)
	for _, inv := range testInvoices {
		if inv.IsPaid {
			isPaidCount++
		} else if targetInv.Id == "" {
			targetInv = inv
		}
	}
	count, err := suite.Map.Query(suite.Ref).
		Assign(&suite.Ref.IsPaid, true).
		Where().
		Equal(&suite.Ref.Id, targetInv.Id).
		Update()
	if suite.NoError(err) {
		suite.Equal(count, 1)
		isPaidCount += count
	}

	invTest, err := suite.Map.Query(suite.Ref).
		Where().
		True(&suite.Ref.IsPaid).
		Select()
	if suite.NoError(err) {
		suite.Equal(len(invTest), isPaidCount)
	}
}

func (suite *QueryLanguageTestSuite) TestQueryLanguage_SelectGreater() {
	invTest, err := suite.Map.Query(suite.Ref).
		Where().
		Greater(&suite.Ref.Updated, 1).
		Select()
	if suite.NoError(err) {
		expectedCount := 0
		for _, inv := range testInvoices {
			if inv.Updated > 1 {
				expectedCount++
			}
		}
		suite.Equal(len(invTest), expectedCount)
	}

	invTest, err = suite.Map.Query(suite.Ref).
		Where().
		Greater(&suite.Ref.Updated, 1).
		Offset(1).
		Limit(1).
		Select()
	if suite.NoError(err) {
		suite.Equal(len(invTest), 1)
	}
}

func (suite *QueryLanguageTestSuite) TestQueryLanguage_SelectFalse() {
	invTest, err := suite.Map.Query(suite.Ref).
		Where().
		False(&suite.Ref.IsPaid).
		Select()
	if suite.NoError(err) {
		expectedCount := 0
		for _, inv := range testInvoices {
			if !inv.IsPaid {
				expectedCount++
			}
		}
		suite.Equal(len(invTest), expectedCount)
	}
}

func (suite *QueryLanguageTestSuite) TestQueryLanguage_SelectFalseAndEqual() {
	count, err := suite.Map.Query(suite.Ref).
		Where().
		False(&suite.Ref.IsPaid).
		Equal(&suite.Ref.Created, 2).
		Count()
	if suite.NoError(err) {
		expectedCount := 0
		for _, inv := range testInvoices {
			if !inv.IsPaid && inv.Created == 2 {
				expectedCount++
			}
		}
		suite.Equal(count, expectedCount)
	}
}

func (suite *QueryLanguageTestSuite) TestQueryLanguage_SelectWithFilter() {
	invTest, err := suite.Map.Query(suite.Ref).
		Where().
		Filter(filters.Or(filters.Equal(&suite.Ref.Memo, "another_test_memo"), filters.Equal(&suite.Ref.Updated, 3))).
		Select()
	if suite.NoError(err) {
		expectedCount := 0
		for _, inv := range testInvoices {
			if inv.Memo == "another_test_memo" || inv.Updated == 3 {
				expectedCount++
			}
		}
		suite.Equal(len(invTest), expectedCount)
	}
}

func (suite *QueryLanguageTestSuite) TestQueryLanguage_Delete() {
	count, err := suite.Map.Query(suite.Ref).
		Where().
		False(&suite.Ref.IsPaid).
		Delete()
	if suite.NoError(err) {
		expectedCount := 0
		for _, inv := range testInvoices {
			if !inv.IsPaid {
				expectedCount++
			}
		}
		suite.Equal(count, expectedCount)

		count, err = suite.Map.Query(suite.Ref).
			Where().
			False(&suite.Ref.IsPaid).
			Count()
		if suite.NoError(err) {
			suite.Equal(count, 0, "No unpaid invoices should exist after deleting all unpaid invoices")
		}
	}
}

func (suite *QueryLanguageTestSuite) TestQueryLanguage_WhereClauseLower() {
	inv := OverriddenInvoice{
		Id: "79",
		Invoice: Invoice{
			Created: 2,
			Updated: 1,
			Memo: "A Test Memo With Capitals",
			PersonId: 1,
			IsPaid: true,
		},
	}
	err := suite.Map.Query(&inv).
		Assign(&inv.Id, inv.Id).
		Assign(&inv.Created, inv.Created).
		Assign(&inv.Updated, inv.Updated).
		Assign(&inv.Memo, inv.Memo).
		Assign(&inv.PersonId, inv.PersonId).
		Assign(&inv.IsPaid, inv.IsPaid).
		Insert()
	if !suite.NoError(err) {
		suite.T().FailNow()
	}
	invTest, err := suite.Map.Query(&inv).
		Where().
		Equal(Lower(&inv.Memo), "a test memo with capitals").
		Select()
	if suite.NoError(err) {
		if suite.Equal(len(invTest), 1) {
			suite.Equal(invTest[0].(*OverriddenInvoice).Memo, "A Test Memo With Capitals")
		}
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
