package plans

import (
	"database/sql"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/go-gorp/gorp"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
	"github.com/nelsam/gorq/dialects"
	"github.com/nelsam/gorq/filters"
	"github.com/nelsam/gorq/interfaces"
	"github.com/stretchr/testify/suite"
	_ "github.com/ziutek/mymysql/godrv"
)

var (
	postgresURL = os.Getenv("POSTGRES_TEST_URL")
	myMySQLURL  = os.Getenv("MY_MYSQL_TEST_URL")
	mySQLURL    = os.Getenv("MYSQL_TEST_URL")
	sqliteURL   = os.Getenv("SQLITE_TEST_URL")
)

type UnmappedStruct struct {
	Id int
}

type EmptyStruct struct{}

type InvalidStruct struct {
	notExportedValue string
}

type OnlyTransientFields struct {
	ExportedTransientValue string `db:"-"`
}

type ValidStruct struct {
	ExportedTransientValue string `db:"-"`
	ExportedValue          string
}

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
	Id           string
	TransientId  string `db:"-"`
	unexportedId string `db:"-"`
}

var testInvoices = []OverriddenInvoice{
	OverriddenInvoice{
		Id: "1",
		Invoice: Invoice{
			Created:  1,
			Updated:  1,
			Memo:     "test_memo",
			PersonId: 1,
			IsPaid:   false,
		},
	},
	OverriddenInvoice{
		Id: "2",
		Invoice: Invoice{
			Created:  2,
			Updated:  2,
			Memo:     "another_test_memo",
			PersonId: 2,
			IsPaid:   false,
		},
	},
	OverriddenInvoice{
		Id: "3",
		Invoice: Invoice{
			Created:  1,
			Updated:  3,
			Memo:     "test_memo",
			PersonId: 1,
			IsPaid:   false,
		},
	},
	OverriddenInvoice{
		Id: "4",
		Invoice: Invoice{
			Created:  2,
			Updated:  1,
			Memo:     "another_test_memo",
			PersonId: 1,
			IsPaid:   true,
		},
	},
	OverriddenInvoice{
		Id: "5",
		Invoice: Invoice{
			Created:  1,
			Updated:  3,
			Memo:     "test_memo",
			PersonId: 1,
			IsPaid:   false,
		},
	},
}

type DbTestSuite struct {
	suite.Suite
	Map *gorp.DbMap
}

func (suite *DbTestSuite) SetupSuite() {
	suite.Map.AddTable(InvalidStruct{})
	suite.Map.AddTable(ValidStruct{})
	suite.Map.AddTable(OverriddenInvoice{}).SetKeys(false, "Id")
	if err := suite.Map.CreateTablesIfNotExists(); !suite.NoError(err) {
		suite.T().FailNow()
	}

	// These cause syntax errors during table creation
	suite.Map.AddTable(OnlyTransientFields{})
	suite.Map.AddTable(EmptyStruct{})
}

func (suite *DbTestSuite) TearDownSuite() {
	suite.Map.Db.Close()
}

type QueryPlanTestSuite struct {
	DbTestSuite
}

func runQueryPlanSuite(t *testing.T, dialect gorp.Dialect, connection *sql.DB) {
	dbMap := new(gorp.DbMap)
	dbMap.Dialect = dialect
	dbMap.Db = connection
	testSuite := new(QueryPlanTestSuite)
	testSuite.Map = dbMap
	suite.Run(t, testSuite)
}

func TestQueryPlanPostgres(t *testing.T) {
	dialect := gorp.PostgresDialect{}
	connection, err := sql.Open("postgres", postgresURL)
	if err != nil {
		t.Errorf("Could not connect to postgres: %s", err)
		return
	}
	runQueryPlanSuite(t, dialect, connection)
}

func TestQueryPlanMyMySql(t *testing.T) {
	dialect := gorp.MySQLDialect{"InnoDB", "UTF8"}
	connection, err := sql.Open("mymysql", myMySQLURL)
	if err != nil {
		t.Errorf("Could not connect to mysql (using mysql bindings): %s", err)
		return
	}
	runQueryPlanSuite(t, dialect, connection)
}

func TestQueryPlanMySql(t *testing.T) {
	dialect := gorp.MySQLDialect{"InnoDB", "UTF8"}
	connection, err := sql.Open("mysql", mySQLURL)
	if err != nil {
		t.Errorf("Could not connect to mysql (using native mysql): %s", err)
		return
	}
	runQueryPlanSuite(t, dialect, connection)
}

func TestQueryPlanSqlite(t *testing.T) {
	dialect := gorp.SqliteDialect{}
	connection, err := sql.Open("sqlite3", sqliteURL)
	if err != nil {
		t.Errorf("Could not connect to sqlite: %s", err)
		return
	}
	runQueryPlanSuite(t, dialect, connection)
}

// TODO: DRY.  This is copied from ../db_map_test.go.
func (suite *QueryPlanTestSuite) getQueryPlanFor(value interface{}) *QueryPlan {
	var ptr, val interface{}
	valueOfStruct := reflect.ValueOf(value)
	if valueOfStruct.Kind() == reflect.Ptr {
		ptr = value
		val = valueOfStruct.Elem().Interface()
	} else {
		ptr = reflect.New(valueOfStruct.Type()).Interface()
		val = value
	}

	q := Query(suite.Map, suite.Map, val)
	suite.Implements((*interfaces.Query)(nil), q)
	if plan, ok := q.(*QueryPlan); suite.True(ok) {
		suite.NotEqual(0, len(plan.Errors),
			"Query(ref) should error if ref is not a pointer to a struct")
	}

	q = Query(suite.Map, suite.Map, ptr)
	plan, ok := q.(*QueryPlan)
	if !suite.True(ok) {
		suite.T().FailNow()
	}
	return plan
}

func (suite *QueryPlanTestSuite) TestQueryPlan_EmptyStruct() {
	q := suite.getQueryPlanFor(EmptyStruct{})
	suite.NotEqual(0, len(q.Errors),
		"Query(ref) should generate errors if ref is an empty struct")
}

func (suite *QueryPlanTestSuite) TestQueryPlan_InvalidStruct() {
	q := suite.getQueryPlanFor(InvalidStruct{})
	suite.NotEqual(0, len(q.Errors),
		"Query(ref) should generate errors if ref has no exported fields")
}

func (suite *QueryPlanTestSuite) TestQueryPlan_OnlyTransient() {
	q := suite.getQueryPlanFor(OnlyTransientFields{})
	suite.NotEqual(0, len(q.Errors),
		"Query(ref) should generate errors if ref has only transient fields")
}

func (suite *QueryPlanTestSuite) TestQueryPlan_NonStruct() {
	for _, val := range []interface{}{"Test", 1, 1.0} {
		q := suite.getQueryPlanFor(val)
		suite.NotEqual(0, len(q.Errors),
			"Query(ref) should generate errors if ref is a non-struct type")
	}
}

func (suite *QueryPlanTestSuite) TestQueryPlan_UnmappedStruct() {
	q := suite.getQueryPlanFor(UnmappedStruct{})
	suite.NotEqual(0, len(q.Errors),
		"Query(ref) should generate errors if ref has not yet been mapped")
}

func (suite *QueryPlanTestSuite) TestQueryPlan_NonFieldPtr() {
	q := suite.getQueryPlanFor(ValidStruct{})
	q.Assign(new(int), 20)
	suite.NotEqual(0, len(q.Errors),
		"Assign(fieldPtr, value) should generate errors if fieldPtr is not a pointer to a field in ref")
}

type QueryLanguageTestSuite struct {
	DbTestSuite
	Ref *OverriddenInvoice
}

// TODO: DRY the suite initialization code
func runQueryLanguageSuite(t *testing.T, dialect gorp.Dialect, connection *sql.DB) {
	dbMap := new(gorp.DbMap)
	dbMap.Dialect = dialect
	dbMap.Db = connection
	//dbMap.TraceOn("TEST DB: ", log.New(os.Stdout, "", log.LstdFlags))
	testSuite := new(QueryLanguageTestSuite)
	testSuite.Map = dbMap
	suite.Run(t, testSuite)
}

func TestQueryLanguagePostgres(t *testing.T) {
	dialect := gorp.PostgresDialect{}
	connection, err := sql.Open("postgres", postgresURL)
	if err != nil {
		t.Errorf("Could not connect to postgres: %s", err)
		return
	}
	runQueryLanguageSuite(t, dialect, connection)
}

func TestQueryLanguageMyMySql(t *testing.T) {
	dialect := gorp.MySQLDialect{"InnoDB", "UTF8"}
	connection, err := sql.Open("mymysql", myMySQLURL)
	if err != nil {
		t.Errorf("Could not connect to mysql (using mysql bindings): %s", err)
		return
	}
	runQueryLanguageSuite(t, dialect, connection)
}

func TestQueryLanguageMySql(t *testing.T) {
	dialect := gorp.MySQLDialect{"InnoDB", "UTF8"}
	connection, err := sql.Open("mysql", mySQLURL)
	if err != nil {
		t.Errorf("Could not connect to mysql (using native mysql): %s", err)
		return
	}
	runQueryLanguageSuite(t, dialect, connection)
}

func TestQueryLanguageSqlite(t *testing.T) {
	dialect := gorp.SqliteDialect{}
	connection, err := sql.Open("sqlite3", sqliteURL)
	if err != nil {
		t.Errorf("Could not connect to sqlite: %s", err)
		return
	}
	runQueryLanguageSuite(t, dialect, connection)
}

func (suite *QueryLanguageTestSuite) SetupTest() {
	suite.Ref = new(OverriddenInvoice)
	suite.insertInvoices()
}

func (suite *QueryLanguageTestSuite) TearDownTest() {
	var err error
	switch suite.Map.Dialect.(type) {
	case dialects.SqliteDialect:
		// SQLite3 doesn't have a TRUNCATE TABLE command.
		_, err = Query(suite.Map, suite.Map, suite.Ref).Delete()
	default:
		err = Query(suite.Map, suite.Map, suite.Ref).Truncate()
	}
	if !suite.NoError(err) {
		suite.T().FailNow()
	}
}

// insertInvoices() runs some insert queries to ensure that there is
// data available for the other queries.  If any error occurs, it will
// call suite.T().FailNow() to skip testing the rest of the suite.
func (suite *QueryLanguageTestSuite) insertInvoices() {
	if suite.T().Failed() {
		// Leave the data for inspection
		return
	}
	for _, inv := range testInvoices {
		err := Query(suite.Map, suite.Map, suite.Ref).
			Assign(&suite.Ref.Id, inv.Id).
			Assign(&suite.Ref.Created, inv.Created).
			Assign(&suite.Ref.Updated, inv.Updated).
			Assign(&suite.Ref.Memo, inv.Memo).
			Assign(&suite.Ref.PersonId, inv.PersonId).
			Assign(&suite.Ref.IsPaid, inv.IsPaid).
			Insert()
		if !suite.NoError(err, "Error [%s] while inserting invoice id %s", err, inv.Id) {
			suite.T().FailNow()
		}
	}
}

func (suite *QueryLanguageTestSuite) expectedLength(matcherFunc func(OverriddenInvoice) bool) (expected int) {
	for _, inv := range testInvoices {
		if matcherFunc(inv) {
			expected++
		}
	}
	if expected == 0 {
		panic("Cannot continue tests with no matches for expected length")
	}
	return
}

func (suite *QueryLanguageTestSuite) TestQueryLanguage_TransientField() {
	q := Query(suite.Map, suite.Map, suite.Ref).
		Assign(&suite.Ref.TransientId, "Test")
	suite.NotEqual(0, len(q.(*AssignQueryPlan).Errors),
		"Transient fields should generate errors when used as columns")
}

func (suite *QueryLanguageTestSuite) TestQueryLanguage_UnexportedField() {
	q := Query(suite.Map, suite.Map, suite.Ref).
		Assign(&suite.Ref.unexportedId, "Test")
	suite.NotEqual(0, len(q.(*AssignQueryPlan).Errors),
		"Unexported fields should generate errors when used as columns")
}

func (suite *QueryLanguageTestSuite) TestQueryLanguage_Update() {
	var (
		targetInv   OverriddenInvoice
		isPaidCount int64
	)
	for _, inv := range testInvoices {
		if inv.IsPaid {
			isPaidCount++
		} else if targetInv.Id == "" {
			targetInv = inv
		}
	}

	count, err := Query(suite.Map, suite.Map, suite.Ref).
		Assign(&suite.Ref.IsPaid, true).
		Where().
		Equal(&suite.Ref.Id, targetInv.Id).
		Update()
	if suite.NoError(err) {
		suite.Equal(1, count)
		isPaidCount += count
	}

	invTest, err := Query(suite.Map, suite.Map, suite.Ref).
		Where().
		True(&suite.Ref.IsPaid).
		Select()
	if suite.NoError(err) {
		suite.Equal(isPaidCount, len(invTest))
	}
}

func (suite *QueryLanguageTestSuite) TestQueryLanguage_SelectSimple() {
	invTest, err := Query(suite.Map, suite.Map, suite.Ref).Select()
	if suite.NoError(err) {
		suite.Equal(len(testInvoices), len(invTest))
	}
}

func (suite *QueryLanguageTestSuite) TestQueryLanguage_CountSimple() {
	count, err := Query(suite.Map, suite.Map, suite.Ref).Count()
	if suite.NoError(err) {
		suite.Equal(len(testInvoices), count)
	}
}

func (suite *QueryLanguageTestSuite) TestQueryLanguage_OrderBy_ASC() {
	invTest, err := Query(suite.Map, suite.Map, suite.Ref).OrderBy(&suite.Ref.Updated, "asc").Select()
	if suite.NoError(err) {
		previous := invTest[0].(*OverriddenInvoice).Updated
		for _, result := range invTest {
			inv := result.(*OverriddenInvoice)
			suite.True(previous <= inv.Updated, "OrderBy ASC means %d should be <= %d", previous, inv.Updated)
			previous = inv.Updated
		}
	}
}

func (suite *QueryLanguageTestSuite) TestQueryLanguage_OrderBy_DESC() {
	invTest, err := Query(suite.Map, suite.Map, suite.Ref).OrderBy(&suite.Ref.Updated, "DESC").Select()
	if suite.NoError(err) {
		previous := invTest[0].(*OverriddenInvoice).Updated
		for _, result := range invTest {
			inv := result.(*OverriddenInvoice)
			suite.True(previous >= inv.Updated, "OrderBy DESC means %d should be >= %d", previous, inv.Updated)
			previous = inv.Updated
		}
	}
}

func (suite *QueryLanguageTestSuite) TestQueryLanguage_SelectLike() {
	search := "another"
	expectedCount := suite.expectedLength(func(inv OverriddenInvoice) bool {
		return strings.Contains(inv.Memo, search)
	})

	matchSequence := "%"
	if _, ok := suite.Map.Dialect.(gorp.MySQLDialect); ok {
		// MySQL has non-standard everything
		matchSequence = "\\%"
	}
	count, err := Query(suite.Map, suite.Map, suite.Ref).
		Where().
		Like(&suite.Ref.Memo, matchSequence+search+matchSequence).
		Count()
	if suite.NoError(err) {
		suite.Equal(expectedCount, count)
	}
}

func (suite *QueryLanguageTestSuite) TestQueryLanguage_SelectEqual() {
	match := "test_memo"
	expected := suite.expectedLength(func(inv OverriddenInvoice) bool {
		return inv.Memo == match
	})

	invTest, err := Query(suite.Map, suite.Map, suite.Ref).
		Where().
		Equal(&suite.Ref.Memo, match).
		Select()
	if suite.NoError(err) {
		suite.Equal(expected, len(invTest))
	}
}

func (suite *QueryLanguageTestSuite) TestQueryLanguage_SelectNotEqual() {
	match := "test_memo"
	expected := suite.expectedLength(func(inv OverriddenInvoice) bool {
		return inv.Memo != match
	})

	invTest, err := Query(suite.Map, suite.Map, suite.Ref).
		Where().
		NotEqual(&suite.Ref.Memo, match).
		Select()
	if suite.NoError(err) {
		suite.Equal(expected, len(invTest))
	}
}

func (suite *QueryLanguageTestSuite) TestQueryLanguage_SelectLess() {
	expectedCount := suite.expectedLength(func(inv OverriddenInvoice) bool {
		return inv.Updated < 2
	})

	invTest, err := Query(suite.Map, suite.Map, suite.Ref).
		Where().
		Less(&suite.Ref.Updated, 2).
		Select()
	if suite.NoError(err) {
		suite.Equal(expectedCount, len(invTest))
	}
}

func (suite *QueryLanguageTestSuite) TestQueryLanguage_SelectLessOrEqual() {
	expectedCount := suite.expectedLength(func(inv OverriddenInvoice) bool {
		return inv.Updated <= 2
	})

	invTest, err := Query(suite.Map, suite.Map, suite.Ref).
		Where().
		LessOrEqual(&suite.Ref.Updated, 2).
		Select()
	if suite.NoError(err) {
		suite.Equal(expectedCount, len(invTest))
	}
}

func (suite *QueryLanguageTestSuite) TestQueryLanguage_SelectGreater() {
	expectedCount := suite.expectedLength(func(inv OverriddenInvoice) bool {
		return inv.Updated > 2
	})

	invTest, err := Query(suite.Map, suite.Map, suite.Ref).
		Where().
		Greater(&suite.Ref.Updated, 2).
		Select()
	if suite.NoError(err) {
		suite.Equal(expectedCount, len(invTest))
	}
}

func (suite *QueryLanguageTestSuite) TestQueryLanguage_SelectGreater_OffsetAndLimit() {
	invTest, err := Query(suite.Map, suite.Map, suite.Ref).
		Where().
		Greater(&suite.Ref.Updated, 1).
		Offset(1).
		Limit(1).
		Select()
	if suite.NoError(err) {
		suite.Equal(1, len(invTest))
	}
}

func (suite *QueryLanguageTestSuite) TestQueryLanguage_SelectGreaterOrEqual() {
	expectedCount := suite.expectedLength(func(inv OverriddenInvoice) bool {
		return inv.Updated >= 2
	})

	invTest, err := Query(suite.Map, suite.Map, suite.Ref).
		Where().
		GreaterOrEqual(&suite.Ref.Updated, 2).
		Select()
	if suite.NoError(err) {
		suite.Equal(expectedCount, len(invTest))
	}
}

func (suite *QueryLanguageTestSuite) TestQueryLanguage_SelectTrue() {
	suite.expectedLength(func(inv OverriddenInvoice) bool {
		return inv.IsPaid
	})

	invTest, err := Query(suite.Map, suite.Map, suite.Ref).
		Where().
		True(&suite.Ref.IsPaid).
		Limit(1).
		Select()
	if suite.NoError(err) {
		suite.Equal(1, len(invTest))
	}
}

func (suite *QueryLanguageTestSuite) TestQueryLanguage_SelectFalse() {
	expectedCount := suite.expectedLength(func(inv OverriddenInvoice) bool {
		return !inv.IsPaid
	})

	invTest, err := Query(suite.Map, suite.Map, suite.Ref).
		Where().
		False(&suite.Ref.IsPaid).
		Select()
	if suite.NoError(err) {
		suite.Equal(expectedCount, len(invTest))
	}
}

func (suite *QueryLanguageTestSuite) TestQueryLanguage_SelectFalseAndEqual() {
	expectedCount := suite.expectedLength(func(inv OverriddenInvoice) bool {
		return !inv.IsPaid && inv.Created == 2
	})

	count, err := Query(suite.Map, suite.Map, suite.Ref).
		Where().
		False(&suite.Ref.IsPaid).
		Equal(&suite.Ref.Created, 2).
		Count()
	if suite.NoError(err) {
		suite.Equal(expectedCount, count)
	}
}

func (suite *QueryLanguageTestSuite) TestQueryLanguage_SelectWithFilter() {
	expectedCount := suite.expectedLength(func(inv OverriddenInvoice) bool {
		return inv.Memo == "another_test_memo" || inv.Updated == 3
	})

	invTest, err := Query(suite.Map, suite.Map, suite.Ref).
		Where().
		Filter(filters.Or(filters.Equal(&suite.Ref.Memo, "another_test_memo"), filters.Equal(&suite.Ref.Updated, 3))).
		Select()
	if suite.NoError(err) {
		suite.Equal(expectedCount, len(invTest))
	}
}

func (suite *QueryLanguageTestSuite) TestQueryLanguage_Delete() {
	expectedCount := suite.expectedLength(func(inv OverriddenInvoice) bool {
		return !inv.IsPaid
	})

	count, err := Query(suite.Map, suite.Map, suite.Ref).
		Where().
		False(&suite.Ref.IsPaid).
		Delete()
	if suite.NoError(err) {
		suite.Equal(expectedCount, count)

		count, err = Query(suite.Map, suite.Map, suite.Ref).
			Where().
			False(&suite.Ref.IsPaid).
			Count()
		if suite.NoError(err) {
			suite.Equal(0, count, "No unpaid invoices should exist after deleting all unpaid invoices")
		}
	}
}

func (suite *QueryLanguageTestSuite) TestQueryLanguage_Truncate() {
	// SQLite3 doesn't support TRUNCATE TABLE
	if _, ok := suite.Map.Dialect.(dialects.SqliteDialect); ok {
		return
	}
	expectedCount := 0

	err := Query(suite.Map, suite.Map, suite.Ref).
		Truncate()
	if !suite.NoError(err) {
		suite.T().FailNow()
	}
	count, err := Query(suite.Map, suite.Map, suite.Ref).
		Count()
	if !suite.NoError(err) {
		suite.T().FailNow()
	}
	suite.Equal(expectedCount, count)
}

// func (suite *QueryLanguageTestSuite) TestQueryLanguage_WhereClauseLower() {
// 	inv := OverriddenInvoice{
// 		Id: "79",
// 		Invoice: Invoice{
// 			Created:  2,
// 			Updated:  1,
// 			Memo:     "A Test Memo With Capitals",
// 			PersonId: 1,
// 			IsPaid:   true,
// 		},
// 	}
// 	err := Query(suite.Map, suite.Map, &inv).
// 		Assign(&inv.Id, inv.Id).
// 		Assign(&inv.Created, inv.Created).
// 		Assign(&inv.Updated, inv.Updated).
// 		Assign(&inv.Memo, inv.Memo).
// 		Assign(&inv.PersonId, inv.PersonId).
// 		Assign(&inv.IsPaid, inv.IsPaid).
// 		Insert()
// 	if !suite.NoError(err) {
// 		suite.T().FailNow()
// 	}
// 	invTest, err := Query(suite.Map, suite.Map, &inv).
// 		Where().
// 		Equal(Lower(&inv.Memo), "a test memo with capitals").
// 		Select()
// 	if suite.NoError(err) {
// 		if suite.Equal(len(invTest), 1) {
// 			suite.Equal(invTest[0].(*OverriddenInvoice).Memo, "A Test Memo With Capitals")
// 		}
// 	}
// }

func (suite *QueryLanguageTestSuite) TestQueryLanguage_WhereClauseIn() {
	ids := []interface{}{"1", "2", "3"}
	ref := new(OverriddenInvoice)
	count, err := Query(suite.Map, suite.Map, ref).
		Where().
		In(&ref.Id, ids...).
		Count()
	if suite.NoError(err) {
		suite.Equal(count, len(ids))
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
