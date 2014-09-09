package foreign_keys

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/suite"
)

type TestParent struct {
	Child Keyer
}

type TestChild struct {
	Id int64
}

func (c *TestChild) Key() interface{} {
	return c.Id
}

func (c *TestChild) SetKey(value interface{}) error {
	switch t := value.(type) {
	case int64:
		c.Id = t
	case int:
		c.Id = int64(t)
	case int32:
		c.Id = int64(t)
	default:
		return errors.New("SetKey for child must be an int type")
	}
	return nil
}

type ForeignKeyTestSuite struct {
	suite.Suite
}

func TestForeignKeys(t *testing.T) {
	suite.Run(t, new(ForeignKeyTestSuite))
}

func (suite *ForeignKeyTestSuite) SetupTest() {
	relationships = make([]*relationship, 0, 10)
}

func (suite *ForeignKeyTestSuite) TestRegister_Name() {
	suite.NoError(Register(TestParent{}, "Child", new(TestChild)))
}

func (suite *ForeignKeyTestSuite) TestRegister_Ptr() {
	ref := new(TestParent)
	suite.NoError(Register(ref, &ref.Child, new(TestChild)))
}

func (suite *ForeignKeyTestSuite) TestLookup_Name() {
	// Don't care about registration errors
	Register(TestParent{}, "Child", new(TestChild))
	newChild, err := Lookup(TestParent{}, "Child")
	if suite.NoError(err) {
		suite.IsType(new(TestChild), newChild)
	}
}

func (suite *ForeignKeyTestSuite) TestLookup_Ptr() {
	// Don't care about registration errors
	ref := new(TestParent)
	Register(ref, &ref.Child, new(TestChild))
	newChild, err := Lookup(ref, &ref.Child)
	if suite.NoError(err) {
		suite.IsType(new(TestChild), newChild)
	}
}
