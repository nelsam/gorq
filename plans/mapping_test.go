package plans

import (
	"reflect"
	"testing"

	"github.com/go-gorp/gorp"
	"github.com/stretchr/testify/assert"
)

type ValidStructWithOnlyAnon struct {
	ValidStruct
}

type InvalidStructWithOnlyAnon struct {
	InvalidStruct
}

func TestMappingColumnWithAnonStruct(t *testing.T) {
	p := new(QueryPlan)
	p.dbMap = &gorp.DbMap{
		Dialect: gorp.PostgresDialect{},
	}
	p.dbMap.AddTable(ValidStructWithOnlyAnon{})
	p.dbMap.AddTable(InvalidStructWithOnlyAnon{})

	_, err := p.mapTable(reflect.ValueOf(new(ValidStructWithOnlyAnon)))
	assert.NoError(t, err)

	_, err = p.mapTable(reflect.ValueOf(new(InvalidStructWithOnlyAnon)))
	assert.Error(t, err)
}
