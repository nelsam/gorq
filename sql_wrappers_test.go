package gorp_queries

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLower(t *testing.T) {
	val := "Test"
	wrapper := Lower(val)
	assert.Equal(t, wrapper.ActualValue(), val)
	assert.Equal(t, wrapper.WrapSql(val), fmt.Sprintf("lower(%s)", val))
}
