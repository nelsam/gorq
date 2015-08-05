package gorq

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLower(t *testing.T) {
	val := "Test"
	wrapper := Lower(val)
	assert.Equal(t, val, wrapper.ActualValue())
	assert.Equal(t, fmt.Sprintf("lower(%s)", val), wrapper.WrapSql(val))
}
