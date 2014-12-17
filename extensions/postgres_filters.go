package extensions

import (
	"github.com/nelsam/gorq/filters"
)

func ILike(fieldPtr interface{}, pattern string) filters.Filter {
	return &filters.ComparisonFilter{
		Left:       fieldPtr,
		Comparison: " ilike ",
		Right:      pattern,
	}
}
