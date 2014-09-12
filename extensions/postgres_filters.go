package extensions

import (
	"github.com/nelsam/gorp_queries/filters"
)

func ILike(fieldPtr interface{}, pattern string) filters.Filter {
	return &filters.ComparisonFilter{
		Left:       fieldPtr,
		Comparison: " ilike ",
		Right:      pattern,
	}
}
