package extensions

import (
	"github.com/nelsam/gorp_queries/filters"
)

func ILike(fieldPtr interface{}, pattern string) Filter {
	return &filters.ComparisonFilter{
		left:       fieldPtr,
		comparison: " ilike ",
		right:      pattern,
	}
}
