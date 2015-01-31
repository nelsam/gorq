package dialects

import (
	"fmt"

	"github.com/outdoorsy/gorp"
)

type SqliteDialect struct {
	gorp.SqliteDialect
}

func (dialect SqliteDialect) Limit(bindVar interface{}) string {
	return fmt.Sprintf("limit %s", bindVar)
}
