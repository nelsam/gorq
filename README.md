gorp_queries
============

gorp_queries extends [gorp](github.com/coopernurse/gorp) with a query
DSL intended to catch SQL mistakes at compile time instead of runtime.
This is accomplished using reference structs and a relatively
complicated interface structure.

To get started, use go get and import the package:

```bash
go get github.com/nelsam/gorp_queries
```

```go
import "github.com/nelsam/gorp_queries"
```

Then, set up your DB map using `gorp_queries.DbMap` and use gorp as
normal.  `gorp_queries.DbMap` includes all of the functionality of
`gorp.DbMap`, with a few extensions.  See
[the documentation for gorp_queries](godoc.org/github.com/nelsam/gorp_queries)
for details on the extensions.  See
[the documentation for gorp](godoc.org/github.com/coopernurse/gorp)
for details on the functionality provided by gorp.
