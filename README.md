[![GoDoc](https://godoc.org/github.com/savaki/dag?status.svg)](https://godoc.org/github.com/savaki/dag)
[![Go Report Card](https://goreportcard.com/badge/github.com/savaki/dag)](https://goreportcard.com/report/github.com/savaki/dag)
[![Coverage Status](https://coveralls.io/repos/github/savaki/dag/badge.svg)](https://coveralls.io/github/savaki/dag)

dag
-----------------------
`dag` - library to manage processing via directed acyclic graphs

#### Example

```go
func main() {
  var dataSource task.DataSource
  stream := dag.Serial(
    task.Enrich(dataSource),
    task.Canonicalize(toCanonical),
    task.Normalize("foo", normalizer)
  )
  stream.Use(middleware)

  ctx := context.Background()
  record := &dag.Record{}
  err := task.Apply(ctx, record)
}
```