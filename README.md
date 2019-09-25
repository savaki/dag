[![GoDoc](https://godoc.org/github.com/savaki/dag?status.svg)](https://godoc.org/github.com/savaki/dag)

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
}
```