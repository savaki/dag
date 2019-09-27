package builtin

import (
	"net/http"

	"github.com/savaki/dag"
)

func containsString(slice []string, want string) bool {
	for _, item := range slice {
		if item == want {
			return true
		}
	}
	return false
}

type transportFunc func(req *http.Request) (*http.Response, error)

func (fn transportFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return fn(req)
}

func withName(name string, target dag.TaskFunc) dag.NamedTask {
	return dag.WithName(name, target)
}
