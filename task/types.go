package task

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/savaki/dag"
)

// DataSource provides an abstraction for a remote data source for enrichment
type DataSource interface {
	// Get the record from the remote data source
	Get(ctx context.Context, key string) (map[string]interface{}, error)
}

// KeyFunc constructs a lookup key given a record.  Returns nil if the fields are not found
type KeyFunc func(record *dag.Record) (string, error)

// BasicKeyFunc returns a key func that simply concatenates the requested fields together
func BasicKeyFunc(field string, fields ...string) KeyFunc {
	if len(fields) == 0 {
		return func(record *dag.Record) (string, error) {
			v, err := record.String(field)
			if err != nil {
				return "", err
			}
			return v, nil
		}
	}

	return func(record *dag.Record) (string, error) {
		parts := make([]string, 0, len(fields)+1)

		v, err := record.String(field)
		if err != nil {
			return "", err
		}
		parts = append(parts, v)

		for _, field := range fields {
			v, err := record.String(field)
			if err != nil {
				return "", err
			}
			parts = append(parts, v)
		}

		return strings.Join(parts, ":"), nil
	}
}

// MapDataSource provides a test interface for data source
type MapDataSource map[string]interface{}

// Get implements DataSource
func (s MapDataSource) Get(ctx context.Context, key string) (map[string]interface{}, error) {
	return s, nil
}

// NestedMapDataSource provides another data source for testing
type NestedMapDataSource map[string]map[string]interface{}

// Get implements DataSource
func (s NestedMapDataSource) Get(ctx context.Context, key string) (map[string]interface{}, error) {
	m, ok := s[key]
	if !ok {
		return nil, fmt.Errorf("key, %v, not found", key)
	}

	return m, nil
}

func toString(raw interface{}) string {
	switch v := raw.(type) {
	case string:
		return v
	case int:
		return strconv.Itoa(v)
	case int32:
		return strconv.FormatInt(int64(v), 10)
	case int64:
		return strconv.FormatInt(v, 10)
	case float32:
		return strconv.FormatFloat(float64(v), 'f', -1, 32)
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	default:
		return fmt.Sprintf("%v", raw)
	}
}
