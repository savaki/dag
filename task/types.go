package task

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/savaki/dag"
)

type DataSource interface {
	Get(ctx context.Context, key string) (*dag.Record, error)
}

type KeyFunc func(record *dag.Record) (string, error)

func NewKeyFunc(field string, fields ...string) KeyFunc {
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

type MapDataSource map[string]interface{}

func (s MapDataSource) Get(ctx context.Context, key string) (*dag.Record, error) {
	record := &dag.Record{}
	for k, v := range s {
		record.Set(k, v)
	}
	return record, nil
}

type NestedMapDataSource map[string]map[string]interface{}

func (s NestedMapDataSource) Get(ctx context.Context, key string) (*dag.Record, error) {
	m, ok := s[key]
	if !ok {
		return nil, fmt.Errorf("key, %v, not found", key)
	}

	record := &dag.Record{}
	for k, v := range m {
		record.Set(k, v)
	}
	return record, nil
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
