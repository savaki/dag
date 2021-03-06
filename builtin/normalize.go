package builtin

import (
	"context"

	"github.com/savaki/dag"
)

// Normalize the field using the provided func
func Normalize(field string, normalizeFunc ValueMapperFunc) dag.TaskFunc {
	return func(ctx context.Context, record *dag.Record) error {
		v, err := record.Get(field)
		if err != nil {
			return nil
		}

		normalized, err := normalizeFunc(v)
		if err != nil {
			return err
		}

		record.Set(field, normalized)

		return nil
	}
}
