package builtin

import (
	"context"

	"github.com/savaki/dag"
)

// Canonicalize the field names
func Canonicalize(label string, mapField FieldMapperFunc) dag.Task {
	return withName(label, func(ctx context.Context, record *dag.Record) error {
		fields := record.Fields()
		for _, field := range fields {
			mapped, err := mapField(field)
			if err != nil {
				return err
			}
			if field == mapped {
				continue // no change
			}

			if value, err := record.Get(field); err == nil {
				record.Delete(field)
				record.Set(mapped, value)
			}
		}
		return nil
	})
}
