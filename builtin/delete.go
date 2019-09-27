package builtin

import (
	"context"

	"github.com/savaki/dag"
)

// Delete removes the specified fields from the Record if they exist
func Delete(label string, fields ...string) dag.Task {
	return withName(label, func(ctx context.Context, record *dag.Record) error {
		record.Delete(fields...)
		return nil
	})
}
