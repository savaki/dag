package task

import (
	"context"

	"github.com/savaki/dag"
)

// Delete removes the specified fields from the Record if they exist
func Delete(fields ...string) dag.TaskFunc {
	return func(ctx context.Context, record *dag.Record) error {
		record.Delete(fields...)
		return nil
	}
}
