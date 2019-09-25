package task

import (
	"context"
	"testing"

	"github.com/savaki/dag"
	"github.com/tj/assert"
)

func TestDelete(t *testing.T) {
	ctx := context.Background()
	task := Delete("a", "b")

	record := &dag.Record{}
	record.Set("a", "apple")
	err := task.Apply(ctx, record)
	assert.Nil(t, err)
	assert.Empty(t, record.Copy())
}
