package task

import (
	"context"
	"strings"
	"testing"

	"github.com/savaki/dag"
	"github.com/tj/assert"
)

func TestCanonicalize(t *testing.T) {
	mapField := func(field string) (string, error) {
		return strings.ToUpper(field), nil
	}

	record := &dag.Record{}
	record.Set("hello", "world")

	ctx := context.Background()
	task := Canonicalize(mapField)

	// When
	err := task.Apply(ctx, record)
	assert.Nil(t, err)
	assert.Equal(t, map[string]interface{}{"HELLO": "world"}, record.Copy())
}
