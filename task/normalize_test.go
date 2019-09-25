package task

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/tj/assert"

	"github.com/savaki/dag"
)

func TestNormalize(t *testing.T) {
	ctx := context.Background()
	toUpper := func(value interface{}) (interface{}, error) {
		if value == nil {
			return nil, nil
		}

		return strings.ToUpper(value.(string)), nil
	}

	t.Run("ok", func(t *testing.T) {
		record := &dag.Record{}
		record.Set("blah", "blah")

		task := Normalize("blah", toUpper)
		err := task.Apply(ctx, record)
		assert.Nil(t, err)
		assert.Equal(t, map[string]interface{}{"blah": "BLAH"}, record.Copy())
	})

	t.Run("field not found", func(t *testing.T) {
		record := &dag.Record{}
		record.Set("hello", "world")
		want := record.Copy()

		task := Normalize("blah", toUpper)
		err := task.Apply(ctx, record)
		assert.Nil(t, err)
		assert.Equal(t, want, record.Copy())
	})

	t.Run("normalizer failed", func(t *testing.T) {
		want := io.ErrUnexpectedEOF
		boom := func(interface{}) (interface{}, error) { return nil, want }
		record := &dag.Record{}
		record.Set("hello", "world")

		task := Normalize("hello", boom)
		err := task.Apply(ctx, record)
		assert.Equal(t, want, err)
	})
}
