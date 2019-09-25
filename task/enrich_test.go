package task

import (
	"context"
	"testing"

	"github.com/savaki/dag"
	"github.com/tj/assert"
)

// staticKey always returns the same value.  useful for just testing
func staticKey(record *dag.Record) (string, error) {
	return "blah", nil
}

func TestEnrich(t *testing.T) {
	ctx := context.Background()

	t.Run("basic", func(t *testing.T) {
		var (
			want   = MapDataSource{"hello": "world"}
			record = &dag.Record{}
			task   = Enrich(want, staticKey)
		)

		// When
		err := task.Apply(ctx, record)
		assert.Nil(t, err)
		assert.EqualValues(t, want, record.Copy())
	})

	t.Run("only enrich specific fields", func(t *testing.T) {
		var (
			ds = MapDataSource{
				"a": "alpha",
				"b": "bravo",
				"c": "charlie",
			}
			task   = Enrich(ds, staticKey, WithFields("a"))
			record = &dag.Record{}
		)

		// When
		err := task.Apply(ctx, record)
		assert.Nil(t, err)

		want := map[string]interface{}{
			"a": "alpha",
		}
		assert.Equal(t, want, record.Copy())
	})

	t.Run("prefix the fields", func(t *testing.T) {
		var (
			ds = MapDataSource{
				"a": "alpha",
			}
			task   = Enrich(ds, staticKey, WithFields("a"), WithPrefix("prefix_"))
			record = &dag.Record{}
		)

		// When
		err := task.Apply(ctx, record)
		assert.Nil(t, err)

		want := map[string]interface{}{
			"prefix_a": "alpha",
		}
		assert.Equal(t, want, record.Copy())
	})
}
