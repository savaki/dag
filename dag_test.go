package dag

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tj/assert"
)

func TestParallel(t *testing.T) {
	ctx := context.Background()
	record := &Record{}

	t.Run("default", func(t *testing.T) {
		var counter int64
		task := Parallel(counterTask(&counter))
		err := task.Apply(ctx, record)
		assert.Nil(t, err)
		assert.Equal(t, 1, int(counter))
	})

	t.Run("one middleware", func(t *testing.T) {
		var counter int64
		var order []string
		task := Parallel(counterTask(&counter))
		task.Use(middleware(&order, "a"))
		err := task.Apply(ctx, record)
		assert.Nil(t, err)
		assert.Equal(t, 1, int(counter))
		assert.Equal(t, []string{"a"}, order)
	})

	t.Run("middleware ordering", func(t *testing.T) {
		var counter int64
		var order []string
		task := Parallel(counterTask(&counter))
		task.Use(
			middleware(&order, "a"),
			middleware(&order, "b"),
		)
		err := task.Apply(ctx, record)
		assert.Nil(t, err)
		assert.Equal(t, 1, int(counter))
		assert.Equal(t, []string{"b", "a"}, order)
	})
}

func TestSerial(t *testing.T) {
	ctx := context.Background()
	record := &Record{}

	t.Run("default", func(t *testing.T) {
		var counter int64
		task := Serial(counterTask(&counter))
		err := task.Apply(ctx, record)
		assert.Nil(t, err)
		assert.Equal(t, 1, int(counter))
	})

	t.Run("one middleware", func(t *testing.T) {
		var counter int64
		var order []string
		task := Serial(counterTask(&counter))
		task.Use(middleware(&order, "a"))
		err := task.Apply(ctx, record)
		assert.Nil(t, err)
		assert.Equal(t, 1, int(counter))
		assert.Equal(t, []string{"a"}, order)
	})

	t.Run("middleware ordering", func(t *testing.T) {
		var counter int64
		var order []string
		task := Serial(counterTask(&counter))
		task.Use(
			middleware(&order, "a"),
			middleware(&order, "b"),
		)
		err := task.Apply(ctx, record)
		assert.Nil(t, err)
		assert.Equal(t, 1, int(counter))
		assert.Equal(t, []string{"b", "a"}, order)
	})
}

func counterTask(v *int64) TaskFunc {
	return func(ctx context.Context, record *Record) error {
		atomic.AddInt64(v, 1)
		return nil
	}
}

func middleware(col *[]string, v string) func(Task) Task {
	return func(target Task) Task {
		return TaskFunc(func(ctx context.Context, record *Record) error {
			*col = append(*col, v)
			return target.Apply(ctx, record)
		})
	}
}

func TestNewRecord(t *testing.T) {
	want := Meta{
		ID:         "id",
		StartedAt:  time.Now(),
		Properties: map[string]string{"hello": "world"},
	}
	record := NewRecord(want)
	assert.Equal(t, want, record.Meta())
}

func TestRecord(t *testing.T) {
	t.Run("string", func(t *testing.T) {
		record := &Record{}
		record.Set("hello", "world")
		got, err := record.String("hello")
		assert.Nil(t, err)
		assert.Equal(t, "world", got)
	})

	t.Run("int", func(t *testing.T) {
		want := 123
		record := &Record{}
		record.Set("hello", want)
		got, err := record.Int("hello")
		assert.Nil(t, err)
		assert.Equal(t, want, got)
	})

	t.Run("int64", func(t *testing.T) {
		want := int64(123)
		record := &Record{}
		record.Set("hello", want)
		got, err := record.Int64("hello")
		assert.Nil(t, err)
		assert.Equal(t, want, got)
	})

	t.Run("float64", func(t *testing.T) {
		want := 1.23
		record := &Record{}
		record.Set("hello", want)
		got, err := record.Float64("hello")
		assert.Nil(t, err)
		assert.Equal(t, want, got)
	})

	t.Run("not found", func(t *testing.T) {
		var (
			record = &Record{}
			err    error
		)

		_, err = record.String("hello")
		assert.True(t, IsFieldNotFoundError(err))

		_, err = record.Int("hello")
		assert.True(t, IsFieldNotFoundError(err))

		_, err = record.Int64("hello")
		assert.True(t, IsFieldNotFoundError(err))

		_, err = record.Float64("hello")
		assert.True(t, IsFieldNotFoundError(err))
	})

	t.Run("wrong type", func(t *testing.T) {
		var (
			record = &Record{}
			err    error
		)

		record.Set("hello", time.Now())

		_, err = record.String("hello")
		assert.True(t, IsWrongTypeError(err))

		_, err = record.Int("hello")
		assert.True(t, IsWrongTypeError(err))

		_, err = record.Int64("hello")
		assert.True(t, IsWrongTypeError(err))

		_, err = record.Float64("hello")
		assert.True(t, IsWrongTypeError(err))
	})
}
