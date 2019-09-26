package dag

import (
	"context"
	"fmt"
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
		task = Wrap(task, middleware(&order, "a"))
		err := task.Apply(ctx, record)
		assert.Nil(t, err)
		assert.Equal(t, 1, int(counter))
		assert.Equal(t, []string{"a", "a"}, order)
	})

	t.Run("middleware ordering", func(t *testing.T) {
		var counter int64
		var order []string
		task := Parallel(counterTask(&counter))
		task = Wrap(task,
			middleware(&order, "a"),
			middleware(&order, "b"),
		)
		err := task.Apply(ctx, record)
		assert.Nil(t, err)
		assert.Equal(t, 1, int(counter))
		assert.Equal(t, []string{"b", "a", "b", "a"}, order)
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
		task = Wrap(task, middleware(&order, "a"))
		err := task.Apply(ctx, record)
		assert.Nil(t, err)
		assert.Equal(t, 1, int(counter))
		assert.Equal(t, []string{"a", "a"}, order)
	})

	t.Run("middleware ordering", func(t *testing.T) {
		var counter int64
		var order []string
		task := Serial(counterTask(&counter))
		task = Wrap(task,
			middleware(&order, "a"),
			middleware(&order, "b"),
		)
		err := task.Apply(ctx, record)
		assert.Nil(t, err)
		assert.Equal(t, 1, int(counter))
		assert.Equal(t, []string{"b", "a", "b", "a"}, order)
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
	t.Run("get", func(t *testing.T) {
		record := &Record{}
		record.Set("hello", "world")
		got, err := record.Get("hello")
		assert.Nil(t, err)
		assert.Equal(t, "world", got)
	})

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

		_, err = record.Get("hello")
		assert.True(t, IsFieldNotFoundError(err))

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

func TestRecordDelete(t *testing.T) {
	record := &Record{}
	record.Set("a", "alpha")
	record.Set("b", "bravo")
	record.Delete("a")
	want := map[string]interface{}{"b": "bravo"}
	assert.Equal(t, want, record.Copy())
}

func nopTask() TaskFunc {
	return func(ctx context.Context, record *Record) error {
		return nil
	}
}

func Test_wrap(t *testing.T) {
	var stack []string
	task := Serial(
		WithName("a", nopTask()),
		Serial(Serial(WithName("b", nopTask()))),
		Parallel(WithName("c", nopTask())),
	)
	task = Wrap(task, func(t Task) Task {
		return TaskFunc(func(ctx context.Context, record *Record) error {
			name := Name(t)
			for n := Depth(ctx); n > 1; n-- {
				fmt.Print("  ")
			}
			fmt.Println(name)

			stack = append(stack, name)
			return t.Apply(ctx, record)
		})
	})

	ctx := context.Background()
	record := &Record{}
	err := task.Apply(ctx, record)
	assert.Nil(t, err)
	assert.Equal(t, []string{"Serial", "a", "Serial", "Serial", "b", "Parallel", "c"}, stack)
}

func TestPush(t *testing.T) {
	ctx := context.Background()

	assert.Equal(t, 0, Depth(ctx))

	child := Push(ctx)
	assert.Equal(t, 1, Depth(child))
}

func BenchmarkSerial(t *testing.B) {
	var (
		ctx     = context.Background()
		record  = &Record{}
		counter int64
		task    = Serial(counterTask(&counter))
	)

	for i := 0; i < t.N; i++ {
		err := task.Apply(ctx, record)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}
	}
}
