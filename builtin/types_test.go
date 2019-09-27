package builtin

import (
	"context"
	"testing"

	"github.com/savaki/dag"
	"github.com/tj/assert"
)

func Test_toString(t *testing.T) {
	tests := []struct {
		name string
		raw  interface{}
		want string
	}{
		{
			name: "float32",
			raw:  float32(1.2345),
			want: "1.2345",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := toString(tt.raw); got != tt.want {
				t.Errorf("toString() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNewKeyFunc(t *testing.T) {
	t.Run("single", func(t *testing.T) {
		fn := BasicKeyFunc("hello")
		record := &dag.Record{}
		record.Set("hello", "world")

		v, err := fn(record)
		assert.Nil(t, err)
		assert.Equal(t, "world", v)
	})

	t.Run("multiple", func(t *testing.T) {
		fn := BasicKeyFunc("a", "b")
		record := &dag.Record{}
		record.Set("a", "alpha")
		record.Set("b", "bravo")

		v, err := fn(record)
		assert.Nil(t, err)
		assert.Equal(t, "alpha:bravo", v)
	})
}

func TestNestedMapDataSource_Get(t *testing.T) {
	ds := NestedMapDataSource{
		"a": map[string]interface{}{
			"hello": "world",
		},
	}

	t.Run("found", func(t *testing.T) {
		record, err := ds.Get(context.Background(), "a")
		assert.Nil(t, err)
		assert.NotNil(t, record)
	})

	t.Run("not found", func(t *testing.T) {
		_, err := ds.Get(context.Background(), "blah")
		assert.NotNil(t, err)
	})
}

func Test_toString1(t *testing.T) {
	tests := []struct {
		name string
		raw  interface{}
		want string
	}{
		{
			name: "string",
			raw:  "hello",
			want: "hello",
		},
		{
			name: "string",
			raw:  123,
			want: "123",
		},
		{
			name: "string",
			raw:  int64(123),
			want: "123",
		},
		{
			name: "string",
			raw:  1.23,
			want: "1.23",
		},
		{
			name: "string",
			raw:  int32(123),
			want: "123",
		},
		{
			name: "string",
			raw:  uint(123),
			want: "123",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := toString(tt.raw); got != tt.want {
				t.Errorf("toString() = %v, want %v", got, tt.want)
			}
		})
	}
}
