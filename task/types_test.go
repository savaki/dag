package task

import "testing"

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
