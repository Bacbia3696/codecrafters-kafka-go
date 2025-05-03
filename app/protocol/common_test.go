package protocol

import (
	"reflect"
	"testing"
)

func TestEncodeVarint(t *testing.T) {
	tests := []struct {
		name  string
		input int32
		want  []byte
	}{
		{
			name:  "zero",
			input: 0,
			want:  []byte{0x00},
		},
		{
			name:  "one",
			input: 1,
			want:  []byte{0x01},
		},
		{
			name:  "one byte max",
			input: 127,
			want:  []byte{0x7f},
		},
		{
			name:  "two bytes min",
			input: 128,
			want:  []byte{0x80, 0x01},
		},
		{
			name:  "two bytes example (300)",
			input: 300,
			want:  []byte{0xac, 0x02},
		},
		{
			name:  "two bytes max",
			input: 16383,
			want:  []byte{0xff, 0x7f},
		},
		{
			name:  "three bytes min",
			input: 16384,
			want:  []byte{0x80, 0x80, 0x01},
		},
		{
			name:  "three bytes example (2097151)",
			input: 2097151,
			want:  []byte{0xff, 0xff, 0x7f},
		},
		// Add more cases if needed, e.g., max int32 if applicable
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := encodeVarint(tt.input)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("encodeVarint(%d) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}
