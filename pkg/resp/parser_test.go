package resp

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
)

func TestReadValue(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  Value
	}{
		{"simple string", "+OK\r\n", SimpleString("OK")},
		{"error", "-ERR unknown command\r\n", Error("ERR unknown command")},
		{"integer", ":42\r\n", Integer(42)},
		{"negative integer", ":-2\r\n", Integer(-2)},
		{"bulk string", "$5\r\nhello\r\n", BulkString("hello")},
		{"empty bulk string", "$0\r\n\r\n", BulkString("")},
		{"bulk string with crlf", "$5\r\na\r\nb!\r\n", BulkString("a\r\nb!")},
		{"null bulk string", "$-1\r\n", NullBulkString()},
		{"array", "*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n", Array(BulkString("GET"), BulkString("key"))},
		{"empty array", "*0\r\n", Array()},
		{"nested array", "*1\r\n*1\r\n:7\r\n", Array(Array(Integer(7)))},
		{"null array", "*-1\r\n", Value{Type: TypeArray, IsNull: true}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := NewReader(strings.NewReader(tc.input)).ReadValue()
			if err != nil {
				t.Fatalf("ReadValue() error = %v", err)
			}
			if !valuesEqual(got, tc.want) {
				t.Fatalf("ReadValue() = %#v, want %#v", got, tc.want)
			}
		})
	}
}

func TestReadCommand(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  []string
	}{
		{"resp array", "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n", []string{"SET", "foo", "bar"}},
		{"single command", "*1\r\n$4\r\nPING\r\n", []string{"PING"}},
		{"inline", "PING\r\n", []string{"PING"}},
		{"inline with args", "  SET  foo  bar \r\n", []string{"SET", "foo", "bar"}},
		{"empty inline", "\r\n", nil},
		{"empty array", "*0\r\n", nil},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := NewReader(strings.NewReader(tc.input)).ReadCommand()
			if err != nil {
				t.Fatalf("ReadCommand() error = %v", err)
			}
			if len(got) != len(tc.want) {
				t.Fatalf("ReadCommand() = %q, want %q", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("ReadCommand() = %q, want %q", got, tc.want)
				}
			}
		})
	}
}

func TestReadCommandSequence(t *testing.T) {
	// Pipelined requests must be decoded one after another from one stream.
	input := "*1\r\n$4\r\nPING\r\n*2\r\n$3\r\nGET\r\n$1\r\na\r\n"
	reader := NewReader(strings.NewReader(input))

	first, err := reader.ReadCommand()
	if err != nil || len(first) != 1 || first[0] != "PING" {
		t.Fatalf("first command = %q, err = %v", first, err)
	}
	second, err := reader.ReadCommand()
	if err != nil || len(second) != 2 || second[1] != "a" {
		t.Fatalf("second command = %q, err = %v", second, err)
	}
	if _, err := reader.ReadCommand(); !errors.Is(err, io.EOF) {
		t.Fatalf("third read error = %v, want io.EOF", err)
	}
}

func TestReadValueProtocolErrors(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"unknown type", "!1\r\n"},
		{"bad bulk length", "$abc\r\nhi\r\n"},
		{"oversized bulk length", "$536870913\r\n"},
		{"bulk not crlf terminated", "$2\r\nhiXX"},
		{"bad multibulk length", "*-3\r\n"},
		{"missing cr", "+OK\n"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewReader(strings.NewReader(tc.input)).ReadValue()
			var protoErr *ProtocolError
			if !errors.As(err, &protoErr) {
				t.Fatalf("ReadValue() error = %v, want *ProtocolError", err)
			}
		})
	}
}

func TestReadValueTruncated(t *testing.T) {
	// A value cut short must be distinguishable from a clean end of stream,
	// which is how AOF recovery detects a partially written tail.
	_, err := NewReader(strings.NewReader("$5\r\nhel")).ReadValue()
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("ReadValue() error = %v, want io.ErrUnexpectedEOF", err)
	}
	if _, err := NewReader(strings.NewReader("")).ReadValue(); !errors.Is(err, io.EOF) {
		t.Fatalf("ReadValue() on empty input error = %v, want io.EOF", err)
	}
}

func TestMarshal(t *testing.T) {
	tests := []struct {
		name  string
		value Value
		want  string
	}{
		{"simple string", SimpleString("PONG"), "+PONG\r\n"},
		{"error", Error("ERR nope"), "-ERR nope\r\n"},
		{"integer", Integer(-1), ":-1\r\n"},
		{"bulk string", BulkString("hello"), "$5\r\nhello\r\n"},
		{"null bulk string", NullBulkString(), "$-1\r\n"},
		{"empty array", Array(), "*0\r\n"},
		{"command", Command("SET", "k", "v"), "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := string(tc.value.Marshal()); got != tc.want {
				t.Fatalf("Marshal() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestWriterRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	writer := NewWriter(&buf)

	sent := []Value{SimpleString("OK"), Integer(9), BulkString("value"), Command("DEL", "k")}
	for _, value := range sent {
		if err := writer.Write(value); err != nil {
			t.Fatalf("Write() error = %v", err)
		}
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	reader := NewReader(&buf)
	for _, want := range sent {
		got, err := reader.ReadValue()
		if err != nil {
			t.Fatalf("ReadValue() error = %v", err)
		}
		if !valuesEqual(got, want) {
			t.Fatalf("round trip = %#v, want %#v", got, want)
		}
	}
}

func valuesEqual(a, b Value) bool {
	if a.Type != b.Type || a.Str != b.Str || a.Int != b.Int || a.IsNull != b.IsNull {
		return false
	}
	if len(a.Array) != len(b.Array) {
		return false
	}
	for i := range a.Array {
		if !valuesEqual(a.Array[i], b.Array[i]) {
			return false
		}
	}
	return true
}
