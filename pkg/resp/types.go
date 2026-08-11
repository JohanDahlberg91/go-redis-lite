// Package resp implements the subset of the Redis Serialization Protocol
// (RESP2) needed to talk to redis-cli and redis-benchmark.
package resp

import "strconv"

// Type is the leading byte that identifies a RESP value.
type Type byte

const (
	TypeSimpleString Type = '+'
	TypeError        Type = '-'
	TypeInteger      Type = ':'
	TypeBulkString   Type = '$'
	TypeArray        Type = '*'
)

var crlf = []byte("\r\n")

// Value is a decoded RESP value. Only the fields relevant to Type carry
// meaning: Str for simple strings, errors and bulk strings, Int for integers
// and Array for arrays. IsNull marks the null bulk string ($-1) and the null
// array (*-1).
type Value struct {
	Type   Type
	Str    string
	Int    int64
	Array  []Value
	IsNull bool
}

// SimpleString returns a status reply such as +OK.
func SimpleString(s string) Value { return Value{Type: TypeSimpleString, Str: s} }

// Error returns an error reply such as -ERR unknown command.
func Error(s string) Value { return Value{Type: TypeError, Str: s} }

// Integer returns an integer reply.
func Integer(n int64) Value { return Value{Type: TypeInteger, Int: n} }

// BulkString returns a bulk string reply.
func BulkString(s string) Value { return Value{Type: TypeBulkString, Str: s} }

// NullBulkString returns the nil reply used for missing keys.
func NullBulkString() Value { return Value{Type: TypeBulkString, IsNull: true} }

// Array returns an array reply.
func Array(values ...Value) Value { return Value{Type: TypeArray, Array: values} }

// Command builds the array of bulk strings used to send a command on the
// wire. It is how mutating commands are encoded into the append-only file.
func Command(args ...string) Value {
	values := make([]Value, len(args))
	for i, arg := range args {
		values[i] = BulkString(arg)
	}
	return Array(values...)
}

// OK is the canonical success reply.
var OK = SimpleString("OK")

// Marshal encodes the value in RESP wire format.
func (v Value) Marshal() []byte {
	return v.appendTo(make([]byte, 0, 32))
}

func (v Value) appendTo(b []byte) []byte {
	switch v.Type {
	case TypeSimpleString:
		b = append(b, byte(TypeSimpleString))
		b = append(b, v.Str...)
		b = append(b, crlf...)
	case TypeError:
		b = append(b, byte(TypeError))
		b = append(b, v.Str...)
		b = append(b, crlf...)
	case TypeInteger:
		b = append(b, byte(TypeInteger))
		b = strconv.AppendInt(b, v.Int, 10)
		b = append(b, crlf...)
	case TypeBulkString:
		if v.IsNull {
			return append(b, "$-1\r\n"...)
		}
		b = append(b, byte(TypeBulkString))
		b = strconv.AppendInt(b, int64(len(v.Str)), 10)
		b = append(b, crlf...)
		b = append(b, v.Str...)
		b = append(b, crlf...)
	case TypeArray:
		if v.IsNull {
			return append(b, "*-1\r\n"...)
		}
		b = append(b, byte(TypeArray))
		b = strconv.AppendInt(b, int64(len(v.Array)), 10)
		b = append(b, crlf...)
		for _, item := range v.Array {
			b = item.appendTo(b)
		}
	default:
		b = append(b, "-ERR unknown RESP type\r\n"...)
	}
	return b
}
