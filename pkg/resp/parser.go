package resp

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

const (
	// maxBulkLength matches the Redis proto-max-bulk-len default (512MB).
	maxBulkLength = 512 * 1024 * 1024
	// maxArrayLength caps how many elements a single request may carry.
	maxArrayLength = 1024 * 1024
	readBufferSize = 16 * 1024
)

// ProtocolError describes malformed input. Connections that hit one are
// closed after the error is reported to the client, because the stream can no
// longer be resynchronised.
type ProtocolError struct{ Msg string }

func (e *ProtocolError) Error() string { return "Protocol error: " + e.Msg }

func protoErrf(format string, args ...any) error {
	return &ProtocolError{Msg: fmt.Sprintf(format, args...)}
}

// Reader decodes RESP values from a stream.
type Reader struct {
	r *bufio.Reader
}

// NewReader wraps rd in a buffered RESP reader.
func NewReader(rd io.Reader) *Reader {
	return &Reader{r: bufio.NewReaderSize(rd, readBufferSize)}
}

// Buffered reports how many bytes are already available without another read
// from the underlying stream. Servers use it to decide when to flush replies
// while a client is pipelining.
func (r *Reader) Buffered() int { return r.r.Buffered() }

// ReadValue decodes the next RESP value.
func (r *Reader) ReadValue() (Value, error) {
	typ, err := r.r.ReadByte()
	if err != nil {
		return Value{}, err
	}
	switch Type(typ) {
	case TypeSimpleString:
		line, err := r.readLine()
		if err != nil {
			return Value{}, err
		}
		return SimpleString(string(line)), nil
	case TypeError:
		line, err := r.readLine()
		if err != nil {
			return Value{}, err
		}
		return Error(string(line)), nil
	case TypeInteger:
		n, err := r.readInt()
		if err != nil {
			return Value{}, err
		}
		return Integer(n), nil
	case TypeBulkString:
		return r.readBulkString()
	case TypeArray:
		return r.readArray()
	default:
		return Value{}, protoErrf("expected '$', got '%c'", typ)
	}
}

// ReadCommand reads one client request and returns its arguments. Both the
// RESP array form used by clients and the inline form used by a bare telnet
// session are accepted. An empty inline line yields a nil slice and no error.
func (r *Reader) ReadCommand() ([]string, error) {
	first, err := r.r.Peek(1)
	if err != nil {
		return nil, err
	}
	if Type(first[0]) != TypeArray {
		return r.readInlineCommand()
	}
	if _, err := r.r.ReadByte(); err != nil {
		return nil, err
	}
	n, err := r.readInt()
	if err != nil {
		return nil, err
	}
	if n <= 0 {
		if n < -1 {
			return nil, protoErrf("invalid multibulk length")
		}
		return nil, nil
	}
	if n > maxArrayLength {
		return nil, protoErrf("invalid multibulk length")
	}
	args := make([]string, 0, n)
	for i := int64(0); i < n; i++ {
		v, err := r.ReadValue()
		if err != nil {
			return nil, err
		}
		if v.Type != TypeBulkString || v.IsNull {
			return nil, protoErrf("expected '$', got '%c'", byte(v.Type))
		}
		args = append(args, v.Str)
	}
	return args, nil
}

func (r *Reader) readInlineCommand() ([]string, error) {
	line, err := r.readLine()
	if err != nil {
		return nil, err
	}
	fields := strings.Fields(string(line))
	if len(fields) == 0 {
		return nil, nil
	}
	return fields, nil
}

func (r *Reader) readArray() (Value, error) {
	n, err := r.readInt()
	if err != nil {
		return Value{}, err
	}
	if n == -1 {
		return Value{Type: TypeArray, IsNull: true}, nil
	}
	if n < 0 || n > maxArrayLength {
		return Value{}, protoErrf("invalid multibulk length")
	}
	items := make([]Value, 0, n)
	for i := int64(0); i < n; i++ {
		item, err := r.ReadValue()
		if err != nil {
			return Value{}, err
		}
		items = append(items, item)
	}
	return Array(items...), nil
}

func (r *Reader) readBulkString() (Value, error) {
	n, err := r.readInt()
	if err != nil {
		return Value{}, err
	}
	if n == -1 {
		return NullBulkString(), nil
	}
	if n < 0 || n > maxBulkLength {
		return Value{}, protoErrf("invalid bulk length")
	}
	buf := make([]byte, n+2) // payload plus trailing CRLF
	if _, err := io.ReadFull(r.r, buf); err != nil {
		return Value{}, unexpectedEOF(err)
	}
	if buf[n] != '\r' || buf[n+1] != '\n' {
		return Value{}, protoErrf("bulk string is not CRLF terminated")
	}
	return BulkString(string(buf[:n])), nil
}

func (r *Reader) readInt() (int64, error) {
	line, err := r.readLine()
	if err != nil {
		return 0, err
	}
	n, err := strconv.ParseInt(string(line), 10, 64)
	if err != nil {
		return 0, protoErrf("invalid integer %q", line)
	}
	return n, nil
}

// readLine returns the next CRLF terminated line without its terminator. The
// returned slice is only valid until the next read.
func (r *Reader) readLine() ([]byte, error) {
	line, err := r.r.ReadSlice('\n')
	if err != nil {
		if errors.Is(err, bufio.ErrBufferFull) {
			return nil, protoErrf("too big inline request")
		}
		return nil, unexpectedEOF(err)
	}
	if len(line) < 2 || line[len(line)-2] != '\r' {
		return nil, protoErrf("line is not CRLF terminated")
	}
	return line[:len(line)-2], nil
}

// unexpectedEOF converts an EOF in the middle of a value into
// io.ErrUnexpectedEOF so callers can tell a clean disconnect from a truncated
// one. Replaying a partially written AOF relies on this distinction.
func unexpectedEOF(err error) error {
	if errors.Is(err, io.EOF) {
		return io.ErrUnexpectedEOF
	}
	return err
}

// Writer buffers RESP values on their way to a stream.
type Writer struct {
	w *bufio.Writer
}

// NewWriter wraps w in a buffered RESP writer.
func NewWriter(w io.Writer) *Writer {
	return &Writer{w: bufio.NewWriterSize(w, readBufferSize)}
}

// Write buffers one value. Call Flush to push it to the underlying stream.
func (w *Writer) Write(v Value) error {
	_, err := w.w.Write(v.Marshal())
	return err
}

// Flush writes any buffered bytes to the underlying stream.
func (w *Writer) Flush() error { return w.w.Flush() }
