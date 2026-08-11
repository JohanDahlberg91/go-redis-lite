package server

import (
	"context"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JohanDahlberg91/go-redis-lite/pkg/resp"
	"github.com/JohanDahlberg91/go-redis-lite/pkg/store"
)

// recorder captures propagated commands in place of a real AOF.
type recorder struct {
	mu       sync.Mutex
	commands [][]string
}

func (r *recorder) Append(args ...string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.commands = append(r.commands, append([]string(nil), args...))
	return nil
}

func (r *recorder) all() [][]string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.commands
}

func TestExecuteBasicCommands(t *testing.T) {
	e := NewExecutor(store.New(), nil, nil)

	tests := []struct {
		name string
		args []string
		want resp.Value
	}{
		{"ping", []string{"PING"}, resp.SimpleString("PONG")},
		{"ping with message", []string{"PING", "hi"}, resp.BulkString("hi")},
		{"echo", []string{"ECHO", "hello"}, resp.BulkString("hello")},
		{"lowercase name", []string{"ping"}, resp.SimpleString("PONG")},
		{"set", []string{"SET", "key", "value"}, resp.OK},
		{"get", []string{"GET", "key"}, resp.BulkString("value")},
		{"get missing", []string{"GET", "nope"}, resp.NullBulkString()},
		{"exists", []string{"EXISTS", "key", "nope"}, resp.Integer(1)},
		{"del", []string{"DEL", "key"}, resp.Integer(1)},
		{"del missing", []string{"DEL", "key"}, resp.Integer(0)},
		{"unknown command", []string{"NOPE"}, resp.Error("ERR unknown command 'NOPE'")},
		{"wrong arity", []string{"GET"}, resp.Error("ERR wrong number of arguments for 'get' command")},
		{"ttl on missing key", []string{"TTL", "nope"}, resp.Integer(-2)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := e.Execute(tc.args)
			if string(got.Marshal()) != string(tc.want.Marshal()) {
				t.Fatalf("Execute(%q) = %s, want %s", tc.args,
					strings.TrimSpace(string(got.Marshal())),
					strings.TrimSpace(string(tc.want.Marshal())))
			}
		})
	}
}

func TestExecuteSetOptions(t *testing.T) {
	st := store.New()
	e := NewExecutor(st, nil, nil)

	if got := e.Execute([]string{"SET", "key", "a", "NX"}); got.Type != resp.TypeSimpleString {
		t.Fatalf("SET NX on a missing key = %#v", got)
	}
	if got := e.Execute([]string{"SET", "key", "b", "NX"}); !got.IsNull {
		t.Fatalf("SET NX on an existing key = %#v, want nil reply", got)
	}
	if got := e.Execute([]string{"SET", "key", "c", "EX", "100"}); got.Type != resp.TypeSimpleString {
		t.Fatalf("SET EX = %#v", got)
	}
	if got := e.Execute([]string{"TTL", "key"}); got.Int < 95 || got.Int > 100 {
		t.Fatalf("TTL = %d, want about 100", got.Int)
	}
	if got := e.Execute([]string{"SET", "key", "d"}); got.Type != resp.TypeSimpleString {
		t.Fatalf("SET = %#v", got)
	}
	if got := e.Execute([]string{"TTL", "key"}); got.Int != -1 {
		t.Fatalf("TTL after a plain SET = %d, want -1", got.Int)
	}

	errorCases := [][]string{
		{"SET", "key", "v", "EX", "abc"},
		{"SET", "key", "v", "EX", "0"},
		{"SET", "key", "v", "NX", "XX"},
		{"SET", "key", "v", "BOGUS"},
		{"SET", "key", "v", "EX"},
	}
	for _, args := range errorCases {
		if got := e.Execute(args); got.Type != resp.TypeError {
			t.Fatalf("Execute(%q) = %#v, want an error", args, got)
		}
	}
}

func TestExecuteExpiryCommands(t *testing.T) {
	e := NewExecutor(store.New(), nil, nil)
	e.Execute([]string{"SET", "key", "value"})

	if got := e.Execute([]string{"EXPIRE", "key", "50"}); got.Int != 1 {
		t.Fatalf("EXPIRE = %d, want 1", got.Int)
	}
	if got := e.Execute([]string{"TTL", "key"}); got.Int != 50 {
		t.Fatalf("TTL = %d, want 50", got.Int)
	}
	if got := e.Execute([]string{"PTTL", "key"}); got.Int < 49000 || got.Int > 50000 {
		t.Fatalf("PTTL = %d, want about 50000", got.Int)
	}
	if got := e.Execute([]string{"PERSIST", "key"}); got.Int != 1 {
		t.Fatalf("PERSIST = %d, want 1", got.Int)
	}
	if got := e.Execute([]string{"TTL", "key"}); got.Int != -1 {
		t.Fatalf("TTL after PERSIST = %d, want -1", got.Int)
	}
	if got := e.Execute([]string{"EXPIRE", "missing", "10"}); got.Int != 0 {
		t.Fatalf("EXPIRE on a missing key = %d, want 0", got.Int)
	}

	// A non-positive TTL deletes the key outright.
	if got := e.Execute([]string{"EXPIRE", "key", "-1"}); got.Int != 1 {
		t.Fatalf("EXPIRE with a negative TTL = %d, want 1", got.Int)
	}
	if got := e.Execute([]string{"EXISTS", "key"}); got.Int != 0 {
		t.Fatalf("EXISTS after a negative EXPIRE = %d, want 0", got.Int)
	}
}

func TestExecuteCounters(t *testing.T) {
	e := NewExecutor(store.New(), nil, nil)

	if got := e.Execute([]string{"INCR", "n"}); got.Int != 1 {
		t.Fatalf("INCR = %d, want 1", got.Int)
	}
	if got := e.Execute([]string{"INCRBY", "n", "10"}); got.Int != 11 {
		t.Fatalf("INCRBY = %d, want 11", got.Int)
	}
	if got := e.Execute([]string{"DECRBY", "n", "5"}); got.Int != 6 {
		t.Fatalf("DECRBY = %d, want 6", got.Int)
	}
	if got := e.Execute([]string{"DECR", "n"}); got.Int != 5 {
		t.Fatalf("DECR = %d, want 5", got.Int)
	}
	if got := e.Execute([]string{"GET", "n"}); got.Str != "5" {
		t.Fatalf("GET = %q, want \"5\"", got.Str)
	}

	e.Execute([]string{"SET", "text", "abc"})
	if got := e.Execute([]string{"INCR", "text"}); got.Type != resp.TypeError {
		t.Fatalf("INCR on a non-numeric value = %#v, want an error", got)
	}
}

func TestPropagationIsReplayable(t *testing.T) {
	rec := &recorder{}
	original := store.New()
	e := NewExecutor(original, rec, nil)

	e.Execute([]string{"SET", "plain", "value"})
	e.Execute([]string{"SET", "temp", "value", "EX", "3600"})
	e.Execute([]string{"SET", "gone", "value"})
	e.Execute([]string{"DEL", "gone"})
	e.Execute([]string{"INCR", "counter"})
	e.Execute([]string{"INCRBY", "counter", "41"})
	e.Execute([]string{"SET", "expiring", "value"})
	e.Execute([]string{"EXPIRE", "expiring", "600"})
	e.Execute([]string{"GET", "plain"}) // reads must not be persisted

	// Relative expirations must be persisted as absolute timestamps so a
	// replay does not restart the clock.
	for _, args := range rec.all() {
		for _, arg := range args {
			if arg == "EX" || arg == "EXPIRE" {
				t.Fatalf("persisted a relative expiration: %q", args)
			}
		}
	}

	replayed := store.New()
	replayer := NewExecutor(replayed, nil, nil)
	for _, args := range rec.all() {
		if reply := replayer.Execute(args); reply.Type == resp.TypeError {
			t.Fatalf("replaying %q failed: %s", args, reply.Str)
		}
	}

	if value, ok := replayed.Get("plain"); !ok || value != "value" {
		t.Fatalf("replayed plain = %v, %v", value, ok)
	}
	if value, ok := replayed.Get("counter"); !ok || value != "42" {
		t.Fatalf("replayed counter = %v, %v", value, ok)
	}
	if _, ok := replayed.Get("gone"); ok {
		t.Fatal("a deleted key came back after replay")
	}
	if remaining, _, hasTTL := replayed.TTL("temp"); !hasTTL || remaining > time.Hour {
		t.Fatalf("replayed temp TTL = %v, want just under an hour", remaining)
	}
	if remaining, _, hasTTL := replayed.TTL("expiring"); !hasTTL || remaining > 10*time.Minute {
		t.Fatalf("replayed expiring TTL = %v, want just under ten minutes", remaining)
	}
}

func TestServerOverTCP(t *testing.T) {
	client, _, cleanup := startServer(t, nil)
	defer cleanup()

	if got := client.do(t, "PING"); got.Str != "PONG" {
		t.Fatalf("PING = %q, want PONG", got.Str)
	}
	if got := client.do(t, "SET", "greeting", "hello world"); got.Str != "OK" {
		t.Fatalf("SET = %q, want OK", got.Str)
	}
	if got := client.do(t, "GET", "greeting"); got.Str != "hello world" {
		t.Fatalf("GET = %q, want \"hello world\"", got.Str)
	}
	if got := client.do(t, "GET", "missing"); !got.IsNull {
		t.Fatalf("GET on a missing key = %#v, want a nil reply", got)
	}
	if got := client.do(t, "DBSIZE"); got.Int != 1 {
		t.Fatalf("DBSIZE = %d, want 1", got.Int)
	}
	if got := client.do(t, "KEYS", "*"); len(got.Array) != 1 || got.Array[0].Str != "greeting" {
		t.Fatalf("KEYS = %#v", got.Array)
	}
	if got := client.do(t, "FLUSHALL"); got.Str != "OK" {
		t.Fatalf("FLUSHALL = %q, want OK", got.Str)
	}
	if got := client.do(t, "DBSIZE"); got.Int != 0 {
		t.Fatalf("DBSIZE after FLUSHALL = %d, want 0", got.Int)
	}
}

func TestServerExpiresKeysOverTCP(t *testing.T) {
	client, _, cleanup := startServer(t, nil)
	defer cleanup()

	client.do(t, "SET", "temp", "value", "PX", "30")
	if got := client.do(t, "GET", "temp"); got.Str != "value" {
		t.Fatalf("GET before expiry = %#v", got)
	}
	time.Sleep(60 * time.Millisecond)
	if got := client.do(t, "GET", "temp"); !got.IsNull {
		t.Fatalf("GET after expiry = %#v, want a nil reply", got)
	}
}

func TestServerPipelining(t *testing.T) {
	client, _, cleanup := startServer(t, nil)
	defer cleanup()

	// Send three commands before reading any reply.
	for _, args := range [][]string{{"SET", "a", "1"}, {"INCR", "a"}, {"GET", "a"}} {
		if err := client.writer.Write(resp.Command(args...)); err != nil {
			t.Fatalf("Write() error = %v", err)
		}
	}
	if err := client.writer.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	for i, want := range []string{"OK", "2", "2"} {
		got, err := client.reader.ReadValue()
		if err != nil {
			t.Fatalf("reply %d: %v", i, err)
		}
		if text := replyText(got); text != want {
			t.Fatalf("reply %d = %q, want %q", i, text, want)
		}
	}
}

func TestServerRejectsMalformedInput(t *testing.T) {
	client, _, cleanup := startServer(t, nil)
	defer cleanup()

	if _, err := client.conn.Write([]byte("*2\r\n$3\r\nGET\r\n+oops\r\n")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	reply, err := client.reader.ReadValue()
	if err != nil {
		t.Fatalf("ReadValue() error = %v", err)
	}
	if reply.Type != resp.TypeError || !strings.Contains(reply.Str, "Protocol error") {
		t.Fatalf("reply = %#v, want a protocol error", reply)
	}
}

func TestServerShutdownClosesConnections(t *testing.T) {
	client, _, cleanup := startServer(t, nil)

	client.do(t, "PING")
	cleanup() // cancels the context and waits for Serve to return

	client.conn.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := client.reader.ReadValue(); err == nil {
		t.Fatal("connection stayed open after shutdown")
	}
}

func TestServerConcurrentClients(t *testing.T) {
	const (
		clients       = 16
		perClient     = 50
		expectedTotal = clients * perClient
	)

	_, address, cleanup := startServer(t, nil)
	defer cleanup()

	var wg sync.WaitGroup
	for c := 0; c < clients; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := net.Dial("tcp", address)
			if err != nil {
				t.Errorf("Dial() error = %v", err)
				return
			}
			defer conn.Close()

			client := newTestClient(conn)
			for i := 0; i < perClient; i++ {
				if _, err := client.try("INCR", "shared"); err != nil {
					t.Errorf("INCR: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()

	conn, err := net.Dial("tcp", address)
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	defer conn.Close()

	want := strconv.Itoa(expectedTotal)
	if got := newTestClient(conn).do(t, "GET", "shared"); got.Str != want {
		t.Fatalf("GET shared = %q, want %q", got.Str, want)
	}
}

type testClient struct {
	conn   net.Conn
	reader *resp.Reader
	writer *resp.Writer
}

func newTestClient(conn net.Conn) *testClient {
	return &testClient{conn: conn, reader: resp.NewReader(conn), writer: resp.NewWriter(conn)}
}

// try sends a command and returns the reply, for use off the test goroutine.
func (c *testClient) try(args ...string) (resp.Value, error) {
	if err := c.writer.Write(resp.Command(args...)); err != nil {
		return resp.Value{}, err
	}
	if err := c.writer.Flush(); err != nil {
		return resp.Value{}, err
	}
	return c.reader.ReadValue()
}

func (c *testClient) do(t *testing.T, args ...string) resp.Value {
	t.Helper()
	reply, err := c.try(args...)
	if err != nil {
		t.Fatalf("command %q: %v", args, err)
	}
	return reply
}

// replyText renders a reply as the text a client would display.
func replyText(v resp.Value) string {
	if v.Type == resp.TypeInteger {
		return strconv.FormatInt(v.Int, 10)
	}
	return v.Str
}

// startServer boots a server on an ephemeral port and returns a connected
// client, the address it bound to, and a cleanup function that shuts
// everything down.
func startServer(t *testing.T, persister Persister) (*testClient, string, func()) {
	t.Helper()

	srv := New("127.0.0.1:0", NewExecutor(store.New(), persister, nil), nil)
	if err := srv.Listen(); err != nil {
		t.Fatalf("Listen() error = %v", err)
	}
	address := srv.Addr().String()

	ctx, cancel := context.WithCancel(context.Background())
	served := make(chan error, 1)
	go func() { served <- srv.Serve(ctx) }()

	conn, err := net.Dial("tcp", address)
	if err != nil {
		cancel()
		t.Fatalf("Dial() error = %v", err)
	}

	client := newTestClient(conn)
	cleanup := func() {
		cancel()
		select {
		case err := <-served:
			if err != nil {
				t.Errorf("Serve() error = %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("Serve() did not return after cancellation")
		}
	}
	return client, address, cleanup
}
