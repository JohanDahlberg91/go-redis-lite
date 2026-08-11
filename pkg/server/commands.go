package server

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/JohanDahlberg91/go-redis-lite/pkg/resp"
	"github.com/JohanDahlberg91/go-redis-lite/pkg/store"
)

// Persister receives the mutating commands that must survive a restart.
// *aof.Log satisfies it.
type Persister interface {
	Append(args ...string) error
}

// Executor turns a parsed command into a reply against the store and, for
// mutations, forwards a canonical form of the command to the persister.
//
// It is used both by client connections and by the startup AOF replay; replay
// constructs an executor with a nil persister so recovered commands are not
// written back to the log.
type Executor struct {
	store     *store.Store
	persister Persister
	started   time.Time
	onError   func(error)
}

// NewExecutor returns an executor over st. persister may be nil, in which
// case nothing is persisted. onError, if set, is called when a command cannot
// be queued for persistence.
func NewExecutor(st *store.Store, persister Persister, onError func(error)) *Executor {
	return &Executor{
		store:     st,
		persister: persister,
		started:   time.Now(),
		onError:   onError,
	}
}

// commandSpec describes one command. minArgs and maxArgs count the command
// name itself; a maxArgs of -1 means "no upper bound".
type commandSpec struct {
	minArgs int
	maxArgs int
	handler func(*Executor, []string) resp.Value
}

var commands map[string]commandSpec

func init() {
	// Assigned in init rather than as a literal because several handlers
	// refer back to the table (COMMAND) or to each other.
	commands = map[string]commandSpec{
		"PING":      {1, 2, (*Executor).ping},
		"ECHO":      {2, 2, (*Executor).echo},
		"SET":       {3, -1, (*Executor).set},
		"GET":       {2, 2, (*Executor).get},
		"DEL":       {2, -1, (*Executor).del},
		"EXISTS":    {2, -1, (*Executor).exists},
		"EXPIRE":    {3, 3, (*Executor).expire},
		"PEXPIRE":   {3, 3, (*Executor).pexpire},
		"EXPIREAT":  {3, 3, (*Executor).expireAt},
		"PEXPIREAT": {3, 3, (*Executor).pexpireAt},
		"PERSIST":   {2, 2, (*Executor).persist},
		"TTL":       {2, 2, (*Executor).ttl},
		"PTTL":      {2, 2, (*Executor).pttl},
		"INCR":      {2, 2, (*Executor).incr},
		"DECR":      {2, 2, (*Executor).decr},
		"INCRBY":    {3, 3, (*Executor).incrBy},
		"DECRBY":    {3, 3, (*Executor).decrBy},
		"KEYS":      {2, 2, (*Executor).keys},
		"DBSIZE":    {1, 1, (*Executor).dbSize},
		"FLUSHALL":  {1, 2, (*Executor).flushAll},
		"FLUSHDB":   {1, 2, (*Executor).flushAll},
		"INFO":      {1, 2, (*Executor).info},
		"COMMAND":   {1, -1, (*Executor).command},
		"CONFIG":    {2, -1, (*Executor).config},
		"QUIT":      {1, 1, (*Executor).quit},
	}
}

// Execute runs one command and returns the reply to send back.
func (e *Executor) Execute(args []string) resp.Value {
	if len(args) == 0 {
		return resp.Error("ERR empty command")
	}
	name := strings.ToUpper(args[0])
	spec, ok := commands[name]
	if !ok {
		return resp.Error(fmt.Sprintf("ERR unknown command '%s'", args[0]))
	}
	if len(args) < spec.minArgs || (spec.maxArgs >= 0 && len(args) > spec.maxArgs) {
		return errWrongArgs(name)
	}
	return spec.handler(e, args)
}

// propagate hands a mutating command to the persister. Commands are
// normalised by their handlers so that replaying them is deterministic:
// relative expirations become absolute PXAT/PEXPIREAT timestamps and
// increments become the resulting SET.
func (e *Executor) propagate(args ...string) {
	if e.persister == nil {
		return
	}
	if err := e.persister.Append(args...); err != nil && e.onError != nil {
		e.onError(err)
	}
}

func (e *Executor) ping(args []string) resp.Value {
	if len(args) == 2 {
		return resp.BulkString(args[1])
	}
	return resp.SimpleString("PONG")
}

func (e *Executor) echo(args []string) resp.Value {
	return resp.BulkString(args[1])
}

func (e *Executor) quit(args []string) resp.Value {
	return resp.OK
}

// set implements SET key value [EX s|PX ms|EXAT ts|PXAT ts] [NX|XX] [KEEPTTL].
func (e *Executor) set(args []string) resp.Value {
	key, value := args[1], args[2]

	var opts store.SetOptions
	for i := 3; i < len(args); i++ {
		option := strings.ToUpper(args[i])
		switch option {
		case "NX":
			opts.NX = true
		case "XX":
			opts.XX = true
		case "KEEPTTL":
			opts.KeepTTL = true
		case "EX", "PX", "EXAT", "PXAT":
			if i+1 >= len(args) || !opts.ExpiresAt.IsZero() {
				return errSyntax
			}
			n, err := strconv.ParseInt(args[i+1], 10, 64)
			if err != nil {
				return errNotInteger
			}
			at, ok := absoluteExpiry(option, n, false)
			if !ok {
				return resp.Error("ERR invalid expire time in 'set' command")
			}
			opts.ExpiresAt = at
			i++
		default:
			return errSyntax
		}
	}
	if (opts.NX && opts.XX) || (opts.KeepTTL && !opts.ExpiresAt.IsZero()) {
		return errSyntax
	}

	if !e.store.SetWithOptions(key, value, opts) {
		return resp.NullBulkString()
	}

	persisted := []string{"SET", key, value}
	switch {
	case !opts.ExpiresAt.IsZero():
		persisted = append(persisted, "PXAT", milliString(opts.ExpiresAt))
	case opts.KeepTTL:
		persisted = append(persisted, "KEEPTTL")
	}
	e.propagate(persisted...)
	return resp.OK
}

func (e *Executor) get(args []string) resp.Value {
	value, ok := e.store.Get(args[1])
	if !ok {
		return resp.NullBulkString()
	}
	return bulkFrom(value)
}

func (e *Executor) del(args []string) resp.Value {
	removed := e.store.Del(args[1:]...)
	if removed > 0 {
		e.propagate(args...)
	}
	return resp.Integer(int64(removed))
}

func (e *Executor) exists(args []string) resp.Value {
	return resp.Integer(int64(e.store.Exists(args[1:]...)))
}

func (e *Executor) expire(args []string) resp.Value   { return e.expireWithUnit(args, "EX") }
func (e *Executor) pexpire(args []string) resp.Value  { return e.expireWithUnit(args, "PX") }
func (e *Executor) expireAt(args []string) resp.Value { return e.expireWithUnit(args, "EXAT") }
func (e *Executor) pexpireAt(args []string) resp.Value {
	return e.expireWithUnit(args, "PXAT")
}

// expireWithUnit backs EXPIRE, PEXPIRE, EXPIREAT and PEXPIREAT. All four are
// persisted as PEXPIREAT with an absolute timestamp so a replay years later
// still expires the key at the same instant.
func (e *Executor) expireWithUnit(args []string, unit string) resp.Value {
	n, err := strconv.ParseInt(args[2], 10, 64)
	if err != nil {
		return errNotInteger
	}
	// A non-positive TTL is legal here: it deletes the key immediately.
	at, ok := absoluteExpiry(unit, n, true)
	if !ok {
		return resp.Error("ERR invalid expire time")
	}
	if !e.store.ExpireAt(args[1], at) {
		return resp.Integer(0)
	}
	e.propagate("PEXPIREAT", args[1], milliString(at))
	return resp.Integer(1)
}

func (e *Executor) persist(args []string) resp.Value {
	if !e.store.Persist(args[1]) {
		return resp.Integer(0)
	}
	e.propagate("PERSIST", args[1])
	return resp.Integer(1)
}

func (e *Executor) ttl(args []string) resp.Value {
	return e.ttlWithUnit(args[1], time.Second)
}

func (e *Executor) pttl(args []string) resp.Value {
	return e.ttlWithUnit(args[1], time.Millisecond)
}

func (e *Executor) ttlWithUnit(key string, unit time.Duration) resp.Value {
	remaining, exists, hasTTL := e.store.TTL(key)
	switch {
	case !exists:
		return resp.Integer(-2)
	case !hasTTL:
		return resp.Integer(-1)
	default:
		// Round to nearest, as Redis does, so a 1s TTL never reports 0.
		return resp.Integer(int64((remaining + unit/2) / unit))
	}
}

func (e *Executor) incr(args []string) resp.Value   { return e.incrementBy(args[1], 1) }
func (e *Executor) decr(args []string) resp.Value   { return e.incrementBy(args[1], -1) }
func (e *Executor) incrBy(args []string) resp.Value { return e.incrementByArg(args, 1) }
func (e *Executor) decrBy(args []string) resp.Value { return e.incrementByArg(args, -1) }

func (e *Executor) incrementByArg(args []string, sign int64) resp.Value {
	delta, err := strconv.ParseInt(args[2], 10, 64)
	if err != nil {
		return errNotInteger
	}
	if sign < 0 {
		if delta == math.MinInt64 {
			return resp.Error("ERR decrement would overflow")
		}
		delta = -delta
	}
	return e.incrementBy(args[1], delta)
}

func (e *Executor) incrementBy(key string, delta int64) resp.Value {
	value, err := e.store.Incr(key, delta)
	switch {
	case errors.Is(err, store.ErrNotInteger):
		return errNotInteger
	case errors.Is(err, store.ErrOverflow):
		return resp.Error("ERR increment or decrement would overflow")
	case err != nil:
		return resp.Error("ERR " + err.Error())
	}
	// Persisted as the resulting value so replay does not depend on the
	// order in which concurrent increments happened to land.
	e.propagate("SET", key, strconv.FormatInt(value, 10), "KEEPTTL")
	return resp.Integer(value)
}

func (e *Executor) keys(args []string) resp.Value {
	matches := e.store.Keys(args[1])
	values := make([]resp.Value, len(matches))
	for i, key := range matches {
		values[i] = resp.BulkString(key)
	}
	return resp.Array(values...)
}

func (e *Executor) dbSize(args []string) resp.Value {
	return resp.Integer(int64(e.store.Len()))
}

func (e *Executor) flushAll(args []string) resp.Value {
	// ASYNC/SYNC are accepted and ignored: everything is in one map, so the
	// flush is immediate either way.
	if len(args) == 2 {
		switch strings.ToUpper(args[1]) {
		case "ASYNC", "SYNC":
		default:
			return errSyntax
		}
	}
	e.store.Flush()
	e.propagate(args[0])
	return resp.OK
}

func (e *Executor) info(args []string) resp.Value {
	var b strings.Builder
	fmt.Fprintf(&b, "# Server\r\nredis_version:7.0.0-redis-lite\r\n")
	fmt.Fprintf(&b, "uptime_in_seconds:%d\r\n", int64(time.Since(e.started).Seconds()))
	fmt.Fprintf(&b, "\r\n# Keyspace\r\ndb0:keys=%d\r\n", e.store.Len())
	return resp.BulkString(b.String())
}

func (e *Executor) command(args []string) resp.Value {
	// redis-cli issues COMMAND DOCS on connect. Reporting an empty command
	// table is enough to keep it happy.
	return resp.Array()
}

func (e *Executor) config(args []string) resp.Value {
	if strings.EqualFold(args[1], "GET") {
		// redis-benchmark probes a few parameters at startup and tolerates
		// an empty answer.
		return resp.Array()
	}
	return resp.OK
}

// absoluteExpiry converts an EX/PX/EXAT/PXAT argument into an absolute
// deadline. allowNonPositive permits a TTL of zero or less, which the EXPIRE
// family treats as "delete now" but SET rejects. It reports false when the
// value would overflow the time range.
func absoluteExpiry(unit string, n int64, allowNonPositive bool) (time.Time, bool) {
	switch unit {
	case "EX", "PX":
		scale := time.Second
		if unit == "PX" {
			scale = time.Millisecond
		}
		if n < math.MinInt64/int64(scale) || n > math.MaxInt64/int64(scale) {
			return time.Time{}, false
		}
		if n <= 0 && !allowNonPositive {
			return time.Time{}, false
		}
		return time.Now().Add(time.Duration(n) * scale), true
	case "EXAT":
		if n > math.MaxInt64/1000 {
			return time.Time{}, false
		}
		return time.Unix(n, 0), true
	default: // PXAT
		return time.UnixMilli(n), true
	}
}

func milliString(t time.Time) string {
	return strconv.FormatInt(t.UnixMilli(), 10)
}

func bulkFrom(value interface{}) resp.Value {
	switch typed := value.(type) {
	case string:
		return resp.BulkString(typed)
	case []byte:
		return resp.BulkString(string(typed))
	default:
		return resp.BulkString(fmt.Sprint(value))
	}
}

var (
	errSyntax     = resp.Error("ERR syntax error")
	errNotInteger = resp.Error("ERR value is not an integer or out of range")
)

func errWrongArgs(name string) resp.Value {
	return resp.Error(fmt.Sprintf("ERR wrong number of arguments for '%s' command", strings.ToLower(name)))
}
