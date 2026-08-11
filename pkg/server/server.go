// Package server exposes the store over TCP using the RESP protocol. Each
// client connection is handled by its own goroutine; the store's lock makes
// concurrent command execution safe.
package server

import (
	"context"
	"errors"
	"io"
	"log"
	"net"
	"strings"
	"sync"

	"github.com/JohanDahlberg91/go-redis-lite/pkg/resp"
)

// Server accepts client connections and dispatches their commands.
type Server struct {
	addr     string
	executor *Executor
	logger   *log.Logger

	mu       sync.Mutex
	listener net.Listener
	conns    map[net.Conn]struct{}
	closed   bool

	wg sync.WaitGroup
}

// New returns a server that will listen on addr and run commands through
// executor. A nil logger discards output.
func New(addr string, executor *Executor, logger *log.Logger) *Server {
	if logger == nil {
		logger = log.New(io.Discard, "", 0)
	}
	return &Server{
		addr:     addr,
		executor: executor,
		logger:   logger,
		conns:    make(map[net.Conn]struct{}),
	}
}

// Listen binds the server's address without accepting yet. Call it directly
// when you need the resolved address (for example after asking for port 0)
// before serving.
func (s *Server) Listen() error {
	listener, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.listener = listener
	s.mu.Unlock()
	return nil
}

// Addr returns the address the server is bound to, or nil before Listen.
func (s *Server) Addr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener == nil {
		return nil
	}
	return s.listener.Addr()
}

// ListenAndServe binds and then serves until ctx is cancelled.
func (s *Server) ListenAndServe(ctx context.Context) error {
	if err := s.Listen(); err != nil {
		return err
	}
	return s.Serve(ctx)
}

// Serve accepts connections until ctx is cancelled, then closes every open
// connection and waits for the handlers to finish.
func (s *Server) Serve(ctx context.Context) error {
	s.mu.Lock()
	listener := s.listener
	s.mu.Unlock()
	if listener == nil {
		return errors.New("server: Listen must be called before Serve")
	}
	s.logger.Printf("listening on %s", listener.Addr())

	stopped := make(chan struct{})
	defer close(stopped)
	go func() {
		select {
		case <-ctx.Done():
			s.shutdown()
		case <-stopped:
		}
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			if s.isClosed() {
				break
			}
			var netErr net.Error
			if errors.As(err, &netErr) && netErr.Timeout() {
				continue
			}
			s.wg.Wait()
			return err
		}
		s.trackConn(conn)
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			defer s.forgetConn(conn)
			s.handleConn(conn)
		}()
	}

	s.wg.Wait()
	s.logger.Printf("server stopped")
	return nil
}

// Close stops the server the same way a cancelled context would.
func (s *Server) Close() { s.shutdown() }

func (s *Server) shutdown() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	listener := s.listener
	conns := make([]net.Conn, 0, len(s.conns))
	for conn := range s.conns {
		conns = append(conns, conn)
	}
	s.mu.Unlock()

	if listener != nil {
		listener.Close()
	}
	// Closing the sockets unblocks handlers parked in a read.
	for _, conn := range conns {
		conn.Close()
	}
}

func (s *Server) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

func (s *Server) trackConn(conn net.Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		conn.Close()
		return
	}
	s.conns[conn] = struct{}{}
}

func (s *Server) forgetConn(conn net.Conn) {
	s.mu.Lock()
	delete(s.conns, conn)
	s.mu.Unlock()
	conn.Close()
}

// handleConn reads commands until the client goes away or sends something
// unparseable.
func (s *Server) handleConn(conn net.Conn) {
	reader := resp.NewReader(conn)
	writer := resp.NewWriter(conn)

	for {
		args, err := reader.ReadCommand()
		if err != nil {
			var protoErr *resp.ProtocolError
			if errors.As(err, &protoErr) {
				// The stream cannot be resynchronised, so report and hang up.
				writer.Write(resp.Error("ERR " + protoErr.Error()))
				writer.Flush()
				s.logger.Printf("%s: %v", conn.RemoteAddr(), err)
			}
			return
		}
		if len(args) == 0 {
			continue
		}

		if strings.EqualFold(args[0], "QUIT") {
			writer.Write(resp.OK)
			writer.Flush()
			return
		}

		if err := writer.Write(s.executor.Execute(args)); err != nil {
			return
		}
		// Hold the reply back only while more complete commands are already
		// buffered, so a pipelining client gets its replies in one packet.
		if reader.Buffered() == 0 {
			if err := writer.Flush(); err != nil {
				return
			}
		}
	}
}
