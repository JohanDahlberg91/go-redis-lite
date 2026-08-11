Concurrent In-Memory Cache (Redis-Lite)
1. Architectural Architecture & Directory StructurePlaintextredis-lite/
├── cmd/
│   └── server/          # Application entry point
│       └── main.go
├── pkg/
│   ├── resp/            # RESP protocol parser and encoder
│   │   ├── parser.go
│   │   ├── parser_test.go
│   │   └── types.go
│   ├── store/           # In-memory storage engine & eviction
│   │   ├── store.go
│   │   ├── store_test.go
│   │   └── item.go
│   ├── aof/             # Append-Only File persistence
│   │   └── aof.go
│   └── server/          # TCP socket listener & connection handler
│       └── server.go
├── go.mod
└── Makefile

2. Key System ComponentsRESP Protocol Handler: Parses incoming TCP byte streams into typed commands (e.g., PING, GET, SET, DEL, EXPIRE) based on the Redis Serialization Protocol format (* for Arrays, $ for Bulk Strings).Core Store (store.Store): Encapsulates map[string]Item protected by sync.RWMutex.Eviction Worker: Runs a background ticker sampling keys with expiration timestamps and removing stale entries actively, alongside passive expiration on GET calls.AOF Persistence Manager: A background thread that writes mutating commands (SET, DEL, EXPIRE) to disk using bufio.Writer and replays them sequentially upon server reboot.

3. Step-by-Step Implementation Task Plan1.
Phase 1: TCP Server & RESP Protocol Parser:Target: Parse raw client bytes into Go data structures.Implement a basic TCP server using net.Listen("tcp", port).Create pkg/resp to handle reading Redis protocol types:Standard bulk strings ($5\r\nhello\r\n), simple strings (+OK\r\n), and arrays (*2\r\n...).Write unit tests for reading and serializing RESP commands.Output: Server running on port 6379 capable of receiving redis-cli ping and responding with +PONG\r\n.2.

Phase 2: Thread-Safe In-Memory Core Engine:Target: In-memory data store supporting fundamental CRUD operations.Define Item struct holding Value interface{} and ExpiresAt time.Time.Construct Store struct wrapping map[string]Item and sync.RWMutex.Implement SET key val, GET key, DEL key, and EXISTS key.Use read-locks (RLock) for queries and write-locks (Lock) for mutations.Output: Core store passing concurrency race conditions under go test -race.3.

Phase 3: TTL Expiration Mechanism:Target: Passive and active key cleanup without blocking reads.Passive Expiration: On GET, check if time.Now().After(item.ExpiresAt). If expired, delete and return nil.Active Expiration: Launch a background goroutine controlled by a time.Ticker (e.g., every 100ms).Randomly sample 20 keys with active TTLs; if >25% are expired, sweep and repeat immediately.Use context.Context to handle graceful termination of background workers.4.

Phase 4: Persistence via Append-Only File (AOF):Target: Disk persistence and state recovery on reboot.Create pkg/aof to open an appending log file (os.OpenFile).Set up a channel to queue mutating commands (SET, DEL, EXPIRE) and asynchronously write them using bufio.Writer.Write a recovery routine executed during main.go startup that reads the AOF file line-by-line and executes the stored commands back into the store.5.Phase 5: Concurrency Testing & Benchmarking:Target: High throughput and memory safety verification.Write parallel benchmarks (go test -bench=. -benchmem) comparing custom cache performance against standard maps.Run load tests using redis-benchmark -p 6379 -n 100000 -c 50.