package log

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"time"

	gogoproto "github.com/cosmos/gogoproto/proto"
)

// MemLoggerConfig configures the in-memory compressing logger.
type MemLoggerConfig struct {
	// Interval controls how often the current in-memory buffer is compressed
	// and asynchronously appended to the WAL.
	// - > 0: enable time-based flushing at the given cadence.
	// - == 0: disable time-based flushing (size-only mode when MemoryLimitBytes > 0).
	// When both Interval == 0 and MemoryLimitBytes <= 0, a 2s default is used
	// to avoid unbounded buffering.
	Interval time.Duration

	// MemoryLimitBytes caps how many uncompressed bytes are held in memory.
	// When the buffer reaches this size, it is immediately compressed and
	// appended to the WAL (async). If zero, only the time-based trigger is used.
	MemoryLimitBytes int

	// GzipLevel controls compression level for gzip. If 0, uses gzip.DefaultCompression.
	GzipLevel int

	// The peer's canonical ID - the hash of its public key.
	P2pNodeId string

	// OutputDir is the application root directory from which the WAL path
	// is derived ("<OutputDir>/log.wal/..."). If empty, the current
	// working directory is used as the root ("./log.wal/..."). This is not
	// a behavior knob; it's where files are written.
	OutputDir string

	// EnableFilter toggles message filtering using the allow-list built by
	// buildDefaultAllowedMsgs. When true, only messages whose text matches an
	// entry in the allow-list (case-insensitive, exact match) are logged,
	// regardless of level. When false, all messages are logged.
	EnableFilter bool
	// ConsoleLogger, when non-nil, receives INFO/WARN/ERROR logs using the
	// standard SDK Logger interface. Use this to mirror logs to the operator's
	// chosen format/level while still buffering everything in memory/WAL.
	// Leave nil to disable consoleLogger mirroring.
	ConsoleLogger Logger
}

// MemLogger implements Logger and buffers JSONL log events in memory.
// It periodically compresses the buffer into gzip chunks to limit growth,
// and can flush all chunks (concatenated gzip streams) to disk.
type MemLogger struct {
	magg *memAggregator
	ctx  []any
	// consoleLogger mirrors human-facing logs (info/warn/error) through a regular logger
	// so operators see the expected format/level filtering.
	consoleLogger Logger
}

// Ensure MemLogger implements the SDK Logger interface.
var _ Logger = (*MemLogger)(nil)

// NewMemLogger creates a new in-memory compressing logger with the given config.
// WAL is required; if the WAL cannot be initialized, returns an error.
func NewMemLogger(cfg MemLoggerConfig) (Logger, error) {
	// Interval semantics:
	// - Interval > 0: enable time-based flushing at the given cadence.
	// - Interval == 0: disable time-based flushing. Combined with a positive
	//   MemoryLimitBytes, this yields a size-only policy.
	// If both Interval == 0 and MemoryLimitBytes <= 0, fall back to a sane
	// default interval to avoid never flushing.
	if cfg.Interval <= 0 && cfg.MemoryLimitBytes <= 0 {
		cfg.Interval = 2 * time.Second
	}
	if cfg.GzipLevel == 0 {
		// Favor speed to minimize runtime overhead.
		cfg.GzipLevel = gzip.BestSpeed
	}
	console := cfg.ConsoleLogger
	agg, err := newMemAggregator(cfg)
	if err != nil {
		return nil, err
	}
	return &MemLogger{magg: agg, consoleLogger: console}, nil
}

// Info logs a message at level info.
func (l *MemLogger) Info(msg string, keyVals ...any) {
	l.consoleLogger.Info(msg, keyVals...)
	l.magg.append("info", l.ctx, msg, keyVals...)
}

// Warn logs a message at level warn.
func (l *MemLogger) Warn(msg string, keyVals ...any) {
	l.consoleLogger.Warn(msg, keyVals...)
	l.magg.append("warn", l.ctx, msg, keyVals...)
}

// Error logs a message at level error.
func (l *MemLogger) Error(msg string, keyVals ...any) {
	l.consoleLogger.Error(msg, keyVals...)
	l.magg.append("error", l.ctx, msg, keyVals...)
}

// Debug logs a message at level debug.
func (l *MemLogger) Debug(msg string, keyVals ...any) { l.magg.append("debug", l.ctx, msg, keyVals...) }

// With returns a child logger that adds the provided keyvals to each event.
func (l *MemLogger) With(keyVals ...any) Logger {
	// copy context defensively
	newCtx := make([]any, 0, len(l.ctx)+len(keyVals))
	newCtx = append(newCtx, l.ctx...)
	newCtx = append(newCtx, keyVals...)
	var childConsole Logger
	if l.consoleLogger != nil {
		childConsole = l.consoleLogger.With(keyVals...)
	}
	return &MemLogger{magg: l.magg, ctx: newCtx, consoleLogger: childConsole}
}

// Impl returns the underlying implementation (self).
func (l *MemLogger) Impl() any { return l }

// Close stops the background compressor goroutine. It does not flush.
func (l *MemLogger) Close() error { l.magg.close(); return nil }

// Flush compresses any pending in-memory buffer, appends it to the WAL,
// and performs an fsync to ensure durability.
func (l *MemLogger) Flush() error {
	// Forced WAL mode: compress any pending buffer and fsync.
	if l.magg.wal == nil {
		return errors.New("memlogger: WAL not initialized")
	}
	if err := l.magg.flushToWAL(); err != nil {
		return err
	}
	return l.magg.wal.Sync()
}

// ---- Internal aggregator ----

// memAggregator buffers log events in memory, compresses them periodically,
// and appends compressed chunks to a WAL. It manages time-based and size-based
// flushing policies, a compression worker pool, and an optional message filter.
type memAggregator struct {
	cfg MemLoggerConfig

	mu  sync.Mutex
	buf bytes.Buffer // current uncompressed JSONL buffer
	// running stats for current buffer
	curRecs uint32
	firstTS int64
	lastTS  int64

	tick   *time.Ticker
	stopCh chan struct{}
	wg     sync.WaitGroup

	// compression pipeline
	// workCh carries a workItem with the buffer and precomputed metadata.
	// Capacity is sized to reduce blocking on the hot path.
	workCh chan workItem
	compWg sync.WaitGroup

	// pools to reduce allocations during compression
	gzPool  sync.Pool // *gzip.Writer
	bufPool sync.Pool // *bytes.Buffer

	// optional append-only WAL writer; when present, compressed chunks
	// are appended to disk immediately after compression.
	wal *walWriter

	// allowedMsgs holds lowercased allowed messages for exact matching.
	// If non-empty, logs are kept only when their msg matches a key in
	// this set after lowercasing (case-insensitive), regardless of level.
	// Checked before any JSON/gzip.
	allowedMsgs map[string]struct{}
}

// event is encoded to a single flat JSON object similar to TM JSON logger
// output shape: top-level keys include "ts", "level", and "_msg",
// plus any contextual keyvals, without nesting.
type event map[string]any

// workItem bundles an uncompressed buffer and metadata collected during
// logging so the compressor can avoid rescanning the payload.
type workItem struct {
	data    []byte
	recs    uint32
	firstTS int64
	lastTS  int64
}

// newMemAggregator creates and initializes a memAggregator with the given configuration.
// It starts background goroutines for periodic flushing (if enabled) and compression.
// Returns an error if WAL initialization fails.
func newMemAggregator(cfg MemLoggerConfig) (*memAggregator, error) {
	m := &memAggregator{
		cfg:    cfg,
		stopCh: make(chan struct{}),
		// Size the work queue based on the configured uncompressed
		// memory limit. A larger queue helps avoid blocking when
		// the limit is small and many small chunks are enqueued.
		workCh:  make(chan workItem, workQueueCap(cfg.MemoryLimitBytes)),
		gzPool:  sync.Pool{New: func() any { w, _ := gzip.NewWriterLevel(io.Discard, cfg.GzipLevel); return w }},
		bufPool: sync.Pool{New: func() any { return new(bytes.Buffer) }},
	}
	// Initialize allow-list only when filtering is enabled. If filtering is
	// disabled, leave the map nil so the fast-path check naturally passes.
	if cfg.EnableFilter {
		m.allowedMsgs = buildDefaultAllowedMsgs()
	}

	// Initialize a WAL writer so that compressed chunks are appended to disk
	// as they are produced. If OutputDir is empty, fall back to CWD.
	root := cfg.OutputDir
	if root == "" {
		root = "."
	}
	dataDir := root
	if base := filepath.Base(root); base != "data" {
		dataDir = filepath.Join(root, "data")
	}
	// Derive WAL buffer size from the memory limit using a simple heuristic
	// informed by benchmarks. This reduces syscall churn for large chunks.
	// - memory-bytes >= 1 GiB  -> bufSize = 16 MiB
	// - memory-bytes >= 256 MiB-> bufSize = 8 MiB
	// - otherwise              -> bufSize = 4 MiB
	bufSize := 4 << 20
	if cfg.MemoryLimitBytes >= (1 << 30) {
		bufSize = 16 << 20
	} else if cfg.MemoryLimitBytes >= (256 << 20) {
		bufSize = 8 << 20
	}
	w, err := newWalWriter(walWriterConfig{
		DataDir: dataDir,
		NodeID:  cfg.P2pNodeId,
		BufSize: bufSize,
	})
	if err != nil {
		return nil, fmt.Errorf("memlogger: WAL initialization failed: %w", err)
	}
	m.wal = w
	// Start periodic flushing only when enabled (Interval > 0).
	if cfg.Interval > 0 {
		m.tick = time.NewTicker(cfg.Interval)
		m.wg.Add(1)
		go m.run()
	}
	m.compWg.Add(1)
	go m.compressor()
	return m, nil
}

// run is the periodic flushing goroutine that triggers buffer compression
// at the configured interval. It runs only when Interval > 0.
func (magg *memAggregator) run() {
	defer magg.wg.Done()
	for {
		select {
		case <-magg.tick.C:
			magg.enqueueCurrentBuffer()
		case <-magg.stopCh:
			return
		}
	}
}

// append adds a log event to the in-memory buffer. It applies message filtering
// if enabled, merges context and keyvals, encodes the event as JSONL, mirrors
// info/warn/error logs to the consoleLogger if configured, and triggers size-based
// compression if the memory limit is exceeded.
func (magg *memAggregator) append(level string, ctx []any, msg string, keyvals ...any) {
	// Early filter based on message text (case-insensitive), applied to all levels.
	if len(magg.allowedMsgs) > 0 {
		lm := strings.ToLower(msg)
		if _, ok := magg.allowedMsgs[lm]; !ok {
			return
		}
	}
	// Build flat event to mirror go-kit JSON logger format used by TMJSONLogger.
	ev := make(event, 4+len(ctx)+len(keyvals))
	now := time.Now().UTC()
	ev["ts"] = now
	ev["level"] = level
	ev["_msg"] = msg

	// merge ctx + keyvals into top-level fields (pairwise)
	merged := make([]any, 0, len(ctx)+len(keyvals))
	merged = append(merged, ctx...)
	merged = append(merged, keyvals...)
	for i := 0; i < len(merged); i += 2 {
		var key string
		if i < len(merged) {
			if ks, ok := merged[i].(string); ok {
				key = ks
			} else {
				key = toString(merged[i])
			}
		}
		var val any
		if i+1 < len(merged) {
			val = normalizeValue(merged[i+1])
		} else {
			val = "<missing>"
		}
		ev[key] = val
	}

	// encode as JSONL
	b, err := json.Marshal(ev)
	if err != nil {
		// If marshaling fails (e.g., cyclic reference, channel, func), log a fallback message.
		// This should be extremely rare in practice.
		fallback := map[string]any{
			"ts":    now,
			"level": level,
			"_msg":  msg,
			"error": "failed to marshal log event",
		}
		b, _ = json.Marshal(fallback)
	}
	b = append(b, '\n')

	magg.mu.Lock()
	_, _ = magg.buf.Write(b)
	// update running stats for current buffer
	if magg.curRecs == 0 {
		magg.firstTS = now.UnixNano()
	}
	magg.curRecs++
	magg.lastTS = now.UnixNano()

	// Size-based early compression: swap buffer and enqueue to background worker.
	var wi workItem
	if maxBytes := magg.cfg.MemoryLimitBytes; maxBytes > 0 && magg.buf.Len() >= maxBytes {
		wi = magg.takeBufferWithMetaLocked()
	}
	magg.mu.Unlock()

	if len(wi.data) > 0 {
		magg.enqueueItem(wi)
	}
}

// enqueueCurrentBuffer swaps out the current in-memory buffer and sends it
// to the compression worker. Called by the periodic ticker goroutine.
func (magg *memAggregator) enqueueCurrentBuffer() {
	// Swap current buffer if any and enqueue it for compression.
	magg.mu.Lock()
	if magg.buf.Len() == 0 {
		magg.mu.Unlock()
		return
	}
	wi := magg.takeBufferWithMetaLocked()
	magg.mu.Unlock()

	magg.enqueueItem(wi)
}

// takeBufferWithMetaLocked swaps out the current uncompressed buffer and returns
// its bytes along with the collected metadata. Caller must hold m.mu.
func (magg *memAggregator) takeBufferWithMetaLocked() workItem {
	// Extract a copy of the buffer contents to send to the worker.
	// We must copy because bytes.Buffer.Bytes() returns a slice of the internal
	// buffer which will be reused after Reset().
	data := make([]byte, magg.buf.Len())
	copy(data, magg.buf.Bytes())
	wi := workItem{data: data, recs: magg.curRecs, firstTS: magg.firstTS, lastTS: magg.lastTS}
	// reset for next buffer
	magg.buf.Reset()
	magg.curRecs = 0
	magg.firstTS = 0
	magg.lastTS = 0
	return wi
}

// enqueueItem sends a workItem to the compression worker. Blocks if the
// work queue is full, providing backpressure to avoid unbounded memory growth.
func (magg *memAggregator) enqueueItem(wi workItem) {
	// Block to preserve backpressure but outside locks; this avoids dropping logs
	// and keeps compression off the hot path of logging.
	magg.workCh <- wi
}

// compressor is the background worker that reads workItems from the work queue,
// compresses them with gzip, and appends the compressed chunks to the WAL.
//
// IMPORTANT: On compression or WAL append failure, the failed chunk is DROPPED
// to preserve log ordering. Re-appending to the current buffer would cause
// out-of-order logs (failed chunk would appear after newer logs). In production,
// maintaining chronological order is more critical than zero data loss for rare
// failures (OOM during compression, disk full, I/O errors). The system will
// continue processing subsequent logs normally.
func (magg *memAggregator) compressor() {
	defer magg.compWg.Done()
	for wi := range magg.workCh {
		// compress first, then read CRC32 from gzip trailer to avoid rescanning
		chunk, err := magg.gzipWithPool(wi.data)
		if err != nil {
			// Compression failed (likely OOM or corrupted data). Drop this chunk
			// to maintain log ordering. Compression failures are extremely rare
			// in practice and indicate a serious system issue.
			// TODO(production): emit a metric/alert for dropped log chunks
			continue
		}
		// Append to WAL with metadata extracted from gzip trailer.
		var appendErr error
		if crc, ok := gzipCRC32FromMember(chunk); ok {
			appendErr = magg.wal.AppendCompressedWithMeta(chunk, wi.recs, wi.firstTS, wi.lastTS, crc)
		} else {
			// CRC32 unknown; pass 0 (allowed by semantics)
			appendErr = magg.wal.AppendCompressedWithMeta(chunk, wi.recs, wi.firstTS, wi.lastTS, 0)
		}
		if appendErr != nil {
			// WAL append failed (disk full, I/O error, etc.). Drop this chunk to
			// maintain log ordering. WAL failures indicate infrastructure issues
			// that require operator intervention.
			// TODO(production): emit a metric/alert for dropped log chunks
			continue
		}
	}
}

// gzipWithPool compresses the input bytes using pooled gzip.Writer and bytes.Buffer
// to minimize allocations. Returns the compressed bytes or an error if compression fails.
func (magg *memAggregator) gzipWithPool(in []byte) ([]byte, error) {
	// get pooled buffer and writer
	b := magg.bufPool.Get().(*bytes.Buffer)
	b.Reset()
	var out []byte
	w := magg.gzPool.Get().(*gzip.Writer)
	w.Reset(b)
	if _, err := w.Write(in); err != nil {
		_ = w.Close()
		magg.gzPool.Put(w)
		b.Reset()
		magg.bufPool.Put(b)
		return nil, err
	}
	if err := w.Close(); err != nil {
		magg.gzPool.Put(w)
		b.Reset()
		magg.bufPool.Put(b)
		return nil, err
	}
	magg.gzPool.Put(w)
	// Copy bytes to detach from pooled buffer before putting it back.
	out = make([]byte, b.Len())
	copy(out, b.Bytes())
	b.Reset()
	magg.bufPool.Put(b)
	return out, nil
}

// flushToWAL compresses any pending uncompressed buffer and appends it
// synchronously to the WAL. No-op if buffer is empty or WAL is not configured.
func (magg *memAggregator) flushToWAL() error {
	if magg.wal == nil {
		return nil
	}
	magg.mu.Lock()
	if magg.buf.Len() == 0 {
		magg.mu.Unlock()
		return nil
	}
	wi := magg.takeBufferWithMetaLocked()
	magg.mu.Unlock()

	gzChunk, err := magg.gzipWithPool(wi.data)
	if err != nil {
		return err
	}
	if crc, ok := gzipCRC32FromMember(gzChunk); ok {
		return magg.wal.AppendCompressedWithMeta(gzChunk, wi.recs, wi.firstTS, wi.lastTS, crc)
	}
	return magg.wal.AppendCompressedWithMeta(gzChunk, wi.recs, wi.firstTS, wi.lastTS, 0)
}

// close performs an orderly shutdown: stops periodic flushing, enqueues any
// remaining buffer, waits for the compression worker to finish, and syncs/closes
// the WAL. After close() returns, the aggregator must not be used.
func (magg *memAggregator) close() {
	// Stop periodic enqueues.
	close(magg.stopCh)
	if magg.tick != nil {
		magg.tick.Stop()
	}
	magg.wg.Wait()

	// Enqueue any remaining buffer before shutting down compressor.
	magg.mu.Lock()
	if magg.buf.Len() > 0 {
		wi := magg.takeBufferWithMetaLocked()
		magg.mu.Unlock()
		// Best effort: enqueue; if blocked, still wait — we are shutting down.
		magg.workCh <- wi
	} else {
		magg.mu.Unlock()
	}

	// Stop compressor and wait.
	close(magg.workCh)
	magg.compWg.Wait()

	// Ensure WAL is flushed to disk and closed.
	_ = magg.wal.Sync()
	_ = magg.wal.Close()
}

// ---- helpers ----

// toString converts a value to a string representation suitable for use as a log key.
func toString(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case []byte:
		return string(t)
	case time.Time:
		return t.Format(time.RFC3339Nano)
	default:
		b, err := json.Marshal(v)
		if err != nil {
			return "<unstringable>"
		}
		return string(b)
	}
}

// normalizeValue attempts to match the behavior of the TM JSON logger by
// stringifying values that are best represented via String(), while leaving
// proto messages and JSON-marshable types as structured values.
func normalizeValue(v any) any {
	switch t := v.(type) {
	case time.Time:
		return t
	case json.Marshaler:
		// Give precedence to explicit JSON marshaling implementations.
		return t
	case gogoproto.Message:
		// Keep protobuf messages structured to preserve fields.
		return t
	case fmt.Stringer:
		// Use the string form for types like MConnection, LazySprintf, Peer, etc.
		return t.String()
	default:
		return v
	}
}

// workQueueCap returns a buffered channel capacity for the compression work
// queue based on the uncompressed memory limit. The goal is to allow a backlog
// of roughly ~8 MiB of uncompressed data before producers block, while keeping
// caps modest to avoid excessive memory usage for large chunk sizes.
//
// When limit <= 0 (time-based flushing), use a conservative default.
func workQueueCap(limit int) int {
	// Default when no size trigger is set.
	if limit <= 0 {
		return 16
	}
	const targetBacklogBytes = 8 << 20 // ~8 MiB
	queueCap := targetBacklogBytes / limit
	if queueCap < 4 {
		queueCap = 4
	}
	if queueCap > 64 {
		queueCap = 64
	}
	return queueCap
}

// gzipCRC32FromMember extracts the CRC32 of the uncompressed payload from the
// gzip trailer of a compressed member. The gzip format stores the CRC32 and
// uncompressed size in the last 8 bytes of each member.
// Returns (crc32, true) on success, or (0, false) if the member is too short.
func gzipCRC32FromMember(m []byte) (uint32, bool) {
	if len(m) < 8 {
		return 0, false
	}
	// Trailer layout: CRC32 (4 bytes LE) + ISIZE (4 bytes LE)
	crc := binary.LittleEndian.Uint32(m[len(m)-8 : len(m)-4])
	return crc, true
}
