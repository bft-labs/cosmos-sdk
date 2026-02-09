package tracekv

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"
)

const (
	// DefaultMaxBytes is the maximum buffer size before flushing (1MB).
	DefaultMaxBytes = 1 << 20
)

var (
	_ io.Writer = (*MemTraceWriter)(nil)
	_ io.Closer = (*MemTraceWriter)(nil)
)

// storeTraceSet is the bundled format expected by cosmos-analyzer.
type storeTraceSet struct {
	Msg    string            `json:"_msg"`
	Height int64             `json:"height"`
	Count  int               `json:"count"`
	Traces []json.RawMessage `json:"traces"`
}

// traceWorkItem bundles trace data for async compression.
type traceWorkItem struct {
	opsByHeight map[int64][]json.RawMessage
	minHeight   int64
	maxHeight   int64
}

// MemTraceWriter writes trace operations to range-based gzipped files.
// Multiple block heights are buffered and written to a single file named
// trace-{minHeight}-{maxHeight}.gz when the buffer reaches the size threshold.
// Compression and file I/O are performed asynchronously in a background goroutine.
type MemTraceWriter struct {
	dir         string
	opsByHeight map[int64][]json.RawMessage
	minHeight   int64
	maxHeight   int64
	currentSize int
	maxBytes    int
	mu          sync.Mutex

	// async compression
	workCh chan traceWorkItem
	wg     sync.WaitGroup
}

// NewTraceFileWriter creates a new MemTraceWriter that writes to the given directory.
func NewTraceFileWriter(dir string) (*MemTraceWriter, error) {
	return NewTraceFileWriterWithSize(dir, DefaultMaxBytes)
}

// NewTraceFileWriterWithSize creates a MemTraceWriter with a custom buffer size.
func NewTraceFileWriterWithSize(dir string, maxBytes int) (*MemTraceWriter, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create trace dir: %w", err)
	}
	if maxBytes <= 0 {
		maxBytes = DefaultMaxBytes
	}
	w := &MemTraceWriter{
		dir:         dir,
		opsByHeight: make(map[int64][]json.RawMessage),
		maxBytes:    maxBytes,
		workCh:      make(chan traceWorkItem, 64),
	}
	w.wg.Add(1)
	go w.compressor()
	return w, nil
}

// Write implements io.Writer. It buffers the trace operation for batch writing.
func (w *MemTraceWriter) Write(p []byte) (n int, err error) {
	w.mu.Lock()

	height := extractHeightFromOp(p)
	if height == 0 {
		w.mu.Unlock()
		return len(p), nil
	}

	data := make([]byte, len(p))
	copy(data, p)

	if w.minHeight == 0 || height < w.minHeight {
		w.minHeight = height
	}
	if height > w.maxHeight {
		w.maxHeight = height
	}

	w.opsByHeight[height] = append(w.opsByHeight[height], json.RawMessage(data))
	w.currentSize += len(data)

	var wi traceWorkItem
	if w.currentSize >= w.maxBytes {
		wi = w.takeBufferLocked()
	}
	w.mu.Unlock()

	if wi.opsByHeight != nil {
		w.workCh <- wi
	}

	return len(p), nil
}

// takeBufferLocked swaps out the current buffer and returns it. Caller must hold w.mu.
func (w *MemTraceWriter) takeBufferLocked() traceWorkItem {
	wi := traceWorkItem{
		opsByHeight: w.opsByHeight,
		minHeight:   w.minHeight,
		maxHeight:   w.maxHeight,
	}
	w.opsByHeight = make(map[int64][]json.RawMessage)
	w.minHeight = 0
	w.maxHeight = 0
	w.currentSize = 0
	return wi
}

// compressor is the background goroutine that processes work items.
func (w *MemTraceWriter) compressor() {
	defer w.wg.Done()
	for wi := range w.workCh {
		w.writeTraceFile(wi)
	}
}

// writeTraceFile compresses and writes a work item to disk.
func (w *MemTraceWriter) writeTraceFile(wi traceWorkItem) {
	if len(wi.opsByHeight) == 0 {
		return
	}

	filename := filepath.Join(w.dir, fmt.Sprintf("trace-%d-%d.gz", wi.minHeight, wi.maxHeight))
	f, err := os.Create(filename)
	if err != nil {
		return
	}
	defer f.Close()

	gw := gzip.NewWriter(f)
	defer gw.Close()

	for height := wi.minHeight; height <= wi.maxHeight; height++ {
		ops, ok := wi.opsByHeight[height]
		if !ok {
			continue
		}

		traceSet := storeTraceSet{
			Msg:    "store trace set",
			Height: height,
			Count:  len(ops),
			Traces: ops,
		}

		line, err := json.Marshal(traceSet)
		if err != nil {
			continue
		}
		gw.Write(line)
		gw.Write([]byte("\n"))
	}
}

var blockHeightKey = []byte(`"blockHeight":`)

func extractHeightFromOp(data []byte) int64 {
	idx := bytes.Index(data, blockHeightKey)
	if idx == -1 {
		return 0
	}

	start := idx + len(blockHeightKey)
	for start < len(data) && (data[start] == ' ' || data[start] == '\t') {
		start++
	}

	end := start
	for end < len(data) && data[end] >= '0' && data[end] <= '9' {
		end++
	}

	if start == end {
		return 0
	}

	height, _ := strconv.ParseInt(string(data[start:end]), 10, 64)
	return height
}

// Flush synchronously writes any pending buffer to disk.
func (w *MemTraceWriter) Flush() {
	w.mu.Lock()
	if len(w.opsByHeight) == 0 {
		w.mu.Unlock()
		return
	}
	wi := w.takeBufferLocked()
	w.mu.Unlock()

	// Write synchronously for explicit Flush calls
	w.writeTraceFile(wi)
}

// Close stops the background compressor and flushes any remaining data.
func (w *MemTraceWriter) Close() error {
	// Enqueue any remaining buffer
	w.mu.Lock()
	if len(w.opsByHeight) > 0 {
		wi := w.takeBufferLocked()
		w.mu.Unlock()
		w.workCh <- wi
	} else {
		w.mu.Unlock()
	}

	// Stop compressor and wait
	close(w.workCh)
	w.wg.Wait()
	return nil
}

func (w *MemTraceWriter) Dir() string {
	return w.dir
}
