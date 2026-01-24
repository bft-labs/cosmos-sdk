package tracekv

import (
	"bytes"
	"encoding/json"
	"io"

	"cosmossdk.io/log"
)

var (
	_ io.Writer = (*TraceWriter)(nil)
	_ io.Closer = (*TraceWriter)(nil)
)

// TraceWriter implements io.Writer and buffers trace operations.
// It logs all buffered operations via logger.Debug() when Flush() is called.
type TraceWriter struct {
	logger log.Logger
	ops    []json.RawMessage
}

// NewTraceWriter creates a new TraceWriter with the given logger.
func NewTraceWriter(logger log.Logger) *TraceWriter {
	return &TraceWriter{logger: logger}
}

// Write implements io.Writer. It buffers the trace operation (JSON line) for batch logging.
func (w *TraceWriter) Write(p []byte) (n int, err error) {
	// Remove trailing newline if present
	data := bytes.TrimSuffix(p, []byte("\n"))
	if len(data) > 0 {
		w.ops = append(w.ops, json.RawMessage(data))
	}
	return len(p), nil
}

// Flush writes all buffered trace operations to the logger and clears the buffer.
func (w *TraceWriter) Flush() {
	if w.logger == nil || len(w.ops) == 0 {
		return
	}

	w.logger.Debug("store trace set",
		"count", len(w.ops),
		"traces", w.ops,
	)
	w.ops = w.ops[:0]
}

// Close implements io.Closer. It flushes any remaining buffered operations.
func (w *TraceWriter) Close() error {
	w.Flush()
	return nil
}
