package log

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"math/rand"
)

// gzipBytes compresses the input using gzip and returns the compressed bytes.
func gzipBytes(p []byte) []byte {
	var buf bytes.Buffer
	zw, _ := gzip.NewWriterLevel(&buf, gzip.BestSpeed)
	_, _ = zw.Write(p)
	_ = zw.Close()
	return buf.Bytes()
}

// makeIncompressible returns a deterministic, pseudo-random byte slice of length n.
// Using a fixed seed ensures reproducible sizes in tests.
func makeIncompressible(n int) []byte {
	b := make([]byte, n)
	r := rand.New(rand.NewSource(1))
	for i := range b {
		b[i] = byte(r.Intn(256))
	}
	return b
}

// byteSize returns a human-readable string for byte count (e.g., "4KB", "1MB").
func byteSize(n int) string {
	const (
		kb = 1 << 10
		mb = 1 << 20
	)
	switch {
	case n >= mb:
		return fmt.Sprintf("%dMB", n/mb)
	case n >= kb:
		return fmt.Sprintf("%dKB", n/kb)
	default:
		return fmt.Sprintf("%dB", n)
	}
}
