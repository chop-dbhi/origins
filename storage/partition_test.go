package storage

import (
	"io"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestPartition(t *testing.T) {
	engine, _ := open(nil)

	part, _ := initPartition("test.partition", engine)

	data := make([]byte, 1000)

	part.Write(1, data)

	// Check that the header was written.
	if data, _ := engine.Get(part.storeKey); data == nil {
		t.Fatal("partition header not in storage")
	}

	// Access the partition and segment.
	seg := part.get(1)

	// Confirm in storage.
	if data, _ = engine.Get(seg.storeKey); data == nil {
		t.Fatal("segment not in storage")
	}
}

func TestPartitionReader__Full(t *testing.T) {
	engine, _ := open(nil)
	part, _ := initPartition("test.partition", engine)

	b := make([]byte, 100)

	// Fill to differentiate it from zero-allocated slices.
	for i := 0; i < len(b); i++ {
		b[i] = 1
	}

	part.Write(1, b)
	part.Write(2, b)
	part.Write(3, b)

	r := part.Reader()

	// 1000 byte buffer, allocated as zeros
	buf := make([]byte, 1000)

	n, err := r.Read(buf)

	if err != io.EOF {
		t.Error("partition reader did not return an EOF")
	}

	// All three segments were read
	assert.Equal(t, 300, n)
	assert.Equal(t, byte(1), buf[299])
	assert.Equal(t, byte(0), buf[300])
}

func TestPartitionReader__Incr(t *testing.T) {
	engine, _ := open(nil)
	part, _ := initPartition("test.partition", engine)

	b := make([]byte, 100)

	// Fill to differentiate it from zero-allocated slices.
	for i := 0; i < len(b); i++ {
		b[i] = 1
	}

	part.Write(1, b)
	part.Write(2, b)
	part.Write(3, b)

	r := part.Reader()
	buf := make([]byte, 10)

	// Track iterations
	i := 0

	for {
		n, err := r.Read(buf)

		// Nothing expected for this iteration
		if i == 30 && (err != io.EOF || n != 0) {
			t.Fatal("expected an EOF with zero bytes")
		} else {
			break
		}

		if n != len(buf) {
			t.Fatalf("expected only 10 bytes to be read, got %d", n)
		}

		i += 1
	}
}

// Fetches segments from storage transparently.
func TestPartitionReader__Lazy(t *testing.T) {
	engine, _ := open(nil)
	part, _ := initPartition("test.partition", engine)

	b := make([]byte, 100)

	// Fill to differentiate it from zero-allocated slices.
	for i := 0; i < len(b); i++ {
		b[i] = 1
	}

	part.Write(1, b)
	part.Write(2, b)
	part.Write(3, b)

	// Clear local cache
	part.segments = make(map[uint64]*segment)

	r := part.Reader()
	buf := make([]byte, 10)

	// Track iterations
	i := 0

	for {
		n, err := r.Read(buf)

		// One segment should be loaded every 10 iterations for this
		// size buffer.
		if i%10 == 0 {
			assert.Equal(t, i/10+1, len(part.segments))
		}

		// Nothing expected for this iteration
		if i == 30 && (err != io.EOF || n != 0) {
			t.Fatal("expected an EOF with zero bytes")
		} else {
			break
		}

		if n != len(buf) {
			t.Fatalf("expected only 10 bytes to be read, got %d", n)
		}

		i += 1
	}
}

func benchPartitionWrite(b *testing.B, engine Engine, n int) {
	part, _ := initPartition("test.partition", engine)

	// Allocate N zeros
	data := make([]byte, n)

	for i := 0; i < b.N; i++ {
		part.Write(uint64(i), data)
	}
}

func BenchmarkMemoryPartitionWrite__100(b *testing.B) {
	logrus.SetLevel(logrus.WarnLevel)
	engine, _ := open(nil)
	benchPartitionWrite(b, engine, 100)
}

func BenchmarkMemoryPartitionWrite__1000(b *testing.B) {
	logrus.SetLevel(logrus.WarnLevel)
	engine, _ := open(nil)
	benchPartitionWrite(b, engine, 1000)
}

func BenchmarkMemoryPartitionWrite__10000(b *testing.B) {
	logrus.SetLevel(logrus.WarnLevel)
	engine, _ := open(nil)
	benchPartitionWrite(b, engine, 10000)
}
