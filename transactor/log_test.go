package transactor

import (
	"fmt"
	"os"
	"testing"

	"github.com/chop-dbhi/origins"
	"github.com/chop-dbhi/origins/storage"
)

const testDBPath = "test.boltdb"

func TestSegment(t *testing.T) {
	os.Remove(testDBPath)

	engine, err := origins.Open("boltdb", storage.Options{
		"path": testDBPath,
	})

	segment := NewSegment(engine, 1, "test")

	gen := newRandGenerator("test", 1, 0)

	var (
		f *origins.Fact
		n = 10000
	)

	for i := 0; i < n; i++ {
		f, _ = gen.Next()

		if err = segment.Append(f); err != nil {
			t.Fatal(err)
		}
	}

	if segment.Count != n {
		t.Errorf("segment: expected %d count, got %d", n, segment.Count)
	}

	if segment.Blocks != n/blockSize {
		t.Errorf("segment: expected %d blocks, got %d", n/blockSize, segment.Blocks)
	}

	var (
		k string
		b []byte
	)

	// Confirm bolt is doing its job..
	for i := 0; i < segment.Blocks; i++ {
		k = fmt.Sprintf(BlockKey, segment.ID, i)
		b, err = engine.Get("test", k)

		if b == nil {
			t.Fatalf("segment: %s is nil", k)
		}
	}

	if err = segment.Abort(engine); err != nil {
		t.Error("segment: abort %s", err)
	}

	// Confirm aborted entries have been deleted
	for i := 0; i < segment.Blocks; i++ {
		k = fmt.Sprintf(BlockKey, segment.ID, i)
		b, err = engine.Get("test", k)

		if b != nil {
			t.Fatalf("segment: %s should be nil", k)
		}
	}
}

func benchSegmentBlockSize(b *testing.B, bs int) {
	os.Remove(testDBPath)

	engine, _ := origins.Open("boltdb", storage.Options{
		"path": testDBPath,
	})

	blockSize = bs

	var f *origins.Fact

	gen := newRandGenerator("test", 1, 0)

	segment := NewSegment(engine, 1, "test")

	for i := 0; i < b.N; i++ {
		f, _ = gen.Next()
		segment.Append(f)
	}

	engine.Multi(func(tx storage.Tx) error {
		return segment.Write(tx)
	})

	blockSize = 1000
}

func BenchmarkSegmentBlockSize__100(b *testing.B) {
	benchSegmentBlockSize(b, 100)
}

func BenchmarkSegmentBlockSize__500(b *testing.B) {
	benchSegmentBlockSize(b, 500)
}

func BenchmarkSegmentBlockSize__1000(b *testing.B) {
	benchSegmentBlockSize(b, 1000)
}

func BenchmarkSegmentBlockSize__2000(b *testing.B) {
	benchSegmentBlockSize(b, 2000)
}

func BenchmarkSegmentBlockSize__5000(b *testing.B) {
	benchSegmentBlockSize(b, 5000)
}

func BenchmarkSegmentBlockSize__10000(b *testing.B) {
	benchSegmentBlockSize(b, 10000)
}

func benchSegmentSize(b *testing.B, n int) {
	os.Remove(testDBPath)

	engine, _ := origins.Open("boltdb", storage.Options{
		"path": testDBPath,
	})

	var f *origins.Fact

	for i := 0; i < b.N; i++ {
		gen := newRandGenerator("test", 1, n)
		segment := NewSegment(engine, 1, "test")

		for j := 0; j < n; j++ {
			f, _ = gen.Next()
			segment.Append(f)
		}

		engine.Multi(func(tx storage.Tx) error {
			return segment.Write(tx)
		})
	}
}

// Segment size
func BenchmarkSegmentSize__100(b *testing.B) {
	benchSegmentSize(b, 100)
}

func BenchmarkSegmentSize__500(b *testing.B) {
	benchSegmentSize(b, 500)
}

func BenchmarkSegmentSize__1000(b *testing.B) {
	benchSegmentSize(b, 1000)
}

func BenchmarkSegmentSize__2000(b *testing.B) {
	benchSegmentSize(b, 2000)
}

func BenchmarkSegmentSize__5000(b *testing.B) {
	benchSegmentSize(b, 5000)
}

func BenchmarkSegmentSize__10000(b *testing.B) {
	benchSegmentSize(b, 10000)
}

func BenchmarkSegmentSize__50000(b *testing.B) {
	benchSegmentSize(b, 50000)
}

func BenchmarkSegmentSize__200000(b *testing.B) {
	benchSegmentSize(b, 200000)
}

func BenchmarkSegmentSize__1000000(b *testing.B) {
	benchSegmentSize(b, 1000000)
}
