package transactor

import (
	"testing"

	"github.com/chop-dbhi/origins"
	"github.com/chop-dbhi/origins/dal"
	"github.com/chop-dbhi/origins/storage"
	"github.com/chop-dbhi/origins/testutil"
)

func TestSegment(t *testing.T) {
	engine, err := origins.Init("mem", nil)

	segment := NewSegment(engine, "test", 1)

	gen := testutil.NewRandGenerator("test", 1, 0)

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

	id := segment.UUID

	for i := 0; i < segment.Blocks; i++ {
		bl, _ := dal.GetBlock(engine, "test", id, i, segment.Transaction)

		if bl == nil {
			t.Errorf("segment: block %s.%d is nil", id, i)
		}
	}

	if err = segment.Abort(engine); err != nil {
		t.Error("segment: abort %s", err)
	}

	for i := 0; i < segment.Blocks; i++ {
		bl, _ := dal.GetBlock(engine, "test", id, i, segment.Transaction)

		if bl != nil {
			t.Fatalf("segment: %s.%d should be nil", id, i)
		}
	}
}

func benchSegmentBlockSize(b *testing.B, bs int) {
	engine, _ := origins.Init("mem", nil)

	blockSize = bs

	var f *origins.Fact

	gen := testutil.NewRandGenerator("test", 1, 0)

	segment := NewSegment(engine, "test", 1)

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
	engine, _ := origins.Init("mem", nil)

	var f *origins.Fact

	for i := 0; i < b.N; i++ {
		gen := testutil.NewRandGenerator("test", 1, n)
		segment := NewSegment(engine, "test", 1)

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
