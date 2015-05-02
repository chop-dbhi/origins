package transactor

import (
	"testing"

	"github.com/chop-dbhi/origins/fact"
	"github.com/chop-dbhi/origins/testutil"
)

func benchIndexRand(b *testing.B, n int) {
	b.StopTimer()

	facts := testutil.RandFacts(n, "d", "e", "a", "v")

	var f *fact.Fact

	b.StartTimer()

	for i := 0; i < b.N; i++ {
		idx := NewIndex()

		for _, f = range facts {
			idx.Add(f)
		}
	}
}

func BenchmarkIndexRand__100(b *testing.B) {
	benchIndexRand(b, 100)
}

func BenchmarkIndexRand__1000(b *testing.B) {
	benchIndexRand(b, 1000)
}

func BenchmarkIndexRand__10000(b *testing.B) {
	benchIndexRand(b, 10000)
}

func BenchmarkIndexRand__100000(b *testing.B) {
	benchIndexRand(b, 100000)
}

func BenchmarkIndexRand__1000000(b *testing.B) {
	benchIndexRand(b, 1000000)
}

func benchIndex(b *testing.B, n int) {
	b.StopTimer()

	facts := testutil.VariableFacts(n, 20, 0.3)

	var (
		f   *fact.Fact
		idx *Index
	)

	b.StartTimer()

	for i := 0; i < b.N; i++ {
		idx = NewIndex()

		for _, f = range facts {
			idx.Add(f)
		}
	}
}

func BenchmarkIndex__100(b *testing.B) {
	benchIndex(b, 10)
}

func BenchmarkIndex__1000(b *testing.B) {
	benchIndex(b, 100)
}

func BenchmarkIndex__10000(b *testing.B) {
	benchIndex(b, 1000)
}

func BenchmarkIndex__100000(b *testing.B) {
	benchIndex(b, 10000)
}
