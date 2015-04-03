package transactor_test

import (
	"testing"

	"github.com/chop-dbhi/origins/fact"
	"github.com/chop-dbhi/origins/storage"
	"github.com/chop-dbhi/origins/storage/memory"
	"github.com/chop-dbhi/origins/testutil"
	"github.com/chop-dbhi/origins/transactor"
)

// Random facts, reset after each iteration
func benchTransactRandomReset(b *testing.B, n int) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()

		engine, _ := memory.Open(&storage.Options{})

		store, _ := storage.Init(&storage.Config{
			Engine: engine,
		})

		facts := testutil.RandFacts(n, "test")
		r := fact.NewReader(facts)

		b.StartTimer()

		transactor.Commit(store, r, "test", true)
	}
}

func BenchmarkTransactRandomReset__10(b *testing.B) {
	benchTransactRandomReset(b, 10)
}

func BenchmarkTransactRandomReset__100(b *testing.B) {
	benchTransactRandomReset(b, 100)
}

func BenchmarkTransactRandomReset__1000(b *testing.B) {
	benchTransactRandomReset(b, 1000)
}

// Random facts, persist across iterations. This tests the redundancy checker.
func benchTransactRandomPersistent(b *testing.B, n int) {
	engine, _ := memory.Open(&storage.Options{})

	store, _ := storage.Init(&storage.Config{
		Engine: engine,
	})

	for i := 0; i < b.N; i++ {
		b.StopTimer()

		facts := testutil.RandFacts(n, "test")
		r := fact.NewReader(facts)

		b.StartTimer()

		transactor.Commit(store, r, "test", true)
	}
}

func BenchmarkTransactRandomPersistent__10(b *testing.B) {
	benchTransactRandomPersistent(b, 10)
}

func BenchmarkTransactRandomPersistent__100(b *testing.B) {
	benchTransactRandomPersistent(b, 100)
}

func BenchmarkTransactRandomPersistent__1000(b *testing.B) {
	benchTransactRandomPersistent(b, 1000)
}
