package transactor_test

import (
	"testing"

	"github.com/chop-dbhi/origins/fact"
	"github.com/chop-dbhi/origins/testutil"
	"github.com/chop-dbhi/origins/transactor"
)

// Random facts, reset after each iteration
func benchEvaluateRandomReset(b *testing.B, n int) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()

		store := testutil.Store()

		facts := testutil.RandFacts(n, "test")
		r := fact.NewReader(facts)

		b.StartTimer()

		transactor.Evaluate(store, r, "test", true)
	}
}

func BenchmarkEvaluateRandomReset__10(b *testing.B) {
	benchEvaluateRandomReset(b, 10)
}

func BenchmarkEvaluateRandomReset__100(b *testing.B) {
	benchEvaluateRandomReset(b, 100)
}

func BenchmarkEvaluateRandomReset__1000(b *testing.B) {
	benchEvaluateRandomReset(b, 1000)
}

// Random facts, persist across iterations. This tests the redundancy checker.
func benchEvaluateRandomPersistent(b *testing.B, n int) {
	store := testutil.Store()

	for i := 0; i < b.N; i++ {
		b.StopTimer()

		facts := testutil.RandFacts(n, "test")
		r := fact.NewReader(facts)

		b.StartTimer()

		transactor.Evaluate(store, r, "test", true)
	}
}

func BenchmarkEvaluateRandomPersistent__10(b *testing.B) {
	benchEvaluateRandomPersistent(b, 10)
}

func BenchmarkEvaluateRandomPersistent__100(b *testing.B) {
	benchEvaluateRandomPersistent(b, 100)
}

func BenchmarkEvaluateRandomPersistent__1000(b *testing.B) {
	benchEvaluateRandomPersistent(b, 1000)
}
