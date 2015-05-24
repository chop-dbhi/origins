// Benchmark to measure the overhead of using a channel when communicating facts.
// The outcome of this evaluation is to decide if a channel-based transaction
// router can be implemented efficiently.
package transactor

import "testing"

func BenchmarkLoopFacts(b *testing.B) {
	gen := newRandGenerator("test", 1, 0)

	for i := 0; i < b.N; i++ {
		gen.Next()
	}
}

func BenchmarkChanFacts(b *testing.B) {
	gen := newRandGenerator("test", 1, 0)

	closer := make(chan struct{})
	fch, _ := gen.Subscribe(closer)

	for i := 0; i < b.N; i++ {
		<-fch
	}

	// Clean up
	close(closer)
}
