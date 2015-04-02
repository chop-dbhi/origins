// Benchmarks caching one identity as the baseline vs. a bunch of random identities
// in the cache structure.
package identity

import (
	"math/rand"
	"testing"
)

func benchCacheOne(b *testing.B, n int) {
	cache := make(Cache)

	// Pre-populate ids so they do not influence the timing.
	ids := make([]*Ident, n)

	rand.Seed(1337)
	local := string(rand.Int63())

	for i, _ := range ids {
		ids[i] = New("", local)
	}

	var id *Ident

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for j := 0; j < n; j++ {
			id = ids[j]
			cache.Add(id.Domain, id.Local)
		}
	}
}

func benchCacheRandom(b *testing.B, n int) {
	cache := make(Cache)

	// Pre-populate ids so they do not influence the timing.
	ids := make([]*Ident, n)

	rand.Seed(1337)

	for i, _ := range ids {
		ids[i] = New("", string(rand.Int63()))
	}

	var id *Ident

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for j := 0; j < n; j++ {
			id = ids[j]
			cache.Add(id.Domain, id.Local)
		}
	}
}

// One
func BenchmarkCacheOne__100(b *testing.B) {
	benchCacheOne(b, 100)
}

func BenchmarkCacheOne__1000(b *testing.B) {
	benchCacheOne(b, 1000)
}

func BenchmarkCacheOne__10000(b *testing.B) {
	benchCacheOne(b, 10000)
}

func BenchmarkCacheOne__100000(b *testing.B) {
	benchCacheOne(b, 100000)
}

// Random
func BenchmarkCacheRandom__100(b *testing.B) {
	benchCacheRandom(b, 100)
}

func BenchmarkCacheRandom__1000(b *testing.B) {
	benchCacheRandom(b, 1000)
}

func BenchmarkCacheRandom__10000(b *testing.B) {
	benchCacheRandom(b, 10000)
}

func BenchmarkCacheRandom__100000(b *testing.B) {
	benchCacheRandom(b, 100000)
}
