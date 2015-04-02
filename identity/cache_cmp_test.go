// Mimic initializing then comparing a set ids. This workflow would
// be similar to reading in raw data (e.g. CSV file or disk) and converting
// them into structures. The second step is to match ids which mimics
// operations like filtering and aggregation.
package identity

import "testing"

func benchStringNoCache(b *testing.B, n int) {
	ids := make([]*Ident, n)

	for i := 0; i < b.N; i++ {
		for j := 0; j < n; j++ {
			if j%5 == 0 {
				ids[j] = New("", "b")
			} else {
				ids[j] = New("", "a")
			}
		}

		for j := 0; j < n; j++ {
			if ids[j].Domain == "" && ids[j].Local == "a" {
				continue
			}
		}
	}
}

func benchPtrCache(b *testing.B, n int) {
	ids := make([]*Ident, n)
	cache := make(Cache)

	for i := 0; i < b.N; i++ {
		for j := 0; j < n; j++ {
			if j%5 == 0 {
				ids[j] = cache.Add("", "b")
			} else {
				ids[j] = cache.Add("", "a")
			}
		}

		var id *Ident

		for j := 0; j < n; j++ {
			id = cache.Add("", "a")

			if ids[j] == id {
				continue
			}
		}
	}
}

// String + no cache
func BenchmarkStringNoCache__100(b *testing.B) {
	benchStringNoCache(b, 100)
}

func BenchmarkStringNoCache__1000(b *testing.B) {
	benchStringNoCache(b, 1000)
}

func BenchmarkStringNoCache__10000(b *testing.B) {
	benchStringNoCache(b, 10000)
}

func BenchmarkStringNoCache__100000(b *testing.B) {
	benchStringNoCache(b, 100000)
}

// Pointer + cache
func BenchmarkPtrCache__100(b *testing.B) {
	benchPtrCache(b, 100)
}

func BenchmarkPtrCache__1000(b *testing.B) {
	benchPtrCache(b, 1000)
}

func BenchmarkPtrCache__10000(b *testing.B) {
	benchPtrCache(b, 10000)
}

func BenchmarkPtrCache__100000(b *testing.B) {
	benchPtrCache(b, 100000)
}
