package origins

import (
	"math/rand"
	"strconv"
	"testing"
	"time"
)

// Generates a set of facts consisting of n entities each with m attributes.
// Variability (r) defines how variable the attribute values are.
func generateSet(n, m int, r float32) Facts {
	if r <= 0 || r > 1 {
		panic("sort: variability must be between (0,1]")
	}

	// Fixed time
	t := time.Now().UTC()

	facts := make(Facts, n*m)

	vn := int(float32(n*m) * r)

	l := 0

	for i := 0; i < n; i++ {
		e := &Ident{
			Domain: "test",
			Name:   strconv.Itoa(i),
		}

		// Shuffle order of attributes per entity to give it a bit
		// more permutation.
		for _, j := range rand.Perm(m) {
			facts[l] = &Fact{
				Domain:    "test",
				Operation: Assertion,
				Time:      t,
				Entity:    e,
				Attribute: &Ident{
					Domain: "test",
					Name:   strconv.Itoa(j),
				},
				Value: &Ident{
					Domain: "test",
					Name:   strconv.Itoa(rand.Intn(vn)),
				},
			}

			l += 1
		}
	}

	return facts
}

func benchTimsort(b *testing.B, comp Comparator, n, m int) {
	b.StopTimer()

	// Copy to preserve original set.
	facts := generateSet(n, m, 0.2)
	dest := make(Facts, len(facts))

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		copy(dest, facts)
		b.StartTimer()
		Timsort(dest, comp)
	}
}

// EAVT; 20 x m
func BenchmarkTimsort_EAVT__20_2(b *testing.B) {
	benchTimsort(b, EAVTComparator, 20, 2)
}

func BenchmarkTimsort_EAVT__20_5(b *testing.B) {
	benchTimsort(b, EAVTComparator, 20, 5)
}

func BenchmarkTimsort_EAVT__20_20(b *testing.B) {
	benchTimsort(b, EAVTComparator, 20, 20)
}

func BenchmarkTimsort_EAVT__20_100(b *testing.B) {
	benchTimsort(b, EAVTComparator, 20, 100)
}

// EAVT; 100 x m
func BenchmarkTimsort_EAVT__100_2(b *testing.B) {
	benchTimsort(b, EAVTComparator, 100, 2)
}

func BenchmarkTimsort_EAVT__100_5(b *testing.B) {
	benchTimsort(b, EAVTComparator, 100, 5)
}

func BenchmarkTimsort_EAVT__100_20(b *testing.B) {
	benchTimsort(b, EAVTComparator, 100, 2)
}

func BenchmarkTimsort_EAVT__100_100(b *testing.B) {
	benchTimsort(b, EAVTComparator, 100, 5)
}

// EAVT; 5000 x m
func BenchmarkTimsort_EAVT__5000_2(b *testing.B) {
	benchTimsort(b, EAVTComparator, 5000, 2)
}

func BenchmarkTimsort_EAVT__5000_5(b *testing.B) {
	benchTimsort(b, EAVTComparator, 5000, 5)
}

func BenchmarkTimsort_EAVT__5000_20(b *testing.B) {
	benchTimsort(b, EAVTComparator, 5000, 2)
}

func BenchmarkTimsort_EAVT__5000_100(b *testing.B) {
	benchTimsort(b, EAVTComparator, 5000, 5)
}

// EAVT; 10000 x m
func BenchmarkTimsort_EAVT__10000_2(b *testing.B) {
	benchTimsort(b, EAVTComparator, 10000, 2)
}

func BenchmarkTimsort_EAVT__10000_5(b *testing.B) {
	benchTimsort(b, EAVTComparator, 10000, 5)
}

func BenchmarkTimsort_EAVT__10000_20(b *testing.B) {
	benchTimsort(b, EAVTComparator, 10000, 2)
}

func BenchmarkTimsort_EAVT__10000_100(b *testing.B) {
	benchTimsort(b, EAVTComparator, 10000, 5)
}

// AVET; 20 x m
func BenchmarkTimsort_AVET__20_2(b *testing.B) {
	benchTimsort(b, AVETComparator, 20, 2)
}

func BenchmarkTimsort_AVET__20_5(b *testing.B) {
	benchTimsort(b, AVETComparator, 20, 5)
}

func BenchmarkTimsort_AVET__20_20(b *testing.B) {
	benchTimsort(b, AVETComparator, 20, 20)
}

func BenchmarkTimsort_AVET__20_100(b *testing.B) {
	benchTimsort(b, AVETComparator, 20, 100)
}

// AVET; 100 x m
func BenchmarkTimsort_AVET__100_2(b *testing.B) {
	benchTimsort(b, AVETComparator, 100, 2)
}

func BenchmarkTimsort_AVET__100_5(b *testing.B) {
	benchTimsort(b, AVETComparator, 100, 5)
}

func BenchmarkTimsort_AVET__100_20(b *testing.B) {
	benchTimsort(b, AVETComparator, 100, 2)
}

func BenchmarkTimsort_AVET__100_100(b *testing.B) {
	benchTimsort(b, AVETComparator, 100, 5)
}

// AVET; 5000 x m
func BenchmarkTimsort_AVET__5000_2(b *testing.B) {
	benchTimsort(b, AVETComparator, 5000, 2)
}

func BenchmarkTimsort_AVET__5000_5(b *testing.B) {
	benchTimsort(b, AVETComparator, 5000, 5)
}

func BenchmarkTimsort_AVET__5000_20(b *testing.B) {
	benchTimsort(b, AVETComparator, 5000, 2)
}

func BenchmarkTimsort_AVET__5000_100(b *testing.B) {
	benchTimsort(b, AVETComparator, 5000, 5)
}

// AVET; 10000 x m
func BenchmarkTimsort_AVET__10000_2(b *testing.B) {
	benchTimsort(b, AVETComparator, 10000, 2)
}

func BenchmarkTimsort_AVET__10000_5(b *testing.B) {
	benchTimsort(b, AVETComparator, 10000, 5)
}

func BenchmarkTimsort_AVET__10000_20(b *testing.B) {
	benchTimsort(b, AVETComparator, 10000, 2)
}

func BenchmarkTimsort_AVET__10000_100(b *testing.B) {
	benchTimsort(b, AVETComparator, 10000, 5)
}

// AEVT; 20 x m
func BenchmarkTimsort_AEVT__20_2(b *testing.B) {
	benchTimsort(b, AEVTComparator, 20, 2)
}

func BenchmarkTimsort_AEVT__20_5(b *testing.B) {
	benchTimsort(b, AEVTComparator, 20, 5)
}

func BenchmarkTimsort_AEVT__20_20(b *testing.B) {
	benchTimsort(b, AEVTComparator, 20, 20)
}

func BenchmarkTimsort_AEVT__20_100(b *testing.B) {
	benchTimsort(b, AEVTComparator, 20, 100)
}

// AEVT; 100 x m
func BenchmarkTimsort_AEVT__100_2(b *testing.B) {
	benchTimsort(b, AEVTComparator, 100, 2)
}

func BenchmarkTimsort_AEVT__100_5(b *testing.B) {
	benchTimsort(b, AEVTComparator, 100, 5)
}

func BenchmarkTimsort_AEVT__100_20(b *testing.B) {
	benchTimsort(b, AEVTComparator, 100, 2)
}

func BenchmarkTimsort_AEVT__100_100(b *testing.B) {
	benchTimsort(b, AEVTComparator, 100, 5)
}

// AEVT; 5000 x m
func BenchmarkTimsort_AEVT__5000_2(b *testing.B) {
	benchTimsort(b, AEVTComparator, 5000, 2)
}

func BenchmarkTimsort_AEVT__5000_5(b *testing.B) {
	benchTimsort(b, AEVTComparator, 5000, 5)
}

func BenchmarkTimsort_AEVT__5000_20(b *testing.B) {
	benchTimsort(b, AEVTComparator, 5000, 2)
}

func BenchmarkTimsort_AEVT__5000_100(b *testing.B) {
	benchTimsort(b, AEVTComparator, 5000, 5)
}

// AEVT; 10000 x m
func BenchmarkTimsort_AEVT__10000_2(b *testing.B) {
	benchTimsort(b, AEVTComparator, 10000, 2)
}

func BenchmarkTimsort_AEVT__10000_5(b *testing.B) {
	benchTimsort(b, AEVTComparator, 10000, 5)
}

func BenchmarkTimsort_AEVT__10000_20(b *testing.B) {
	benchTimsort(b, AEVTComparator, 10000, 2)
}

func BenchmarkTimsort_AEVT__10000_100(b *testing.B) {
	benchTimsort(b, AEVTComparator, 10000, 5)
}

// VAET; 20 x m
func BenchmarkTimsort_VAET__20_2(b *testing.B) {
	benchTimsort(b, VAETComparator, 20, 2)
}

func BenchmarkTimsort_VAET__20_5(b *testing.B) {
	benchTimsort(b, VAETComparator, 20, 5)
}

func BenchmarkTimsort_VAET__20_20(b *testing.B) {
	benchTimsort(b, VAETComparator, 20, 20)
}

func BenchmarkTimsort_VAET__20_100(b *testing.B) {
	benchTimsort(b, VAETComparator, 20, 100)
}

// VAET; 100 x m
func BenchmarkTimsort_VAET__100_2(b *testing.B) {
	benchTimsort(b, VAETComparator, 100, 2)
}

func BenchmarkTimsort_VAET__100_5(b *testing.B) {
	benchTimsort(b, VAETComparator, 100, 5)
}

func BenchmarkTimsort_VAET__100_20(b *testing.B) {
	benchTimsort(b, VAETComparator, 100, 2)
}

func BenchmarkTimsort_VAET__100_100(b *testing.B) {
	benchTimsort(b, VAETComparator, 100, 5)
}

// VAET; 5000 x m
func BenchmarkTimsort_VAET__5000_2(b *testing.B) {
	benchTimsort(b, VAETComparator, 5000, 2)
}

func BenchmarkTimsort_VAET__5000_5(b *testing.B) {
	benchTimsort(b, VAETComparator, 5000, 5)
}

func BenchmarkTimsort_VAET__5000_20(b *testing.B) {
	benchTimsort(b, VAETComparator, 5000, 2)
}

func BenchmarkTimsort_VAET__5000_100(b *testing.B) {
	benchTimsort(b, VAETComparator, 5000, 5)
}

// VAET; 10000 x m
func BenchmarkTimsort_VAET__10000_2(b *testing.B) {
	benchTimsort(b, VAETComparator, 10000, 2)
}

func BenchmarkTimsort_VAET__10000_5(b *testing.B) {
	benchTimsort(b, VAETComparator, 10000, 5)
}

func BenchmarkTimsort_VAET__10000_20(b *testing.B) {
	benchTimsort(b, VAETComparator, 10000, 2)
}

func BenchmarkTimsort_VAET__10000_100(b *testing.B) {
	benchTimsort(b, VAETComparator, 10000, 5)
}

// Built-in sort.
func benchSort(b *testing.B, comp Comparator, n, m int) {
	b.StopTimer()

	// Copy to preserve original set.
	facts := generateSet(n, m, 0.2)
	dest := make(Facts, len(facts))

	for i := 0; i < b.N; i++ {
		copy(dest, facts)
		b.StartTimer()
		Sort(dest, comp)
		b.StopTimer()
	}
}

// EAVT; 20 x m
func BenchmarkSort_EAVT__20_2(b *testing.B) {
	benchSort(b, EAVTComparator, 20, 2)
}

func BenchmarkSort_EAVT__20_5(b *testing.B) {
	benchSort(b, EAVTComparator, 20, 5)
}

func BenchmarkSort_EAVT__20_20(b *testing.B) {
	benchSort(b, EAVTComparator, 20, 20)
}

func BenchmarkSort_EAVT__20_100(b *testing.B) {
	benchSort(b, EAVTComparator, 20, 100)
}

// EAVT; 100 x m
func BenchmarkSort_EAVT__100_2(b *testing.B) {
	benchSort(b, EAVTComparator, 100, 2)
}

func BenchmarkSort_EAVT__100_5(b *testing.B) {
	benchSort(b, EAVTComparator, 100, 5)
}

func BenchmarkSort_EAVT__100_20(b *testing.B) {
	benchSort(b, EAVTComparator, 100, 2)
}

func BenchmarkSort_EAVT__100_100(b *testing.B) {
	benchSort(b, EAVTComparator, 100, 5)
}

// EAVT; 5000 x m
func BenchmarkSort_EAVT__5000_2(b *testing.B) {
	benchSort(b, EAVTComparator, 5000, 2)
}

func BenchmarkSort_EAVT__5000_5(b *testing.B) {
	benchSort(b, EAVTComparator, 5000, 5)
}

func BenchmarkSort_EAVT__5000_20(b *testing.B) {
	benchSort(b, EAVTComparator, 5000, 2)
}

func BenchmarkSort_EAVT__5000_100(b *testing.B) {
	benchSort(b, EAVTComparator, 5000, 5)
}

// EAVT; 10000 x m
func BenchmarkSort_EAVT__10000_2(b *testing.B) {
	benchSort(b, EAVTComparator, 10000, 2)
}

func BenchmarkSort_EAVT__10000_5(b *testing.B) {
	benchSort(b, EAVTComparator, 10000, 5)
}

func BenchmarkSort_EAVT__10000_20(b *testing.B) {
	benchSort(b, EAVTComparator, 10000, 2)
}

func BenchmarkSort_EAVT__10000_100(b *testing.B) {
	benchSort(b, EAVTComparator, 10000, 5)
}

// AVET; 20 x m
func BenchmarkSort_AVET__20_2(b *testing.B) {
	benchSort(b, AVETComparator, 20, 2)
}

func BenchmarkSort_AVET__20_5(b *testing.B) {
	benchSort(b, AVETComparator, 20, 5)
}

func BenchmarkSort_AVET__20_20(b *testing.B) {
	benchSort(b, AVETComparator, 20, 20)
}

func BenchmarkSort_AVET__20_100(b *testing.B) {
	benchSort(b, AVETComparator, 20, 100)
}

// AVET; 100 x m
func BenchmarkSort_AVET__100_2(b *testing.B) {
	benchSort(b, AVETComparator, 100, 2)
}

func BenchmarkSort_AVET__100_5(b *testing.B) {
	benchSort(b, AVETComparator, 100, 5)
}

func BenchmarkSort_AVET__100_20(b *testing.B) {
	benchSort(b, AVETComparator, 100, 2)
}

func BenchmarkSort_AVET__100_100(b *testing.B) {
	benchSort(b, AVETComparator, 100, 5)
}

// AVET; 5000 x m
func BenchmarkSort_AVET__5000_2(b *testing.B) {
	benchSort(b, AVETComparator, 5000, 2)
}

func BenchmarkSort_AVET__5000_5(b *testing.B) {
	benchSort(b, AVETComparator, 5000, 5)
}

func BenchmarkSort_AVET__5000_20(b *testing.B) {
	benchSort(b, AVETComparator, 5000, 2)
}

func BenchmarkSort_AVET__5000_100(b *testing.B) {
	benchSort(b, AVETComparator, 5000, 5)
}

// AVET; 10000 x m
func BenchmarkSort_AVET__10000_2(b *testing.B) {
	benchSort(b, AVETComparator, 10000, 2)
}

func BenchmarkSort_AVET__10000_5(b *testing.B) {
	benchSort(b, AVETComparator, 10000, 5)
}

func BenchmarkSort_AVET__10000_20(b *testing.B) {
	benchSort(b, AVETComparator, 10000, 2)
}

func BenchmarkSort_AVET__10000_100(b *testing.B) {
	benchSort(b, AVETComparator, 10000, 5)
}

// AEVT; 20 x m
func BenchmarkSort_AEVT__20_2(b *testing.B) {
	benchSort(b, AEVTComparator, 20, 2)
}

func BenchmarkSort_AEVT__20_5(b *testing.B) {
	benchSort(b, AEVTComparator, 20, 5)
}

func BenchmarkSort_AEVT__20_20(b *testing.B) {
	benchSort(b, AEVTComparator, 20, 20)
}

func BenchmarkSort_AEVT__20_100(b *testing.B) {
	benchSort(b, AEVTComparator, 20, 100)
}

// AEVT; 100 x m
func BenchmarkSort_AEVT__100_2(b *testing.B) {
	benchSort(b, AEVTComparator, 100, 2)
}

func BenchmarkSort_AEVT__100_5(b *testing.B) {
	benchSort(b, AEVTComparator, 100, 5)
}

func BenchmarkSort_AEVT__100_20(b *testing.B) {
	benchSort(b, AEVTComparator, 100, 2)
}

func BenchmarkSort_AEVT__100_100(b *testing.B) {
	benchSort(b, AEVTComparator, 100, 5)
}

// AEVT; 5000 x m
func BenchmarkSort_AEVT__5000_2(b *testing.B) {
	benchSort(b, AEVTComparator, 5000, 2)
}

func BenchmarkSort_AEVT__5000_5(b *testing.B) {
	benchSort(b, AEVTComparator, 5000, 5)
}

func BenchmarkSort_AEVT__5000_20(b *testing.B) {
	benchSort(b, AEVTComparator, 5000, 2)
}

func BenchmarkSort_AEVT__5000_100(b *testing.B) {
	benchSort(b, AEVTComparator, 5000, 5)
}

// AEVT; 10000 x m
func BenchmarkSort_AEVT__10000_2(b *testing.B) {
	benchSort(b, AEVTComparator, 10000, 2)
}

func BenchmarkSort_AEVT__10000_5(b *testing.B) {
	benchSort(b, AEVTComparator, 10000, 5)
}

func BenchmarkSort_AEVT__10000_20(b *testing.B) {
	benchSort(b, AEVTComparator, 10000, 2)
}

func BenchmarkSort_AEVT__10000_100(b *testing.B) {
	benchSort(b, AEVTComparator, 10000, 5)
}

// VAET; 20 x m
func BenchmarkSort_VAET__20_2(b *testing.B) {
	benchSort(b, VAETComparator, 20, 2)
}

func BenchmarkSort_VAET__20_5(b *testing.B) {
	benchSort(b, VAETComparator, 20, 5)
}

func BenchmarkSort_VAET__20_20(b *testing.B) {
	benchSort(b, VAETComparator, 20, 20)
}

func BenchmarkSort_VAET__20_100(b *testing.B) {
	benchSort(b, VAETComparator, 20, 100)
}

// VAET; 100 x m
func BenchmarkSort_VAET__100_2(b *testing.B) {
	benchSort(b, VAETComparator, 100, 2)
}

func BenchmarkSort_VAET__100_5(b *testing.B) {
	benchSort(b, VAETComparator, 100, 5)
}

func BenchmarkSort_VAET__100_20(b *testing.B) {
	benchSort(b, VAETComparator, 100, 2)
}

func BenchmarkSort_VAET__100_100(b *testing.B) {
	benchSort(b, VAETComparator, 100, 5)
}

// VAET; 5000 x m
func BenchmarkSort_VAET__5000_2(b *testing.B) {
	benchSort(b, VAETComparator, 5000, 2)
}

func BenchmarkSort_VAET__5000_5(b *testing.B) {
	benchSort(b, VAETComparator, 5000, 5)
}

func BenchmarkSort_VAET__5000_20(b *testing.B) {
	benchSort(b, VAETComparator, 5000, 2)
}

func BenchmarkSort_VAET__5000_100(b *testing.B) {
	benchSort(b, VAETComparator, 5000, 5)
}

// VAET; 10000 x m
func BenchmarkSort_VAET__10000_2(b *testing.B) {
	benchSort(b, VAETComparator, 10000, 2)
}

func BenchmarkSort_VAET__10000_5(b *testing.B) {
	benchSort(b, VAETComparator, 10000, 5)
}

func BenchmarkSort_VAET__10000_20(b *testing.B) {
	benchSort(b, VAETComparator, 10000, 2)
}

func BenchmarkSort_VAET__10000_100(b *testing.B) {
	benchSort(b, VAETComparator, 10000, 5)
}
