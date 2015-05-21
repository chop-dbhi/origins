package origins

import "testing"

func benchTimsort(b *testing.B, comp Comparator, n, m int) {
	b.StopTimer()

	// Copy to preserve original set.
	facts := generateSet(n, m, 0.2)
	dest := make(Facts, len(facts))

	for i := 0; i < b.N; i++ {
		copy(dest, facts)
		b.StartTimer()
		Timsort(dest, comp)
		b.StopTimer()
	}
}

// EAVT; 20 x m
func BenchmarkTimsort_EAVT__20_2(b *testing.B) {
	benchTimsort(b, eavtComparator, 20, 2)
}

func BenchmarkTimsort_EAVT__20_5(b *testing.B) {
	benchTimsort(b, eavtComparator, 20, 5)
}

func BenchmarkTimsort_EAVT__20_20(b *testing.B) {
	benchTimsort(b, eavtComparator, 20, 20)
}

func BenchmarkTimsort_EAVT__20_100(b *testing.B) {
	benchTimsort(b, eavtComparator, 20, 100)
}

// EAVT; 100 x m
func BenchmarkTimsort_EAVT__100_2(b *testing.B) {
	benchTimsort(b, eavtComparator, 100, 2)
}

func BenchmarkTimsort_EAVT__100_5(b *testing.B) {
	benchTimsort(b, eavtComparator, 100, 5)
}

func BenchmarkTimsort_EAVT__100_20(b *testing.B) {
	benchTimsort(b, eavtComparator, 100, 2)
}

func BenchmarkTimsort_EAVT__100_100(b *testing.B) {
	benchTimsort(b, eavtComparator, 100, 5)
}

// EAVT; 5000 x m
func BenchmarkTimsort_EAVT__5000_2(b *testing.B) {
	benchTimsort(b, eavtComparator, 5000, 2)
}

func BenchmarkTimsort_EAVT__5000_5(b *testing.B) {
	benchTimsort(b, eavtComparator, 5000, 5)
}

func BenchmarkTimsort_EAVT__5000_20(b *testing.B) {
	benchTimsort(b, eavtComparator, 5000, 2)
}

func BenchmarkTimsort_EAVT__5000_100(b *testing.B) {
	benchTimsort(b, eavtComparator, 5000, 5)
}

// EAVT; 10000 x m
func BenchmarkTimsort_EAVT__10000_2(b *testing.B) {
	benchTimsort(b, eavtComparator, 10000, 2)
}

func BenchmarkTimsort_EAVT__10000_5(b *testing.B) {
	benchTimsort(b, eavtComparator, 10000, 5)
}

func BenchmarkTimsort_EAVT__10000_20(b *testing.B) {
	benchTimsort(b, eavtComparator, 10000, 2)
}

func BenchmarkTimsort_EAVT__10000_100(b *testing.B) {
	benchTimsort(b, eavtComparator, 10000, 5)
}

// AVET; 20 x m
func BenchmarkTimsort_AVET__20_2(b *testing.B) {
	benchTimsort(b, avetComparator, 20, 2)
}

func BenchmarkTimsort_AVET__20_5(b *testing.B) {
	benchTimsort(b, avetComparator, 20, 5)
}

func BenchmarkTimsort_AVET__20_20(b *testing.B) {
	benchTimsort(b, avetComparator, 20, 20)
}

func BenchmarkTimsort_AVET__20_100(b *testing.B) {
	benchTimsort(b, avetComparator, 20, 100)
}

// AVET; 100 x m
func BenchmarkTimsort_AVET__100_2(b *testing.B) {
	benchTimsort(b, avetComparator, 100, 2)
}

func BenchmarkTimsort_AVET__100_5(b *testing.B) {
	benchTimsort(b, avetComparator, 100, 5)
}

func BenchmarkTimsort_AVET__100_20(b *testing.B) {
	benchTimsort(b, avetComparator, 100, 2)
}

func BenchmarkTimsort_AVET__100_100(b *testing.B) {
	benchTimsort(b, avetComparator, 100, 5)
}

// AVET; 5000 x m
func BenchmarkTimsort_AVET__5000_2(b *testing.B) {
	benchTimsort(b, avetComparator, 5000, 2)
}

func BenchmarkTimsort_AVET__5000_5(b *testing.B) {
	benchTimsort(b, avetComparator, 5000, 5)
}

func BenchmarkTimsort_AVET__5000_20(b *testing.B) {
	benchTimsort(b, avetComparator, 5000, 2)
}

func BenchmarkTimsort_AVET__5000_100(b *testing.B) {
	benchTimsort(b, avetComparator, 5000, 5)
}

// AVET; 10000 x m
func BenchmarkTimsort_AVET__10000_2(b *testing.B) {
	benchTimsort(b, avetComparator, 10000, 2)
}

func BenchmarkTimsort_AVET__10000_5(b *testing.B) {
	benchTimsort(b, avetComparator, 10000, 5)
}

func BenchmarkTimsort_AVET__10000_20(b *testing.B) {
	benchTimsort(b, avetComparator, 10000, 2)
}

func BenchmarkTimsort_AVET__10000_100(b *testing.B) {
	benchTimsort(b, avetComparator, 10000, 5)
}

// AEVT; 20 x m
func BenchmarkTimsort_AEVT__20_2(b *testing.B) {
	benchTimsort(b, aevtComparator, 20, 2)
}

func BenchmarkTimsort_AEVT__20_5(b *testing.B) {
	benchTimsort(b, aevtComparator, 20, 5)
}

func BenchmarkTimsort_AEVT__20_20(b *testing.B) {
	benchTimsort(b, aevtComparator, 20, 20)
}

func BenchmarkTimsort_AEVT__20_100(b *testing.B) {
	benchTimsort(b, aevtComparator, 20, 100)
}

// AEVT; 100 x m
func BenchmarkTimsort_AEVT__100_2(b *testing.B) {
	benchTimsort(b, aevtComparator, 100, 2)
}

func BenchmarkTimsort_AEVT__100_5(b *testing.B) {
	benchTimsort(b, aevtComparator, 100, 5)
}

func BenchmarkTimsort_AEVT__100_20(b *testing.B) {
	benchTimsort(b, aevtComparator, 100, 2)
}

func BenchmarkTimsort_AEVT__100_100(b *testing.B) {
	benchTimsort(b, aevtComparator, 100, 5)
}

// AEVT; 5000 x m
func BenchmarkTimsort_AEVT__5000_2(b *testing.B) {
	benchTimsort(b, aevtComparator, 5000, 2)
}

func BenchmarkTimsort_AEVT__5000_5(b *testing.B) {
	benchTimsort(b, aevtComparator, 5000, 5)
}

func BenchmarkTimsort_AEVT__5000_20(b *testing.B) {
	benchTimsort(b, aevtComparator, 5000, 2)
}

func BenchmarkTimsort_AEVT__5000_100(b *testing.B) {
	benchTimsort(b, aevtComparator, 5000, 5)
}

// AEVT; 10000 x m
func BenchmarkTimsort_AEVT__10000_2(b *testing.B) {
	benchTimsort(b, aevtComparator, 10000, 2)
}

func BenchmarkTimsort_AEVT__10000_5(b *testing.B) {
	benchTimsort(b, aevtComparator, 10000, 5)
}

func BenchmarkTimsort_AEVT__10000_20(b *testing.B) {
	benchTimsort(b, aevtComparator, 10000, 2)
}

func BenchmarkTimsort_AEVT__10000_100(b *testing.B) {
	benchTimsort(b, aevtComparator, 10000, 5)
}

// VAET; 20 x m
func BenchmarkTimsort_VAET__20_2(b *testing.B) {
	benchTimsort(b, vaetComparator, 20, 2)
}

func BenchmarkTimsort_VAET__20_5(b *testing.B) {
	benchTimsort(b, vaetComparator, 20, 5)
}

func BenchmarkTimsort_VAET__20_20(b *testing.B) {
	benchTimsort(b, vaetComparator, 20, 20)
}

func BenchmarkTimsort_VAET__20_100(b *testing.B) {
	benchTimsort(b, vaetComparator, 20, 100)
}

// VAET; 100 x m
func BenchmarkTimsort_VAET__100_2(b *testing.B) {
	benchTimsort(b, vaetComparator, 100, 2)
}

func BenchmarkTimsort_VAET__100_5(b *testing.B) {
	benchTimsort(b, vaetComparator, 100, 5)
}

func BenchmarkTimsort_VAET__100_20(b *testing.B) {
	benchTimsort(b, vaetComparator, 100, 2)
}

func BenchmarkTimsort_VAET__100_100(b *testing.B) {
	benchTimsort(b, vaetComparator, 100, 5)
}

// VAET; 5000 x m
func BenchmarkTimsort_VAET__5000_2(b *testing.B) {
	benchTimsort(b, vaetComparator, 5000, 2)
}

func BenchmarkTimsort_VAET__5000_5(b *testing.B) {
	benchTimsort(b, vaetComparator, 5000, 5)
}

func BenchmarkTimsort_VAET__5000_20(b *testing.B) {
	benchTimsort(b, vaetComparator, 5000, 2)
}

func BenchmarkTimsort_VAET__5000_100(b *testing.B) {
	benchTimsort(b, vaetComparator, 5000, 5)
}

// VAET; 10000 x m
func BenchmarkTimsort_VAET__10000_2(b *testing.B) {
	benchTimsort(b, vaetComparator, 10000, 2)
}

func BenchmarkTimsort_VAET__10000_5(b *testing.B) {
	benchTimsort(b, vaetComparator, 10000, 5)
}

func BenchmarkTimsort_VAET__10000_20(b *testing.B) {
	benchTimsort(b, vaetComparator, 10000, 2)
}

func BenchmarkTimsort_VAET__10000_100(b *testing.B) {
	benchTimsort(b, vaetComparator, 10000, 5)
}
