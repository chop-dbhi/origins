package fact

import "testing"

func benchTimsort(b *testing.B, comp Comparator, n, m int) {
	b.StopTimer()

	// Copy to preserve original set.
	facts := generateSet(n, m, 0.2)
	dest := make(Facts, len(facts))

	for i := 0; i < b.N; i++ {
		copy(dest, facts)
		b.StartTimer()
		TimsortBy(comp, dest)
		b.StopTimer()
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

func benchTimsortConcatSort(b *testing.B, comp Comparator, n, m int) {
	b.StopTimer()

	f1 := generateSet(n, m, 0.2)
	f2 := generateSet(n, m, 0.2)
	f3 := generateSet(n, m, 0.2)
	f4 := generateSet(n, m, 0.2)
	f5 := generateSet(n, m, 0.2)

	b.StartTimer()

	for i := 0; i < b.N; i++ {
		facts := Concat(f1, f2, f3, f4, f5)
		TimsortBy(comp, facts)
	}
}

func BenchmarkTimsort_EAVT_ConcatSort__500_10(b *testing.B) {
	benchTimsortConcatSort(b, EAVTComparator, 500, 10)
}

func BenchmarkTimsort_AVET_ConcatSort__500_10(b *testing.B) {
	benchTimsortConcatSort(b, AVETComparator, 500, 10)
}

func BenchmarkTimsort_AEVT_ConcatSort__500_10(b *testing.B) {
	benchTimsortConcatSort(b, AEVTComparator, 500, 10)
}

func BenchmarkTimsort_VAET_ConcatSort__500_10(b *testing.B) {
	benchTimsortConcatSort(b, VAETComparator, 500, 10)
}
