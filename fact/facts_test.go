package fact

import "testing"

func TestConcat(t *testing.T) {
	f1 := generateSet(10, 5, 0.2)
	f2 := generateSet(10, 5, 0.2)
	f3 := generateSet(10, 5, 0.2)

	total := 3 * 10 * 5

	ft := Concat(f1, f2, f3)

	if len(ft) != total {
		t.Errorf("concat: expected %d, got %d", total, len(ft))
	}

	if ft[total-1] == nil {
		t.Errorf("concat: element is nil")
	}
}

func BenchmarkConcat(b *testing.B) {
	b.StopTimer()

	f1 := generateSet(100, 5, 0.2)
	f2 := generateSet(100, 5, 0.2)
	f3 := generateSet(100, 5, 0.2)

	b.StartTimer()

	for i := 0; i < b.N; i++ {
		Concat(f1, f2, f3)
	}
}
