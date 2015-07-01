package test

import (
	"math/rand"
	"testing"

	"github.com/chop-dbhi/origins/storage"
)

var letters = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randSeq(n int) []byte {
	b := make([]byte, n)

	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}

	return b
}

func TestEngine(t *testing.T, n string, e storage.Engine) {
	p := "test"
	k := "hello"
	v := "world"

	if err := e.Set(p, k, []byte(v)); err != nil {
		t.Fatal(err)
	}

	b, err := e.Get(p, k)

	if err != nil {
		t.Fatal(err)
	}

	if string(b) != v {
		t.Errorf("%s: expected %s, got %s", n, v, string(b))
	}

	id, err := e.Incr(p, "counter")

	if err != nil {
		t.Fatalf("%s: incr error %s", n, err)
	}

	if id != 1 {
		t.Errorf("%s: expected 1, got %v", n, id)
	}

	id, err = e.Incr(p, "counter")

	if err != nil {
		t.Fatalf("%s: incr error %s", n, err)
	}

	if id != 2 {
		t.Errorf("%s: expected 2, got %v", n, id)
	}

	if err = e.Delete(p, "counter"); err != nil {
		t.Errorf("%s: %s", n, err)
	}

	if v, err := e.Get(p, "counter"); v != nil {
		t.Errorf("%s: expected delete", n)
	} else if err != nil {
		t.Errorf("%s: %s", n, err)
	}
}

func TestTx(t *testing.T, n string, e storage.Engine) {
	p := "test"
	k := "hello"
	v := "world"

	e.Multi(func(tx storage.Tx) error {
		v = "bill"

		if err := tx.Set(p, k, []byte(v)); err != nil {
			t.Errorf("%s: set failed in tx", n)
		}

		return nil
	})

	// Ensure the value is visible outside of the transaction.
	b, _ := e.Get(p, k)

	if string(b) != v {
		t.Errorf("%s: expected %s, got %s", n, v, string(b))
	}
}

func BenchmarkEngineGet(b *testing.B, n string, e storage.Engine) {
	p := "test"
	k := "data"
	// Mimicking a 1000 facts at 150 bytes each
	v := randSeq(150 * 1000)

	e.Set(p, k, v)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		e.Get(p, k)
	}
}

func BenchmarkEngineSet(b *testing.B, n string, e storage.Engine) {
	p := "test"
	k := "data"
	// Mimicking a 1000 facts at 150 bytes each
	v := randSeq(150 * 1000)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		e.Set(p, k, v)
	}
}

func BenchmarkEngineDelete(b *testing.B, n string, e storage.Engine) {
	p := "test"
	k := "data"
	// Mimicking a 1000 facts at 150 bytes each
	v := randSeq(150 * 1000)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		e.Set(p, k, v)
		b.StartTimer()
		e.Delete(p, k)
	}
}

func BenchmarkEngineIncr(b *testing.B, n string, e storage.Engine) {
	p := "test"
	k := "counter"

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		e.Incr(p, k)
	}
}

func BenchmarkTxGet(b *testing.B, n string, e storage.Engine) {
	p := "test"
	k := "data"
	// Mimicking a 1000 facts at 150 bytes each
	v := randSeq(150 * 1000)

	e.Multi(func(tx storage.Tx) error {
		tx.Set(p, k, v)

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			tx.Get(p, k)
		}

		return nil
	})
}

func BenchmarkTxSet(b *testing.B, n string, e storage.Engine) {
	p := "test"
	k := "data"
	// Mimicking a 1000 facts at 150 bytes each
	v := randSeq(150 * 1000)

	e.Multi(func(tx storage.Tx) error {
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			tx.Set(p, k, v)
		}

		return nil
	})
}

func BenchmarkTxDelete(b *testing.B, n string, e storage.Engine) {
	p := "test"
	k := "data"
	// Mimicking a 1000 facts at 150 bytes each
	v := randSeq(150 * 1000)

	e.Multi(func(tx storage.Tx) error {
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			b.StopTimer()
			tx.Set(p, k, v)

			b.StartTimer()
			tx.Delete(p, k)
		}

		return nil
	})
}

func BenchmarkTxIncr(b *testing.B, n string, e storage.Engine) {
	p := "test"
	k := "counter"

	e.Multi(func(tx storage.Tx) error {
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			tx.Incr(p, k)
		}

		return nil
	})
}
