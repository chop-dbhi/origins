package test

import (
	"testing"

	"github.com/chop-dbhi/origins/engine"
)

func TestEngine(t *testing.T, n string, e engine.Engine) {
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
}

func TestTx(t *testing.T, n string, e engine.Engine) {
	p := "test"
	k := "hello"
	v := "world"

	e.Multi(func(tx engine.Tx) error {
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

func BenchmarkEngineIncr(b *testing.B, n string, e engine.Engine) {
	p := "test"
	k := "counter"

	for i := 0; i < b.N; i++ {
		e.Incr(p, k)
	}
}

func BenchmarkTxIncr(b *testing.B, n string, e engine.Engine) {
	p := "test"
	k := "counter"

	e.Multi(func(tx engine.Tx) error {
		for i := 0; i < b.N; i++ {
			tx.Incr(p, k)
		}

		return nil
	})
}
