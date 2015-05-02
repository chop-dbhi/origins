package memory

import "testing"

func TestEngine(t *testing.T) {
	engine, _ := Open(nil)

	k := "hello"
	v := "world"

	if err := engine.Set(k, []byte(v)); err != nil {
		t.Fatal(err)
	}

	b, err := engine.Get(k)

	if err != nil {
		t.Fatal(err)
	}

	if string(b) != v {
		t.Errorf("memory: expected %s, got %s", v, string(b))
	}

	id, err := engine.Incr("counter")

	if err != nil {
		t.Fatalf("memory: incr error %s", err)
	}

	if id != 1 {
		t.Errorf("memory: expected 1, got %v", id)
	}

	id, err = engine.Incr("counter")

	if err != nil {
		t.Fatalf("memory: incr error %s", err)
	}

	if id != 2 {
		t.Errorf("memory: expected 2, got %v", id)
	}
}

func BenchmarkIncr(b *testing.B) {
	engine, _ := Open(nil)

	for i := 0; i < b.N; i++ {
		engine.Incr("counter")
	}
}
