package boltdb

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/chop-dbhi/origins/storage"
)

func TestEngine(t *testing.T) {
	f, _ := ioutil.TempFile("", "")

	defer func() {
		os.Remove(f.Name())
	}()

	engine, err := Open(&storage.Options{
		Path: f.Name(),
	})

	if err != nil {
		t.Fatal(err)
	}

	// Close handle
	f.Close()

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
		t.Errorf("boltdb: expected %s, got %s", v, string(b))
	}

	id, err := engine.Incr("counter")

	if err != nil {
		t.Fatalf("boltdb: incr error %s", err)
	}

	if id != 1 {
		t.Errorf("boltdb: expected 1, got %v", id)
	}

	id, err = engine.Incr("counter")

	if err != nil {
		t.Fatalf("boltdb: incr error %s", err)
	}

	if id != 2 {
		t.Errorf("boltdb: expected 2, got %v", id)
	}
}

func BenchmarkIncr(b *testing.B) {
	f, _ := ioutil.TempFile("", "")

	defer func() {
		os.Remove(f.Name())
	}()

	engine, err := Open(&storage.Options{
		Path: f.Name(),
	})

	if err != nil {
		b.Fatal(err)
	}

	// Close handle
	f.Close()

	for i := 0; i < b.N; i++ {
		engine.Incr("counter")
	}
}
