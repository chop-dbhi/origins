package boltdb

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/chop-dbhi/origins/storage"
	"github.com/chop-dbhi/origins/storage/test"
)

func TestEngine(t *testing.T) {
	f, _ := ioutil.TempFile("", "")

	defer func() {
		os.Remove(f.Name())
	}()

	e, err := Init(storage.Options{
		"path": f.Name(),
	})

	if err != nil {
		t.Fatal(err)
	}

	f.Close()

	test.TestEngine(t, "boltdb", e)
}

func TestTx(t *testing.T) {
	f, _ := ioutil.TempFile("", "")

	defer func() {
		os.Remove(f.Name())
	}()

	e, err := Init(storage.Options{
		"path": f.Name(),
	})

	if err != nil {
		t.Fatal(err)
	}

	f.Close()

	test.TestTx(t, "boltdb", e)
}

func BenchmarkEngineIncr(b *testing.B) {
	f, _ := ioutil.TempFile("", "")

	defer func() {
		os.Remove(f.Name())
	}()

	e, err := Init(storage.Options{
		"path": f.Name(),
	})

	if err != nil {
		b.Fatal(err)
	}

	f.Close()

	test.BenchmarkEngineIncr(b, "boltdb", e)
}

func BenchmarkTxIncr(b *testing.B) {
	f, _ := ioutil.TempFile("", "")

	defer func() {
		os.Remove(f.Name())
	}()

	e, err := Init(storage.Options{
		"path": f.Name(),
	})

	if err != nil {
		b.Fatal(err)
	}

	f.Close()

	test.BenchmarkTxIncr(b, "boltdb", e)
}
