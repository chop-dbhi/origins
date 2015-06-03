package view

import (
	"io"
	"testing"

	"github.com/chop-dbhi/origins"
	"github.com/chop-dbhi/origins/storage"
	"github.com/chop-dbhi/origins/testutil"
	"github.com/chop-dbhi/origins/transactor"
)

// Initializes an in-memory store and generates n transactions each with m
// randomly generated facts.
func randStorage(domain string, n, m int) storage.Engine {
	engine, _ := origins.Open("memory", nil)

	for i := 0; i < m; i++ {
		tx, _ := transactor.New(engine, transactor.DefaultOptions)

		gen := testutil.NewRandGenerator(domain, tx.ID, n)

		tx.AppendIter(gen)
		tx.Commit()
	}

	return engine
}

func TestLogIter(t *testing.T) {
	domain := "test"

	// Transactions
	n := 100

	// Size of write
	m := 100

	engine := randStorage(domain, n, m)

	// Open the commit log.
	log, err := OpenLog(engine, domain, "log.commit")

	if err != nil {
		t.Fatal(err)
	}

	var i int

	iter := log.Iter()

	for {
		f, err := iter.Next()

		if f != nil {
			i++
		}

		if err != nil {
			if err == io.EOF {
				break
			}

			t.Fatal(err)
		}
	}

	if i != n*m {
		t.Errorf("expected %d facts, got %d", n, n*m)
	}
}
