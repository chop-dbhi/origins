package view

import (
	"testing"
	"time"

	"github.com/chop-dbhi/origins"
	"github.com/chop-dbhi/origins/storage"
	"github.com/chop-dbhi/origins/testutil"
	"github.com/chop-dbhi/origins/transactor"
)

// Initializes an in-memory store and generates n transactions each with m
// randomly generated facts.
func randStorage(domain string, n, m int) storage.Engine {
	engine, _ := origins.Init("memory", nil)

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
	log, err := OpenLog(engine, domain, "commit")

	if err != nil {
		t.Fatal(err)
	}

	var (
		i int
		f *origins.Fact
	)

	iter := log.Now()

	for {
		if f = iter.Next(); f == nil {
			break
		}

		i++
	}

	if iter.Err() != nil {
		t.Fatal(iter.Err())
	}

	if i != n*m {
		t.Errorf("expected %d facts, got %d", n*m, i)
	}
}

func TestLogReader(t *testing.T) {
	domain := "test"

	// Transactions
	n := 100

	// Size of write
	m := 100

	engine := randStorage(domain, n, m)

	// Open the commit log.
	log, err := OpenLog(engine, domain, "commit")

	if err != nil {
		t.Fatal(err)
	}

	iter := log.Now()

	facts, err := origins.ReadAll(iter)

	if err != nil {
		t.Fatal(err)
	}

	if len(facts) != n*m {
		t.Errorf("expected %d facts, got %d", n*m, len(facts))
	}
}

func TestLogAsof(t *testing.T) {
	domain := "test"

	// Transactions
	n := 100

	// Size of write
	m := 100

	engine := randStorage(domain, n, m)

	// Open the commit log.
	log, err := OpenLog(engine, domain, "commit")

	// 1 minute before
	max := time.Now().Add(-time.Minute)

	iter := log.Asof(max)

	facts, err := origins.ReadAll(iter)

	if err != nil {
		t.Fatal(err)
	}

	if len(facts) != 0 {
		t.Errorf("expected 0 facts, got %d", len(facts))
	}

	// 1 minute later
	max = time.Now().Add(time.Minute)

	iter = log.Asof(max)

	facts, err = origins.ReadAll(iter)

	if err != nil {
		t.Fatal(err)
	}

	if len(facts) != n*m {
		t.Errorf("expected %d facts, got %d", n*m, len(facts))
	}
}

func TestLogSince(t *testing.T) {
	domain := "test"

	// Transactions
	n := 100

	// Size of write
	m := 100

	engine := randStorage(domain, n, m)

	// Open the commit log.
	log, err := OpenLog(engine, domain, "commit")

	// 1 minute before
	min := time.Now().Add(-time.Minute)

	iter := log.Since(min)

	facts, err := origins.ReadAll(iter)

	if err != nil {
		t.Fatal(err)
	}

	if len(facts) != n*m {
		t.Errorf("expected %d facts, got %d", n*m, len(facts))
	}

	// 1 minute later
	min = time.Now().Add(time.Minute)

	iter = log.Since(min)

	facts, err = origins.ReadAll(iter)

	if err != nil {
		t.Fatal(err)
	}

	if len(facts) != 0 {
		t.Errorf("expected 0 facts, got %d", len(facts))
	}
}
