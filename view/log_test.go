package view_test

import (
	"strconv"
	"testing"
	"time"

	"github.com/chop-dbhi/origins"
	"github.com/chop-dbhi/origins/storage"
	"github.com/chop-dbhi/origins/testutil"
	"github.com/chop-dbhi/origins/transactor"
	"github.com/chop-dbhi/origins/view"
)

// Initializes an in-memory store and generates n transactions each with m
// randomly generated facts.
func randStorage(domain string, n, m int) storage.Engine {
	engine, _ := origins.Init("memory", nil)

	for i := 0; i < m; i++ {
		tx, _ := transactor.New(engine, transactor.Options{
			AllowDuplicates: true,
		})

		gen := testutil.NewRandGenerator(domain, tx.ID, n)

		origins.Copy(gen, tx)
		tx.Commit()
	}

	return engine
}

// Initializes an in-memory store and generates n transactions each with m
// randomly generated facts that belong to one of the specified domains.
func randMultidomainStorage(domains []string, n, m int) storage.Engine {
	engine, _ := origins.Init("memory", nil)

	for i := 0; i < m; i++ {
		tx, _ := transactor.New(engine, transactor.Options{
			AllowDuplicates: true,
		})

		gen := testutil.NewMultidomainGenerator(domains, tx.ID, n)

		origins.Copy(gen, tx)
		tx.Commit()
	}

	return engine
}

// Initializes an in-memory store and generates n transactions each with m
// facts randomly generated from the same dictionary of possible E, A, V values.
// Varying the size of the dictionary relative to the size of the store
// allows to guarantee repeating facts.
func randStorageWRepeats(domain string, n, m, eLen, aLen, vLen int) storage.Engine {
	engine, _ := origins.Init("memory", nil)

	dictionary := testutil.NewEAVDictionary(eLen, aLen, vLen)

	for i := 0; i < m; i++ {
		tx, _ := transactor.New(engine, transactor.Options{
			AllowDuplicates: true,
		})

		gen := testutil.NewDictionaryBasedGenerator(dictionary, domain, tx.ID, n)

		origins.Copy(gen, tx)
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
	log, err := view.OpenLog(engine, domain, "commit")

	if err != nil {
		t.Fatal(err)
	}

	num, err := testNext(log.Now())

	if err != nil {
		t.Fatal(err)
	}

	if num != n*m {
		t.Errorf("expected %d facts, got %d", n*m, num)
	}
}

func TestMultiDomainLogIter(t *testing.T) {
	domains := []string{"test", "another_test"}

	// Number of transactions
	n := 100

	// Number of facts per transaction
	m := 100

	engine := randMultidomainStorage(domains, n, m)

	// Open and merge the commit logs.

	log0, err := view.OpenLog(engine, domains[0], "commit")
	if err != nil {
		t.Fatal(err)
	}

	log1, err := view.OpenLog(engine, domains[1], "commit")

	if err != nil {
		t.Fatal(err)
	}

	now := time.Now()
	mergedStreams := view.Merge(log0.Asof(now), log1.Asof(now))

	if err = mergedStreams.Err(); err != nil {
		t.Fatal(err)
	}

	i := 0
	var prevTrnId uint64 = 1<<64 - 1
	for {
		f := mergedStreams.Next()

		if err = mergedStreams.Err(); err != nil {
			t.Fatal(err)
		}

		if f == nil {
			break
		} else {
			if prevTrnId < f.Transaction {
				t.Errorf("Transactions are ordered incorrectly")
			}
			i++
		}
	}

	if i != n*m {
		t.Errorf("expected %d facts, got %d", n*m, i)
	}
}

// Test the Next() method for the given iterator.
// Return the number of elements encountered.
func testNext(iter origins.Iterator) (int, error) {
	var (
		i int
		f *origins.Fact
	)

	for {
		if f = iter.Next(); f == nil {
			break
		}

		i++
	}

	if err := iter.Err(); err != nil {
		return 0, err
	}

	return i, nil
}

func TestLogExcludeDuplicates(t *testing.T) {
	domain := "test"

	// number of transactions
	n := 100

	// number of facts per transaction
	m := 100

	eLen, aLen, vLen := 2, 3, 4

	engine := randStorageWRepeats(domain, n, m, eLen, aLen, vLen)

	// Open the commit log.
	log, err := view.OpenLog(engine, domain, "commit")

	if err != nil {
		t.Fatal(err)
	}

	// first check the total number of facts
	num, err := testNext(log.Now())

	if err != nil {
		t.Fatal(err)
	}

	if num != n*m {
		t.Errorf("expected %d total facts, got %d", n*m, num)
	}

	// Now check that Next() works on the deduplicated stream,
	// and verify the number of unique facts.
	iter := view.Deduplicate(log.Now())

	if err := iter.Err(); err != nil {
		t.Fatal(err)
	}

	num, err = testNext(iter)

	if err != nil {
		t.Fatal(err)
	}

	// With the dictionary size being very small (e.g. 24) compared to the number of generated facts (e.g. 10,000),
	// the probability that any of the possible facts didn't get generated is negligible
	if num != eLen*aLen*vLen {
		t.Errorf("expected %d unique facts, got %d", eLen*aLen*vLen, num)
	}
}

func benchmarkDeduplication(b *testing.B, numTrn, factsPerTrn, eLen, aLen, vLen int) {
	domain := "test"

	engine := randStorageWRepeats(domain, numTrn, factsPerTrn, eLen, aLen, vLen)

	log, err := view.OpenLog(engine, domain, "commit")

	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {

		now := log.Now()

		iter := view.Deduplicate(now)

		if err = iter.Err(); err != nil {
			b.Fatal(err)
		}

		// The Deduplicate() operation is lazy, and most of the actual work happens during Next(),
		// so to evaluate the true cost of deduplication we need to time how long it takes to step
		// through the resulting iterator.
		_, err = testNext(iter)

		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDeduplication(b *testing.B) {
	benchmarkDeduplication(b, 100, 100, 3, 4, 5)
}

func benchmarkDomainMerge(b *testing.B, numTrn, factsPerTrn, numDomains int) {
	var err error

	domains := make([]string, numDomains)
	iters := make([]origins.Iterator, len(domains))
	logs := make([]*view.Log, len(domains))

	for j := 0; j < numDomains; j++ {
		domains[j] = "domain_" + strconv.Itoa(j+1)
	}

	engine := randMultidomainStorage(domains, numTrn, factsPerTrn)

	for j := 0; j < len(domains); j++ {
		logs[j], err = view.OpenLog(engine, domains[j], "commit")

		if err != nil {
			b.Fatal(err)
		}
	}

	now := time.Now()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for j := 0; j < len(domains); j++ {
			iters[j] = logs[j].Asof(now)
		}

		iter := view.Merge(iters...)

		if err := iter.Err(); err != nil {
			b.Fatal(err)
		}

		// The Merge() operation is lazy, and most of the actual work happens during Next(),
		// so to evaluate the true cost of merging we need to time how long it takes to step
		// through the resulting iterator.
		_, err = testNext(iter)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDomainMerge(b *testing.B) {
	benchmarkDomainMerge(b, 100, 100, 3)
}

func TestLogReader(t *testing.T) {
	domain := "test"

	// Transactions
	n := 100

	// Size of write
	m := 100

	engine := randStorage(domain, n, m)

	// Open the commit log.
	log, err := view.OpenLog(engine, domain, "commit")

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
	log, err := view.OpenLog(engine, domain, "commit")

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
	log, err := view.OpenLog(engine, domain, "commit")

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
