package transactor

import (
	"fmt"
	"sync"
	"testing"

	"github.com/chop-dbhi/origins"
	"github.com/chop-dbhi/origins/dal"
	"github.com/chop-dbhi/origins/storage"
	"github.com/chop-dbhi/origins/testutil"
	"github.com/stretchr/testify/assert"
)

func checkCommitted(t *testing.T, engine storage.Engine, domain string, id uint64) *dal.Log {
	log, err := dal.GetLog(engine, domain, "commit")

	if err != nil {
		t.Fatal(err)
	} else if log == nil {
		t.Fatalf("transactor: %s log does not exist", "commit")
	}

	seg, err := dal.GetSegment(engine, domain, log.Head)

	if err != nil {
		t.Fatal(err)
	}

	if seg.Transaction != id {
		t.Errorf("expected %d, got %d", id, seg.Transaction)
	}

	return log
}

func checkCanceled(t *testing.T, engine storage.Engine, domain string, id uint64) {
	log, err := dal.GetLog(engine, domain, "commit")

	if err != nil {
		t.Fatal(err)
	}

	if log != nil {
		t.Error("transact: log should not exist")
	}
}

func TestCommit(t *testing.T) {
	engine, _ := origins.Init("mem", nil)

	domain := "test"

	tx, _ := New(engine, DefaultOptions)

	gen := testutil.NewRandGenerator(domain, tx.ID, 500)

	origins.Copy(gen, tx)
	tx.Commit()

	checkCommitted(t, engine, domain, tx.ID)
}

func TestCancel(t *testing.T) {
	engine, _ := origins.Init("mem", nil)

	domain := "test"

	tx, _ := New(engine, DefaultOptions)

	gen := testutil.NewRandGenerator("test", tx.ID, 2000)

	origins.Copy(gen, tx)
	tx.Cancel()

	checkCanceled(t, engine, domain, tx.ID)
}

func TestMultiple(t *testing.T) {
	engine, _ := origins.Init("mem", nil)

	domain := "test"

	for i := 0; i < 5; i++ {
		tx, _ := New(engine, DefaultOptions)

		gen := testutil.NewRandGenerator(domain, tx.ID, 5000)

		origins.Copy(gen, tx)
		tx.Commit()

		checkCommitted(t, engine, domain, tx.ID)
	}
}

func TestMultipleDomains(t *testing.T) {
	engine, _ := origins.Init("mem", nil)

	tx, _ := New(engine, DefaultOptions)

	wg := sync.WaitGroup{}
	wg.Add(5)

	for i := 0; i < 5; i++ {
		go func(i int) {
			domain := fmt.Sprintf("test.%d", i)
			gen := testutil.NewRandGenerator(domain, tx.ID, 10000)
			origins.Copy(gen, tx)
			wg.Done()
		}(i)
	}

	wg.Wait()

	tx.Commit()

	for i := 0; i < 5; i++ {
		domain := fmt.Sprintf("test.%d", i)
		checkCommitted(t, engine, domain, tx.ID)
	}
}

func TestRedundant(t *testing.T) {
	engine, _ := origins.Init("mem", nil)

	domain := "test"

	tx1, _ := New(engine, DefaultOptions)

	// Materialize a set of facts to be repeated.
	facts, _ := origins.ReadAll(testutil.NewRandGenerator(domain, tx1.ID, 500))
	buf := origins.NewBuffer(facts)

	origins.Copy(buf, tx1)

	tx1.Commit()
	l1 := checkCommitted(t, engine, domain, tx1.ID)

	tx2, _ := New(engine, DefaultOptions)
	buf = origins.NewBuffer(facts)

	origins.Copy(buf, tx2)

	tx2.Commit()

	// Using tx1 to check committed since the segment should not have
	// changed.
	l2 := checkCommitted(t, engine, domain, tx1.ID)

	assert.Equal(t, l1.Head, l2.Head)
}

func benchTransaction(b *testing.B, n int, m int) {
	b.StopTimer()

	engine, _ := origins.Init("mem", nil)

	var (
		wg sync.WaitGroup
		tx *Transaction
	)

	for i := 0; i < b.N; i++ {
		b.StopTimer()

		wg = sync.WaitGroup{}
		wg.Add(m)

		tx, _ = New(engine, DefaultOptions)

		b.StartTimer()

		for j := 0; j < m; j++ {
			go func(j int) {
				domain := fmt.Sprintf("test.%d", j)
				gen := testutil.NewRandGenerator(domain, tx.ID, n)
				origins.Copy(gen, tx)
				wg.Done()
			}(j)
		}

		wg.Wait()
		tx.Commit()
	}
}

// 1,000
func BenchmarkTransaction__100_10(b *testing.B) {
	benchTransaction(b, 100, 10)
}

func BenchmarkTransaction__1000_1(b *testing.B) {
	benchTransaction(b, 1000, 1)
}

// 10,000
func BenchmarkTransaction__1000_10(b *testing.B) {
	benchTransaction(b, 100, 10)
}

func BenchmarkTransaction__10000_1(b *testing.B) {
	benchTransaction(b, 1000, 1)
}

// 50,000
func BenchmarkTransaction__10000_5(b *testing.B) {
	benchTransaction(b, 10000, 5)
}

func BenchmarkTransaction__50000_1(b *testing.B) {
	benchTransaction(b, 50000, 1)
}

// 1,000,000
func BenchmarkTransaction__100000_10(b *testing.B) {
	benchTransaction(b, 100000, 10)
}

func BenchmarkTransaction__1000000_1(b *testing.B) {
	benchTransaction(b, 1000000, 1)
}
