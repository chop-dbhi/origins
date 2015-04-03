package transactor_test

import (
	"os"
	"testing"

	"github.com/chop-dbhi/origins/fact"
	"github.com/chop-dbhi/origins/storage"
	"github.com/chop-dbhi/origins/testutil"
	"github.com/chop-dbhi/origins/transactor"
	"github.com/stretchr/testify/assert"
)

var facts fact.Facts

func init() {
	// Load initial set of facts.
	f, err := os.Open("../assets/origins.csv")

	if err != nil {
		panic(err)
	}

	defer f.Close()

	r := fact.CSVReader(f)

	facts, err = fact.ReadAll(r)

	if err != nil {
		panic(err)
	}
}

func loadFacts(t *testing.T, store *storage.Store, expected int) {
	r := fact.NewReader(facts)

	domain := "origins"

	res, err := transactor.Commit(store, r, domain, true)

	if err != nil {
		t.Fatal(err)
	}

	if r, ok := res[domain]; ok {
		assert.Equal(t, expected, r.Count)
	}
}

func TestTransact(t *testing.T) {
	store := testutil.Store()

	loadFacts(t, store, 72)
	loadFacts(t, store, 0)
}
