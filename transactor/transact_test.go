package transactor_test

import (
	"os"
	"strconv"
	"testing"

	"github.com/chop-dbhi/origins/fact"
	"github.com/chop-dbhi/origins/identity"
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

func TestTransaction(t *testing.T) {
	store := testutil.Store()
	tx := transactor.New(store)

	e := identity.New("", "joe")
	a := identity.New("", "likes")
	v := identity.New("", "pizza")

	f := fact.Assert(e, a, v)

	tx.EvaluateFact(f, "test", true)

	assert.Equal(t, "test", f.Domain)
	assert.Equal(t, tx.Time, f.Time)
	assert.Equal(t, "test", e.Domain)
	assert.Equal(t, "test", a.Domain)
	assert.Equal(t, "test", v.Domain)

	f = tx.TxFact("test")

	assert.Equal(t, "origins.domains", f.Domain)
	assert.Equal(t, tx.Time, f.Time)
	assert.Equal(t, "origins.domains", e.Domain)
	assert.Equal(t, "test", e.Local)

	dfacts := tx.DomainFacts("test")

	assert.Equal(t, 2, len(dfacts))
}

func TestDomainMacro(t *testing.T) {
	store := testutil.Store()
	tx := transactor.New(store)

	e := identity.New("origins.macros", "domain")
	a := identity.New("", "createdBy")
	v := identity.New("users", "joe")

	f := fact.Assert(e, a, v)

	tx.EvaluateFact(f, "test", false)

	assert.Equal(t, "origins.domains", f.Domain)
	assert.Equal(t, "origins.domains", e.Domain)
	assert.Equal(t, "test", e.Local)
	assert.Equal(t, "origins.domains", a.Domain)
	assert.Equal(t, "createdBy", a.Local)
	assert.Equal(t, "users", v.Domain)
	assert.Equal(t, "joe", v.Local)
}

func TestTxMacro(t *testing.T) {
	store := testutil.Store()
	tx := transactor.New(store)

	e := identity.New("origins.macros", "tx")
	a := identity.New("", "committedBy")
	v := identity.New("users", "joe")

	f := fact.Assert(e, a, v)

	tx.EvaluateFact(f, "test", false)

	assert.Equal(t, "origins.tx.test", f.Domain)
	assert.Equal(t, "origins.tx.test", e.Domain)
	assert.Equal(t, strconv.FormatInt(tx.Time, 10), e.Local)
	assert.Equal(t, "origins.tx.test", a.Domain)
	assert.Equal(t, "committedBy", a.Local)
	assert.Equal(t, "users", v.Domain)
	assert.Equal(t, "joe", v.Local)
}

func TestNowMacro(t *testing.T) {
	store := testutil.Store()
	tx := transactor.New(store)

	e := identity.New("", "joe")
	a := identity.New("", "joined")
	v := identity.New("origins.macros", "now")

	f := fact.Assert(e, a, v)

	tx.EvaluateFact(f, "test", false)

	assert.Equal(t, "test", f.Domain)
	assert.Equal(t, "test", e.Domain)
	assert.Equal(t, "joe", e.Local)
	assert.Equal(t, "test", a.Domain)
	assert.Equal(t, "joined", a.Local)
	assert.Equal(t, "test", v.Domain)
	assert.Equal(t, strconv.FormatInt(tx.Time, 10), v.Local)
}
