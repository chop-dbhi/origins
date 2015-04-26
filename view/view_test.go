package view_test

import (
	"testing"
	"time"

	"github.com/chop-dbhi/origins"
	"github.com/chop-dbhi/origins/fact"
	"github.com/chop-dbhi/origins/identity"
	"github.com/chop-dbhi/origins/testutil"
	"github.com/chop-dbhi/origins/transactor"
	"github.com/chop-dbhi/origins/view"
	"github.com/stretchr/testify/assert"
)

var (
	joe     = identity.New("", "joe")
	livesIn = identity.New("", "lives in")
	ny      = identity.New("", "New York")
	ca      = identity.New("", "California")
	pa      = identity.New("", "Pennsylvania")
)

func TestViewMin(t *testing.T) {
	var (
		err        error
		t0, t1, t2 int64
		facts      fact.Reader
		tx         *transactor.Transaction
		v          *view.Domain
	)

	domain := "joe"

	store := testutil.Store()

	// Before the store was populated. Offset by one microsecond to
	// mimic pre-transaction.
	t0 = origins.FromTime(time.Now()) - 1

	facts = fact.NewReader(fact.Facts{
		fact.Assert(joe, livesIn, ny),
	})

	tx = transactor.New(store)

	// Time of first transaction.
	t1 = tx.Time

	err = tx.Transact(facts, domain, true)

	if err != nil {
		t.Fatal(err)
	}

	tx.Commit()

	facts = fact.NewReader(fact.Facts{
		fact.Retract(joe, livesIn, ny),
		fact.Assert(joe, livesIn, ca),
	})

	tx = transactor.New(store)

	// Time of second transaction.
	t2 = tx.Time

	err = tx.Transact(facts, domain, true)

	if err != nil {
		t.Fatal(err)
	}

	tx.Commit()

	// View at t0
	v = view.Asof(store, t0).Domain(domain)
	assert.Equal(t, 0, len(v.Facts()))

	// View at t1
	v = view.Asof(store, t1).Domain(domain)
	assert.Equal(t, 1, len(v.Facts()))

	// View at t2
	v = view.Asof(store, t2).Domain(domain)
	assert.Equal(t, 3, len(v.Facts()))

	// Now
	v = view.Now(store).Domain(domain)
	assert.Equal(t, 3, len(v.Facts()))

	// Since t2
	v = view.Since(store, t2).Domain(domain)
	assert.Equal(t, 2, len(v.Facts()))
}
