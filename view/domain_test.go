package view_test

import (
	"testing"
	"time"

	"github.com/chop-dbhi/origins"
	"github.com/chop-dbhi/origins/fact"
	"github.com/chop-dbhi/origins/testutil"
	"github.com/chop-dbhi/origins/transactor"
	"github.com/chop-dbhi/origins/view"
	"github.com/stretchr/testify/assert"
)

func testDomainGaps(t *testing.T, facts fact.Facts) {
	r := fact.NewReader(facts)

	store := testutil.Store()
	domain := "test"

	_, err := transactor.Commit(store, r, domain, true)

	if err != nil {
		panic(err)
	}

	v := view.Now(store)

	dv := v.Domain(domain)

	// 3 days to move is acceptable
	gs := dv.Gaps(3 * 24 * time.Hour)

	assert.Equal(t, 1, len(gs))
	assert.Equal(t, 1, len(gs[0].Gaps))

	// 2 years.. no
	gs = dv.Gaps(2 * 356 * 24 * time.Hour)

	assert.Equal(t, 0, len(gs))
}

// Chronological order.
func TestDomainGapsOrdered(t *testing.T) {
	facts := fact.Facts{
		fact.AssertTime(joe, livesIn, ny, origins.MustParseTime("September 20, 2009")),
		fact.RetractTime(joe, livesIn, ny, origins.MustParseTime("June 19, 2012")),
		fact.AssertTime(joe, livesIn, ca, origins.MustParseTime("March 3, 2013")),
		fact.RetractTime(joe, livesIn, ca, origins.MustParseTime("March 3, 2013")),
		fact.AssertTime(joe, livesIn, pa, origins.MustParseTime("March 6, 2013")),
	}

	testDomainGaps(t, facts)
}

// A retraction is made after a subsequent fact is asserted for the same entity/attribute
// pair.
func TestDomainGapsLateRetract(t *testing.T) {
	facts := fact.Facts{
		fact.AssertTime(joe, livesIn, ny, origins.MustParseTime("September 20, 2009")),
		fact.AssertTime(joe, livesIn, ca, origins.MustParseTime("March 3, 2013")),
		fact.RetractTime(joe, livesIn, ny, origins.MustParseTime("June 19, 2012")),
		fact.RetractTime(joe, livesIn, ca, origins.MustParseTime("March 3, 2013")),
		fact.AssertTime(joe, livesIn, pa, origins.MustParseTime("March 6, 2013")),
	}

	testDomainGaps(t, facts)
}

// A retraction is made before the fact is asserted. This demonstrates that something
// is known *not* to be true, but it is not known when it was began to be true.
func TestDomainGapsEarlyRetract(t *testing.T) {
	facts := fact.Facts{
		fact.RetractTime(joe, livesIn, ny, origins.MustParseTime("June 19, 2012")),
		fact.AssertTime(joe, livesIn, ny, origins.MustParseTime("September 20, 2009")),
		fact.AssertTime(joe, livesIn, ca, origins.MustParseTime("March 3, 2013")),
		fact.RetractTime(joe, livesIn, ca, origins.MustParseTime("March 3, 2013")),
		fact.AssertTime(joe, livesIn, pa, origins.MustParseTime("March 6, 2013")),
	}

	testDomainGaps(t, facts)
}
