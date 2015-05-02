package testutil

import (
	"math/rand"
	"strconv"
	"time"

	"github.com/chop-dbhi/origins/fact"
	"github.com/chop-dbhi/origins/identity"
)

func RandFacts(n int, d, ed, ad, vd string) fact.Facts {
	rand.Seed(time.Now().UnixNano())

	facts := make(fact.Facts, n)
	c := identity.Cache{}

	var (
		e, a, v *identity.Ident
		f       *fact.Fact
	)

	for i := 0; i < n; i++ {
		e = c.Add(ed, string(rand.Int63()))
		a = c.Add(ad, string(rand.Int63()))
		v = c.Add(vd, string(rand.Int63()))

		f = fact.Assert(e, a, v)
		f.Domain = d

		facts[i] = f
	}

	return facts
}

func RandFactsWithTx(n int, d, ed, ad, vd string) fact.Facts {
	rand.Seed(time.Now().UnixNano())

	facts := make(fact.Facts, n)
	c := identity.Cache{}

	var (
		e, a, v, t *identity.Ident
		f          *fact.Fact
	)

	for i := 0; i < n; i++ {
		e = c.Add(ed, string(rand.Int63()))
		a = c.Add(ad, string(rand.Int63()))
		v = c.Add(vd, string(rand.Int63()))
		t = c.Add(d, string(rand.Int63()))

		f = fact.Assert(e, a, v)
		f.Transaction = t
		f.Domain = d

		facts[i] = f
	}

	return facts
}

// Generates a set of facts consisting of n entities each with m attributes.
// Variability (r) defines how variable the attribute values are.
func VariableFacts(n, m int, r float32) fact.Facts {
	if r <= 0 || r > 1 {
		panic("sort: variability must be between (0,1]")
	}

	// Fixed time
	var t int64 = 100

	facts := make(fact.Facts, n*m)

	vn := int(float32(n*m) * r)

	l := 0

	for i := 0; i < n; i++ {
		e := identity.New("test", strconv.Itoa(i))

		// Shuffle order of attributes per entity to give it a bit
		// more permutation.
		for _, j := range rand.Perm(m) {
			a := identity.New("test", strconv.Itoa(j))

			v := identity.New("test", strconv.Itoa(rand.Intn(vn)))

			facts[l] = fact.AssertTime(e, a, v, t)
			l += 1
		}
	}

	return facts
}
