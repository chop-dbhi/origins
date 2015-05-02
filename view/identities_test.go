package view

import (
	"math/rand"
	"testing"

	"github.com/chop-dbhi/origins"
	"github.com/chop-dbhi/origins/fact"
	"github.com/chop-dbhi/origins/identity"
	"github.com/chop-dbhi/origins/testutil"
	"github.com/stretchr/testify/assert"
)

var (
	joe     = identity.New("", "joe")
	livesIn = identity.New("", "lives in")
	ny      = identity.New("", "New York")
	ca      = identity.New("", "California")
	pa      = identity.New("", "Pennsylvania")
)

func TestIdentities(t *testing.T) {
	facts := fact.Facts{
		fact.AssertTime(joe, livesIn, ny, origins.MustParseTime("September 20, 2009")),
		fact.RetractTime(joe, livesIn, ny, origins.MustParseTime("June 19, 2012")),
		fact.AssertTime(joe, livesIn, ca, origins.MustParseTime("March 3, 2013")),
		fact.RetractTime(joe, livesIn, ca, origins.MustParseTime("March 3, 2013")),
		fact.AssertTime(joe, livesIn, pa, origins.MustParseTime("March 6, 2013")),
	}

	var ids identity.Idents

	// Entities
	ids = Identities(facts, entityFilter)
	assert.Equal(t, identity.Idents{joe}, ids)

	// LocalEntities
	ids = Identities(facts, localEntityFilter)
	assert.Equal(t, identity.Idents{joe}, ids)

	// ExternalEntities
	ids = Identities(facts, externalEntityFilter)
	assert.Equal(t, identity.Idents{}, ids)

	// Attributes
	ids = Identities(facts, attributeFilter)
	assert.Equal(t, identity.Idents{livesIn}, ids)

	// LocalAttributes
	ids = Identities(facts, localAttributeFilter)
	assert.Equal(t, identity.Idents{livesIn}, ids)

	// ExternalAttributes
	ids = Identities(facts, externalAttributeFilter)
	assert.Equal(t, identity.Idents{}, ids)

	// Values
	ids = Identities(facts, valueFilter)
	assert.Equal(t, identity.Idents{ny, ca, pa}, ids)

	// LocalValues
	ids = Identities(facts, localValueFilter)
	assert.Equal(t, identity.Idents{ny, ca, pa}, ids)

	// ExternalValues
	ids = Identities(facts, externalValueFilter)
	assert.Equal(t, identity.Idents{}, ids)
}

func randomize(facts fact.Facts) fact.Facts {
	rand.Seed(1337)

	var (
		j int64
		l = int64(len(facts))
	)

	for i, _ := range facts {
		j = rand.Int63n(l)
		facts[i], facts[j] = facts[j], facts[i]
	}

	return facts
}

func benchIdentities(b *testing.B, n int) {
	d := ""
	facts := testutil.RandFactsWithTx(n, d, d, d, d)

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		facts = randomize(facts)
		b.StartTimer()
		Identities(facts, entityFilter)
	}
}

func BenchmarkIdentities__100(b *testing.B) {
	benchIdentities(b, 100)
}

func BenchmarkIdentities__1000(b *testing.B) {
	benchIdentities(b, 1000)
}

func BenchmarkIdentities__10000(b *testing.B) {
	benchIdentities(b, 10000)
}

func BenchmarkIdentities__100000(b *testing.B) {
	benchIdentities(b, 100000)
}

func BenchmarkIdentities__1000000(b *testing.B) {
	benchIdentities(b, 1000000)
}
