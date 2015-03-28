package view

import (
	"testing"

	"github.com/chop-dbhi/origins/fact"
	"github.com/chop-dbhi/origins/identity"
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
		fact.AssertTime(joe, livesIn, ny, fact.MustParseTime("September 20, 2009")),
		fact.RetractTime(joe, livesIn, ny, fact.MustParseTime("June 19, 2012")),
		fact.AssertTime(joe, livesIn, ca, fact.MustParseTime("March 3, 2013")),
		fact.RetractTime(joe, livesIn, ca, fact.MustParseTime("March 3, 2013")),
		fact.AssertTime(joe, livesIn, pa, fact.MustParseTime("March 6, 2013")),
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
