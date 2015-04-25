package fact

import (
	"testing"

	"github.com/chop-dbhi/origins/identity"
	"github.com/stretchr/testify/assert"
)

var (
	compFacts                            Facts
	bobName, sueName, bobColor, sueColor *Fact
)

func init() {
	e1 := identity.New("people", "bob")
	e2 := identity.New("people", "sue")

	a1 := identity.New("", "name")
	a2 := identity.New("", "color")

	v1 := identity.New("", "Bob")
	v2 := identity.New("", "Sue")
	v3 := identity.New("", "red")
	v4 := identity.New("", "blue")

	t0 := MustParseTime("2015-01-01")
	t1 := MustParseTime("2015-01-02")

	bobName = AssertTime(e1, a1, v1, t0)
	bobColor = AssertTime(e1, a2, v3, t0)
	sueName = AssertTime(e2, a1, v2, t1)
	sueColor = AssertTime(e2, a2, v4, t1)

	compFacts = Facts{
		bobName,
		bobColor,
		sueName,
		sueColor,
	}
}

func testComparator(t *testing.T, comp Comparator) Facts {
	facts := make(Facts, 4)
	copy(facts, compFacts)

	TimsortBy(comp, facts)

	return facts
}

func TestEntityComparator(t *testing.T) {
	facts := testComparator(t, EntityComparator)

	// bob, bob, sue, sue
	exp := Facts{
		bobName,
		bobColor,
		sueName,
		sueColor,
	}

	assert.Equal(t, exp, facts)
}

func TestAttributeComparator(t *testing.T) {
	facts := testComparator(t, AttributeComparator)

	// color, color, name, name
	exp := Facts{
		bobColor,
		sueColor,
		bobName,
		sueName,
	}

	assert.Equal(t, exp, facts)
}

func TestValueComparator(t *testing.T) {
	facts := testComparator(t, ValueComparator)

	// blue, Bob, Sue, red
	exp := Facts{
		bobName,
		sueName,
		sueColor,
		bobColor,
	}

	assert.Equal(t, exp, facts)
}

func TestTimeComparator(t *testing.T) {
	facts := testComparator(t, TimeComparator)

	// t0, t0, t1, t1
	exp := Facts{
		bobName,
		bobColor,
		sueName,
		sueColor,
	}

	assert.Equal(t, exp, facts)
}

func TestEAVTComparator(t *testing.T) {
	facts := testComparator(t, EAVTComparator)

	// bob:color:red, bob:name:Bob, sue:color:blue, sue:name:Sue
	exp := Facts{
		bobColor,
		bobName,
		sueColor,
		sueName,
	}

	assert.Equal(t, exp, facts)
}

func TestAVETComparator(t *testing.T) {
	facts := testComparator(t, AVETComparator)

	// color:blue:sue, color:red:bob, name:Bob:bob, name:Sue:sue
	exp := Facts{
		sueColor,
		bobColor,
		bobName,
		sueName,
	}

	assert.Equal(t, exp, facts)
}

func TestAEVTComparator(t *testing.T) {
	facts := testComparator(t, AEVTComparator)

	// color:bob:red, color:sue:blue, name:bob:Bob, name:sue:Sue
	exp := Facts{
		bobColor,
		sueColor,
		bobName,
		sueName,
	}

	assert.Equal(t, exp, facts)
}

func TestVAETComparator(t *testing.T) {
	facts := testComparator(t, VAETComparator)

	// Bob:name:bob, Sue:name:sue, blue:color:sue, red:color:bob,
	exp := Facts{
		bobName,
		sueName,
		sueColor,
		bobColor,
	}

	assert.Equal(t, exp, facts)
}
