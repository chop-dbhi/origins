package fact

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/chop-dbhi/origins/identity"
)

func ParseOperation(s string) (string, error) {
	switch strings.ToLower(s) {
	case "", "assert":
		return "assert", nil
	case "retract":
		return "retract", nil
	}

	return "", errors.New(fmt.Sprintf("unknown operation: %s", s))
}

var timeLayouts = []string{
	"2006-01-02",
	"2006-01-02 3:04 PM",
	"Jan 02, 2006",
	time.RFC3339,
	time.RFC1123,
	time.RFC1123Z,
	time.RFC822,
	time.RFC822Z,
	time.ANSIC,
}

func ParseTime(s string) (int64, error) {
	var (
		t   time.Time
		err error
	)

	for _, layout := range timeLayouts {
		t, err = time.Parse(layout, s)

		if err == nil {
			return t.Unix(), nil
		}
	}

	err = errors.New(fmt.Sprintf("could not parse time: %s", s))
	return 0, err
}

// Fact represents a valid and prepared fact.
type Fact struct {
	Domain      string
	Operation   string
	Time        int64
	Entity      *identity.Ident
	Attribute   *identity.Ident
	Value       *identity.Ident
	Transaction *identity.Ident
	inferred    bool
}

// String satisfies the Stringer interface.
func (f *Fact) String() string {
	return fmt.Sprintf("(%s %s %s)", f.Entity, f.Attribute, f.Value)
}

// Added returns true if the fact is asserted.
func (f *Fact) Added() bool {
	if f.Operation == "retract" {
		return false
	}

	return true
}

// Assert returns a fact that is declared to be asserted.
func Assert(e, a, v, t *identity.Ident) *Fact {
	return &Fact{
		Entity:      e,
		Attribute:   a,
		Value:       v,
		Transaction: t,
		Operation:   "assert",
	}
}

// Retract returns a fact that is declared to be retracted.
func Retract(e, a, v, t *identity.Ident) *Fact {
	return &Fact{
		Entity:      e,
		Attribute:   a,
		Value:       v,
		Transaction: t,
		Operation:   "retract",
	}
}

// AssertRaw returns a fact that is declared to be asserted.
func AssertString(e, a, v string) *Fact {
	eident := identity.MustParse(e)
	aident := identity.MustParse(a)
	vident := identity.MustParse(v)

	return &Fact{
		Entity:    eident,
		Attribute: aident,
		Value:     vident,
		Operation: "assert",
	}
}

// Retract returns a fact that is declared to be retracted.
func RetractString(e, a, v string) *Fact {
	eident := identity.MustParse(e)
	aident := identity.MustParse(a)
	vident := identity.MustParse(v)

	return &Fact{
		Entity:    eident,
		Attribute: aident,
		Value:     vident,
		Operation: "retract",
	}
}
