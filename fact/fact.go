package fact

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/chop-dbhi/origins/identity"
	"github.com/golang/protobuf/proto"
)

type Operation string

const (
	AssertOp  Operation = "assert"
	RetractOp Operation = "retract"
)

func ParseOperation(s string) (Operation, error) {
	switch strings.ToLower(s) {
	case "", "assert":
		return AssertOp, nil
	case "retract":
		return RetractOp, nil
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
	Operation   Operation
	Time        int64
	Entity      *identity.Ident
	Attribute   *identity.Ident
	Value       *identity.Ident
	Transaction *identity.Ident
	Inferred    bool
}

// Proto returns an initialized ProtoFact.
func (f *Fact) Proto() proto.Message {
	return &ProtoFact{}
}

// ToProto returns a protobuf version of the Fact.
func (f *Fact) ToProto() (proto.Message, error) {
	m := ProtoFact{
		Operation:       proto.String(string(f.Operation)),
		Time:            proto.Int64(f.Time),
		EntityDomain:    proto.String(f.Entity.Domain),
		Entity:          proto.String(f.Entity.Local),
		AttributeDomain: proto.String(f.Attribute.Domain),
		Attribute:       proto.String(f.Attribute.Local),
		ValueDomain:     proto.String(f.Value.Domain),
		Value:           proto.String(f.Value.Local),
		Inferred:        proto.Bool(f.Inferred),
	}

	if f.Transaction != nil {
		m.Transaction = proto.String(f.Transaction.Local)
	}

	return &m, nil
}

// FromProto populates the value with the contents of the message.
func (f *Fact) FromProto(m proto.Message) error {
	x := m.(*ProtoFact)

	f.Domain = x.GetDomain()
	f.Time = x.GetTime()
	f.Entity = &identity.Ident{
		Domain: x.GetEntityDomain(),
		Local:  x.GetEntity(),
	}
	f.Attribute = &identity.Ident{
		Domain: x.GetAttributeDomain(),
		Local:  x.GetAttribute(),
	}
	f.Value = &identity.Ident{
		Domain: x.GetValueDomain(),
		Local:  x.GetValue(),
	}
	f.Inferred = x.GetInferred()

	f.Transaction = &identity.Ident{
		Domain: "",
		Local:  x.GetTransaction(),
	}

	return nil
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
		Operation:   AssertOp,
	}
}

// Retract returns a fact that is declared to be retracted.
func Retract(e, a, v, t *identity.Ident) *Fact {
	return &Fact{
		Entity:      e,
		Attribute:   a,
		Value:       v,
		Transaction: t,
		Operation:   RetractOp,
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
		Operation: AssertOp,
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
		Operation: RetractOp,
	}
}
