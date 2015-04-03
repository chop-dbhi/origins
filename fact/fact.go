package fact

import (
	"fmt"

	"github.com/chop-dbhi/origins/identity"
	"github.com/golang/protobuf/proto"
)

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

// Is returns true if the two facts are equivalent. This excludes transaction time.
func (f *Fact) Is(f2 *Fact) bool {
	return (f.Operation == f2.Operation &&
		f.Time == f2.Time &&
		f.Entity.Is(f2.Entity) &&
		f.Attribute.Is(f2.Attribute) &&
		f.Value.Is(f2.Value))
}

// Proto returns an initialized ProtoFact.
func (f *Fact) Proto() proto.Message {
	return &ProtoFact{}
}

// ToProto returns a protobuf version of the Fact.
func (f *Fact) ToProto() (proto.Message, error) {
	m := ProtoFact{
		Domain:          proto.String(f.Domain),
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

	switch x.GetOperation() {
	case "assert":
		f.Operation = AssertOp
	case "retract":
		f.Operation = RetractOp
	default:
		panic(fmt.Sprintf("Invalid operation %v", x.GetOperation()))
	}

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
	return fmt.Sprintf("(%s %s %s %s)", f.Operation, f.Entity, f.Attribute, f.Value)
}

// Added returns true if the fact is asserted.
func (f *Fact) Added() bool {
	if f.Operation == "retract" {
		return false
	}

	return true
}

// Assert returns a fact that is declared to be asserted.
func Assert(e, a, v *identity.Ident) *Fact {
	return &Fact{
		Entity:    e,
		Attribute: a,
		Value:     v,
		Operation: AssertOp,
	}
}

// Retract returns a fact that is declared to be retracted.
func Retract(e, a, v *identity.Ident) *Fact {
	return &Fact{
		Entity:    e,
		Attribute: a,
		Value:     v,
		Operation: RetractOp,
	}
}

// AssertTime asserts a fact add a specified time.
func AssertTime(e, a, v *identity.Ident, t int64) *Fact {
	f := Assert(e, a, v)
	f.Time = t
	return f
}

// RetractTime retracts a fact add a specified time.
func RetractTime(e, a, v *identity.Ident, t int64) *Fact {
	f := Retract(e, a, v)
	f.Time = t
	return f
}

// AssertString returns a fact that is declared to be asserted.
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

// RetractString returns a fact that is declared to be retracted.
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
