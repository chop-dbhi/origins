package origins

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/chop-dbhi/origins/pb"
	"github.com/golang/protobuf/proto"
)

const (
	AssertOp  = "assert"
	RetractOp = "retract"

	identSep = "/"
)

// Regexp to validate a domain and identity name.
var (
	domainRe = regexp.MustCompile(`(?i)[a-z0-9_]+(\.[a-z0-9_])*`)
	nameRe   = regexp.MustCompile(`(?i)[a-z0-9_]+`)
)

// ParseOperation normalizes the string operation denoting an assertion
// or retraction of a fact.
func ParseOperation(s string) (string, error) {
	switch strings.ToLower(s) {
	case AssertOp:
		return AssertOp, nil
	case RetractOp:
		return RetractOp, nil
	}

	return "", fmt.Errorf("fact: invalid operation %s", s)
}

// Ident models an identity of something. For entities and attributes
// a domain is required. For values, if the domain is ommitted, the identity
// is considered an *opaque* value.
type Ident struct {
	Domain string
	Name   string
}

// String implements the Stringer interface.
func (id *Ident) String() string {
	if id.Domain == "" {
		return id.Name
	}

	return fmt.Sprintf("%s%s%s", id.Domain, identSep, id.Name)
}

// Is returns true if the passed identity has the same domain and name.
func (id *Ident) Is(b *Ident) bool {
	return id.Domain == b.Domain && id.Name == b.Name
}

func validateIdent(id *Ident) error {
	if id.Domain == "" {
		return nil
	}

	if !domainRe.MatchString(id.Domain) {
		return fmt.Errorf("ident: non-valid domain %s", id.Domain)
	}

	if !nameRe.MatchString(id.Name) {
		return fmt.Errorf("ident: non-valid name %s", id.Name)
	}

	return nil
}

// NewIdent initializes a new ident.
func NewIdent(d, n string) (*Ident, error) {
	id := &Ident{
		Domain: d,
		Name:   n,
	}

	if err := validateIdent(id); err != nil {
		return nil, err
	}

	return id, nil
}

// ParseIdent parses an identity string and returns an Ident.
func ParseIdent(s string) (*Ident, error) {
	id := &Ident{}

	// Split into two parts. If only one token is present,
	// this is a local name.
	toks := strings.SplitN(s, identSep, 2)

	switch len(toks) {
	case 2:
		id.Domain = toks[0]
		id.Name = toks[1]

		if err := validateIdent(id); err != nil {
			return nil, err
		}
	case 1:
		id.Name = toks[0]
	default:
		return nil, errors.New("ident: invalid ident")
	}

	return id, nil
}

type Fact struct {
	// Assert or retract
	Operation string

	// The domain the fact is contained in.
	Domain string

	// The entity, attribute, and value of the fact.
	Entity    *Ident
	Attribute *Ident
	Value     *Ident

	// The time this fact was valid in the world.
	Time int64

	// Transaction that processed this fact.
	Transaction uint64
}

// String satisfies the Stringer interface.
func (f *Fact) String() string {
	return fmt.Sprintf("(%s %s %s)", f.Entity, f.Attribute, f.Value)
}

func (f *Fact) Marshal() []byte {
	m := pb.Fact{
		Operation: proto.String(f.Operation),
		Entity: &pb.Ident{
			Domain: proto.String(f.Entity.Domain),
			Name:   proto.String(f.Entity.Name),
		},
		Attribute: &pb.Ident{
			Domain: proto.String(f.Entity.Domain),
			Name:   proto.String(f.Entity.Name),
		},
		Value: &pb.Ident{
			Domain: proto.String(f.Entity.Domain),
			Name:   proto.String(f.Entity.Name),
		},
		Time: proto.Int64(f.Time),
	}

	b, err := proto.Marshal(&m)

	if err != nil {
		panic(err)
	}

	return b
}

func (f *Fact) Unmarshal(b []byte) {
	m := pb.Fact{}

	if err := proto.Unmarshal(b, &m); err != nil {
		panic(err)
	}
}

// Assert returns a fact that is declared to be asserted.
func Assert(e, a, v *Ident) *Fact {
	return &Fact{
		Entity:    e,
		Attribute: a,
		Value:     v,
		Operation: AssertOp,
	}
}

// Retract returns a fact that is declared to be retracted.
func Retract(e, a, v *Ident) *Fact {
	return &Fact{
		Entity:    e,
		Attribute: a,
		Value:     v,
		Operation: RetractOp,
	}
}

// AssertForTime asserts a fact add a specified time.
func AssertForTime(e, a, v *Ident, t int64) *Fact {
	f := Assert(e, a, v)
	f.Time = t
	return f
}

// RetractForTime retracts a fact add a specified time.
func RetractForTime(e, a, v *Ident, t int64) *Fact {
	f := Retract(e, a, v)
	f.Time = t
	return f
}
