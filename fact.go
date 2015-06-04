package origins

import (
	"fmt"
	"strings"
	"time"
)

// Operation denotes the validity of a fact. A fact can be asserted in which
// case it is deemed valid and true or retracted which denotes the fact is no
// longer valid or true. A fact may be retracted without being asserted first.
// This denotes some fact is known not to be true without necessarily knowing
// what is true.
type Operation int8

func (o Operation) String() string {
	switch o {
	case Assertion:
		return "assert"
	case Retraction:
		return "retract"
	default:
		return ""
	}
}

const (
	Noop Operation = iota
	Assertion
	Retraction
)

// ParseOperation normalizes the string operation denoting an assertion
// or retraction of a fact.
func ParseOperation(s string) (Operation, error) {
	switch strings.ToLower(s) {
	case "", "assert", "asserted", "a":
		return Assertion, nil
	case "retract", "retracted", "r":
		return Retraction, nil
	}

	return Noop, fmt.Errorf("fact: invalid operation `%s`", s)
}

// Fact is the fundamental unit of the information model. It is an immutable
// declaration of something that is true.
type Fact struct {
	// Denotes whether the fact is asserted or retracted.
	Operation Operation

	// The domain the fact applies to. The motivation is to enable asserting
	// facts about entities in different domains. This model decouples content
	// from context to support auxillary information.
	Domain string

	// The entity, attribute, and value of the fact. Also referred to as a
	// "triple", these make up the content of the fact. The entity and attribute
	// must have a domain associated with it. If not specified, they default to
	// the fact domain. If a value has a domain specified, it will be interpreted
	// as a reference to an entity, otherwise it will be treated as a literal.
	Entity    *Ident
	Attribute *Ident
	Value     *Ident

	// The time the fact is true in the world. Also known as the "valid time",
	// this can be set if the fact is true at an earlier or later time than
	// when it was added.
	Time time.Time

	// Transaction that processed this fact. The transaction serves as the
	// temporal component of the fact. Time-travel queries rely on the
	// transaction time to filter out facts not applicable in the specified
	// time range.
	Transaction uint64
}

// String returns a string representation of the fact.
func (f *Fact) String() string {
	return fmt.Sprintf("(%s %s %s %s)", f.Operation, f.Entity, f.Attribute, f.Value)
}

func parseIdent(id interface{}) (*Ident, error) {
	switch x := id.(type) {
	case *Ident:
		return x, nil
	case string:
		return ParseIdent(x)
	}

	return nil, fmt.Errorf("ident: cannot parse %v", id)
}

func parseFact(op Operation, e, a, v interface{}, t time.Time) (*Fact, error) {
	var (
		err           error
		eid, aid, vid *Ident
	)

	if eid, err = parseIdent(e); err != nil {
		return nil, err
	}

	if aid, err = parseIdent(a); err != nil {
		return nil, err
	}

	if vid, err = parseIdent(v); err != nil {
		return nil, err
	}

	return &Fact{
		Operation: op,
		Entity:    eid,
		Attribute: aid,
		Value:     vid,
		Time:      t,
	}, nil
}

// Assert returns an asserted fact. The EAV values can be strings or Ident values.
func Assert(e, a, v interface{}) (*Fact, error) {
	return parseFact(Assertion, e, a, v, time.Time{})
}

// Retract returns a retracted fact. The EAV values can be strings or Ident values.
func Retract(e, a, v interface{}) (*Fact, error) {
	return parseFact(Retraction, e, a, v, time.Time{})
}

// AssertForTime returns an asserted fact at a specified time.
func AssertForTime(e, a, v interface{}, t time.Time) (*Fact, error) {
	return parseFact(Assertion, e, a, v, t)
}

// RetractForTime returns a retracted fact at a specified time.
func RetractForTime(e, a, v interface{}, t time.Time) (*Fact, error) {
	return parseFact(Retraction, e, a, v, t)
}

// Facts is a slice of facts.
type Facts []*Fact

func (fs Facts) String() string {
	toks := make([]string, len(fs))

	for i, f := range fs {
		toks[i] = f.String()
	}

	return strings.Join(toks, "\n")
}
