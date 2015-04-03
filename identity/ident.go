package identity

import (
	"errors"
	"fmt"
	"strings"

	"github.com/sirupsen/logrus"
)

// Seperator between the domain and local parts of an identity string.
const domainSep = ":"

// Concat concatenates a domain and local part into an identity string.
func Concat(d, l interface{}) string {
	return fmt.Sprintf("%v%s%v", d, domainSep, l)
}

var (
	ErrEmptyIdent         = errors.New("identity is empty")
	ErrInvalidIdentFormat = errors.New("invalid identity format")
)

// Identifiable is an interface that defines an Ident method which is
// expected to return a valid domain identity string for the value.
type Identifiable interface {
	Ident() *Ident
}

// Ident identifies something in a domain.
type Ident struct {
	Domain string
	Local  string
}

func (i *Ident) String() string {
	return Concat(i.Domain, i.Local)
}

// Is returns true if this identity and the passed one are equal.
func (i *Ident) Is(j *Ident) bool {
	return i.Domain == j.Domain && i.Local == j.Local
}

// New returns a new Ident.
func New(domain, local string) *Ident {
	return &Ident{
		Domain: domain,
		Local:  local,
	}
}

type Idents []*Ident

// Parse parses a string and returns an Ident value. This is
// used during parsing of RawFact and is evaluated for the component the
// identity represents, entity, attribute, or value.
func Parse(v interface{}) (*Ident, error) {
	var s string

	switch x := v.(type) {
	case *Ident:
		return x, nil
	case Ident:
		return &x, nil
	case Identifiable:
		return x.Ident(), nil
	case string:
		s = x
	default:
		err := errors.New(fmt.Sprintf("cannot not parse type %T into ident", v))
		return nil, err
	}

	if s == "" {
		return nil, ErrEmptyIdent
	}

	id := Ident{}

	// Split into two parts. If only one token is present,
	// this is a local name.
	toks := strings.SplitN(s, domainSep, 2)

	switch len(toks) {
	case 2:
		id.Domain = toks[0]
		id.Local = toks[1]
	case 1:
		id.Local = toks[0]
	default:
		return nil, ErrInvalidIdentFormat
	}

	return &id, nil
}

// MustParse parses the interface value or panics.
func MustParse(v interface{}) *Ident {
	id, err := Parse(v)

	if err != nil {
		panic(err)
	}

	return id
}

// String takes an interface value and returns an identity string.
func String(v interface{}) string {
	switch x := v.(type) {
	case string:
		return x
	case Ident:
		return x.String()
	case *Ident:
		return x.String()
	default:
		logrus.Panicf("cannot convert %v to identity string", v)
	}

	return ""
}
