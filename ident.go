package origins

import (
	"fmt"
	"regexp"
	"strings"
)

// The separator between domain and name of a fully qualified identity.
const identSeparator = "/"

// Regexp to validate a domain and identity name. Domains and names may
// contain alpha-numeric characters, hyphens, or underscores. Domains may
// also have zero or more additional segments delimited by a period to
// support a namespace model.
var (
	domainRegex = regexp.MustCompile(`(?i)[a-z0-9_\-]+(\.[a-z0-9_\-])*`)
	nameRegex   = regexp.MustCompile(`(?i)[a-z0-9_\-]+`)
)

// Ident models an identity of something. For entities and attributes
// a domain is required. For values, if the domain is ommitted, the identity
// is considered a literal value.
type Ident struct {
	Domain string
	Name   string
}

// Is returns true if the passed ident is the same as the current one.
func (id *Ident) Is(b *Ident) bool {
	return id.Domain == b.Domain && id.Name == b.Name
}

// String returns the fully qualified identity string.
func (id *Ident) String() string {
	if id.Domain == "" {
		return id.Name
	}

	return fmt.Sprintf("%s%s%s", id.Domain, identSeparator, id.Name)
}

// NewIdent validates and initializes a new identity value.
func NewIdent(domain, name string) (*Ident, error) {
	if domain != "" {
		if !domainRegex.MatchString(domain) {
			return nil, fmt.Errorf("ident: invalid domain `%s`", domain)
		}

		if !nameRegex.MatchString(name) {
			return nil, fmt.Errorf("ident: invalid name `%s`", name)
		}
	}

	return &Ident{
		Domain: domain,
		Name:   name,
	}, nil
}

// ParseIdent parses a fully qualified identity string, validates the
// contents and returns an identity value.
func ParseIdent(s string) (*Ident, error) {
	var id *Ident

	// Split into two parts. If only one token is present,
	// this is a local name.
	toks := strings.SplitN(s, identSeparator, 2)

	switch len(toks) {
	case 2:
		return NewIdent(toks[0], toks[1])
	default:
		id = &Ident{
			Name: toks[0],
		}
	}

	return id, nil
}

// Idents is a slice of idents.
type Idents []*Ident

func (ids Idents) Len() int {
	return len(ids)
}

func (ids Idents) Swap(i, j int) {
	ids[i], ids[j] = ids[j], ids[i]
}

func (ids Idents) Less(i, j int) bool {
	return IdentComparator(ids[i], ids[j])
}
