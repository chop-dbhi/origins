package view

import (
	"github.com/chop-dbhi/origins/fact"
	"github.com/chop-dbhi/origins/identity"
)

// IdentityFilter takes a fact and returns an identity.
type IdentityFilter func(f *fact.Fact) *identity.Ident

// Identities returns a unique set of the identities from the domain facts.
// It takes a FactIdentity to get the identity to evaluate. If nil the fact will
// be skipped. If external is false
func Identities(facts fact.Facts, filter IdentityFilter) identity.Idents {
	m := make(map[string]bool)
	ids := make(identity.Idents, 0)

	var (
		ok bool
		s  string
		id *identity.Ident
		f  *fact.Fact
	)

	for _, f = range facts {
		id = filter(f)

		if id == nil {
			continue
		}

		s = id.String()

		if _, ok = m[s]; !ok {
			m[s] = true
			ids = append(ids, id)
		}
	}

	return ids
}

// entityFilter returns all entity identifiers.
func entityFilter(f *fact.Fact) *identity.Ident {
	return f.Entity
}

// localEntityFilter returns entity identifiers that are local to this domain.
func localEntityFilter(f *fact.Fact) *identity.Ident {
	if f.Entity.Domain == "" || f.Domain == f.Entity.Domain {
		return f.Entity
	}

	return nil
}

// externalEntityFilter returns entity identifiers that are external to this domain.
func externalEntityFilter(f *fact.Fact) *identity.Ident {
	if f.Entity.Domain != "" && f.Domain != f.Entity.Domain {
		return f.Entity
	}

	return nil
}

// attributeFilter returns all attribute identifiers.
func attributeFilter(f *fact.Fact) *identity.Ident {
	return f.Attribute
}

// localAttributeFilter returns attribute identifiers that are local to this domain.
func localAttributeFilter(f *fact.Fact) *identity.Ident {
	if f.Attribute.Domain == "" || f.Domain == f.Attribute.Domain {
		return f.Attribute
	}

	return nil
}

// externalAttributeFilter returns attribute identifiers that are external to this domain.
func externalAttributeFilter(f *fact.Fact) *identity.Ident {
	if f.Attribute.Domain != "" && f.Domain != f.Attribute.Domain {
		return f.Attribute
	}

	return nil
}

// valueFilter returns all value identifiers.
func valueFilter(f *fact.Fact) *identity.Ident {
	return f.Value
}

// localValueFilter returns value identifiers that are local to this domain.
func localValueFilter(f *fact.Fact) *identity.Ident {
	if f.Value.Domain == "" || f.Domain == f.Value.Domain {
		return f.Value
	}

	return nil
}

// externalValueFilter returns value identifiers that are external to this domain.
func externalValueFilter(f *fact.Fact) *identity.Ident {
	if f.Value.Domain != "" && f.Domain != f.Value.Domain {
		return f.Value
	}

	return nil
}
