package view

import (
	"fmt"
	"strings"
	"time"

	"github.com/chop-dbhi/origins/fact"
	"github.com/chop-dbhi/origins/identity"
	"github.com/chop-dbhi/origins/storage"
)

// Domain is view for a particular domain.
type Domain struct {
	Name string
	Min  int64
	Max  int64

	store *storage.Store
}

// reader returns a Store reader bound by the min and max time values.
func (d *Domain) reader() *storage.Reader {
	r, err := d.store.RangeReader(d.Name, d.Min, d.Max)

	if err != nil {
		panic(err)
	}

	return r
}

func (d *Domain) Stats() *Stats {
	r := d.reader()

	return &Stats{
		FactCount:        len(d.Facts()),
		TransactionCount: len(d.Transactions()),
		StartTime:        r.StartTime(),
		EndTime:          r.EndTime(),
	}
}

func (d *Domain) Transactions() fact.Facts {
	// This *is* a transaction domain, return it's own facts.
	if strings.HasPrefix(d.Name, "origins.tx.") {
		return d.Facts()
	}

	domain := fmt.Sprintf("origins.tx.%s", d.Name)

	v := Domain{
		Name:  domain,
		Min:   d.Min,
		Max:   d.Max,
		store: d.store,
	}

	return v.Facts()
}

// Facts returns all facts in this domain.
func (d *Domain) Facts() fact.Facts {
	r := d.Reader()
	facts, _ := fact.ReadAll(r)
	return facts
}

// FilteredFacts returns facts in this domain that pass the filter function.
func (d *Domain) FilteredFacts(filter fact.Filter) fact.Facts {
	r := d.FilteredReader(filter)
	facts, _ := fact.ReadAll(r)
	return facts
}

// Reader returns a fact.Reader for this view.
func (d *Domain) Reader() fact.Reader {
	return d.reader()
}

// FilteredReader returns a fact.Reader with a filter function set.
func (d *Domain) FilteredReader(filter fact.Filter) fact.Reader {
	r := d.reader()
	r.Filter = filter
	return r
}

func (d *Domain) Aggregate(id string) *Aggregate {
	return &Aggregate{
		Name:   id,
		Domain: d.Name,
		Min:    d.Min,
		Max:    d.Max,
		store:  d.store,
	}
}

// Identities delegates to view.Identities.
func (d *Domain) Identities(filter IdentityFilter) identity.Idents {
	return Identities(d.Facts(), filter)
}

// Entities returns local and external entities.
func (d *Domain) Entities() identity.Idents {
	return d.Identities(entityFilter)
}

// LocalEntities returns all entities that are local to this domain.
func (d *Domain) LocalEntities() identity.Idents {
	return d.Identities(localEntityFilter)
}

// ExternalEntities returns all entities that are external to this domain.
func (d *Domain) ExternalEntities() identity.Idents {
	return d.Identities(externalEntityFilter)
}

// Attributes returns local and external entities.
func (d *Domain) Attributes() identity.Idents {
	return d.Identities(attributeFilter)
}

// LocalAttributes returns all entities that are local to this domain.
func (d *Domain) LocalAttributes() identity.Idents {
	return d.Identities(localAttributeFilter)
}

// ExternalAttributes returns all entities that are external to this domain.
func (d *Domain) ExternalAttributes() identity.Idents {
	return d.Identities(externalAttributeFilter)
}

// Values returns local and external entities.
func (d *Domain) Values() identity.Idents {
	return d.Identities(valueFilter)
}

// LocalValues returns all entities that are local to this domain.
func (d *Domain) LocalValues() identity.Idents {
	return d.Identities(localValueFilter)
}

// ExternalValues returns all entities that are external to this domain.
func (d *Domain) ExternalValues() identity.Idents {
	return d.Identities(externalValueFilter)
}

// Gaps delegates to view.Gaps.
func (d *Domain) Gaps(threshold time.Duration) []*GapSet {
	return Gaps(d.Facts(), threshold)
}
