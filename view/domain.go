package view

import (
	"fmt"
	"strings"

	"github.com/chop-dbhi/origins/fact"
	"github.com/chop-dbhi/origins/storage"
)

// Domain is view for a particular domain.
type Domain struct {
	Name string

	store *storage.Store
	min   int64
	max   int64
}

func (d *Domain) reader() *storage.Reader {
	r, err := d.store.RangeReader(d.Name, d.min, d.max)

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
		store: d.store,
		min:   d.min,
		max:   d.max,
	}

	return v.Facts()
}

// Facts returns all facts in this domain.
func (d *Domain) Facts() fact.Facts {
	r := d.reader()
	facts, _ := fact.ReadAll(r)

	return facts
}

// Reader returns a fact.Reader for this view.
func (d *Domain) Reader() fact.Reader {
	return d.reader()
}
