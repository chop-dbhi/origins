package view

import (
	"fmt"
	"strings"

	"github.com/chop-dbhi/origins/fact"
	"github.com/chop-dbhi/origins/storage"
)

// Domain is view for a particular domain.
type Domain struct {
	Domain string
	store  *storage.Store
	t0     int64
	t1     int64
}

func (d *Domain) reader() *storage.Reader {
	r, err := d.store.RangeReader(d.Domain, d.t0, d.t1)

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
	if strings.HasPrefix(d.Domain, "origins.tx.") {
		return d.Facts()
	}

	domain := fmt.Sprintf("origins.tx.%s", d.Domain)

	v := Domain{
		Domain: domain,
		store:  d.store,
		t0:     d.t0,
		t1:     d.t1,
	}

	return v.Facts()
}

// Facts returns all facts in this domain.
func (d *Domain) Facts() fact.Facts {
	r := d.reader()
	facts, _ := fact.ReadAll(r)

	return facts
}
