package view

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/chop-dbhi/origins/fact"
	"github.com/chop-dbhi/origins/identity"
	"github.com/chop-dbhi/origins/storage"
)

// Domain is view for a particular domain.
type Domain struct {
	Name string

	store *storage.Store
	min   int64
	max   int64
}

// reader returns a Store reader bound by the min and max time values.
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

// Gap holds two facts that represent a subsequent value change.
type Gap struct {
	Retracted *identity.Ident
	Asserted  *identity.Ident
	Duration  time.Duration
}

// GapSet holds all detected gaps for a entity/attribute pair.
type GapSet struct {
	Entity    *identity.Ident
	Attribute *identity.Ident
	Threshold time.Duration
	Gaps      []*Gap
}

// Gaps returns the entity/attribute pairs that exceed the gap threshold.
// A *gap* is a value that was retracted for an entity/attribute pair at a time
// prior to the next value being asserted for the same entity/attribute pair
// that exceeds the gap threshold.
//
// Transaction time is guaranteed to be chronological, however valid time may be
// out-of-order. A fact may be retracted before it is asserted. This translates to
// a fact known to not be true anymore, but is not yet known when it was true.
//
// The algorithm batches facts by entity/attribute. Each batch is sorted by valid
// time and evaluated for gaps in parallel.
func (d *Domain) Gaps(threshold time.Duration) []*GapSet {
	index := make(map[string]map[string]int)
	batches := make([]fact.FactsByValidTime, 0)

	var (
		ok    bool
		pos   int
		batch fact.FactsByValidTime
		attrs map[string]int
	)

	// Batch facts by entity/attribute
	for _, f := range d.Facts() {
		if attrs, ok = index[f.Entity.String()]; !ok {
			attrs = make(map[string]int)
			attrs[f.Attribute.String()] = len(batches)

			batches = append(batches, fact.FactsByValidTime{f})

			index[f.Entity.String()] = attrs
		} else {
			pos = attrs[f.Attribute.String()]
			batch = batches[pos]

			batches[pos] = append(batch, f)
		}
	}

	// Channel that receives a slice of gaps for each entity/attribute pair.
	gsch := make(chan *GapSet, len(batches))
	gapsets := make([]*GapSet, 0)

	wg := sync.WaitGroup{}
	wg.Add(len(batches))

	// Process batches in parallel
	for _, facts := range batches {
		go func(facts fact.FactsByValidTime) {
			gs := processGaps(threshold, facts)

			if gs != nil {
				gsch <- gs
			}

			wg.Done()
		}(facts)
	}

	wg.Wait()
	close(gsch)

	// Aggregate gaps
	for gs := range gsch {
		gapsets = append(gapsets, gs)
	}

	return gapsets
}

func processGaps(threshold time.Duration, facts fact.FactsByValidTime) *GapSet {
	// Sort by valid time.
	sort.Sort(facts)

	var (
		r    *fact.Fact
		gaps = make([]*Gap, 0)
	)

	for _, f := range facts {
		// Gaps are retraction followed by an assertion.
		if f.Operation == fact.RetractOp {
			r = f
			continue
		} else if r == nil {
			continue
		}

		// Difference in times exceed the threshold
		if f.Time-r.Time > int64(threshold) {
			gaps = append(gaps, &Gap{
				Retracted: r.Value,
				Asserted:  f.Value,
				Duration:  time.Duration(f.Time - r.Time),
			})
		}
	}

	// No gaps detected.
	if len(gaps) == 0 {
		return nil
	}

	// Slice of gaps ordered by transaction time.
	return &GapSet{
		Entity:    facts[0].Entity,
		Attribute: facts[0].Attribute,
		Gaps:      gaps,
		Threshold: threshold,
	}
}
