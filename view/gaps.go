package view

import (
	"sync"
	"time"

	"github.com/chop-dbhi/origins"
	"github.com/chop-dbhi/origins/fact"
	"github.com/chop-dbhi/origins/identity"
)

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
func Gaps(facts fact.Facts, threshold time.Duration) []*GapSet {
	index := make(map[string]map[string]int)
	batches := make([]fact.Facts, 0)

	var (
		ok    bool
		pos   int
		batch fact.Facts
		attrs map[string]int
	)

	// Batch facts by entity/attribute
	for _, f := range facts {
		if attrs, ok = index[f.Entity.String()]; !ok {
			attrs = make(map[string]int)
			attrs[f.Attribute.String()] = len(batches)

			batches = append(batches, fact.Facts{f})

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
		go func(facts fact.Facts) {
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

func processGaps(threshold time.Duration, facts fact.Facts) *GapSet {
	// Sort by valid time.
	fact.TimsortBy(fact.TimeComparator, facts)

	var (
		diff time.Duration
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

		diff = origins.DiffTime(f.Time, r.Time)

		// Difference in times exceed the threshold
		if diff > threshold {
			gaps = append(gaps, &Gap{
				Retracted: r.Value,
				Asserted:  f.Value,
				Duration:  diff,
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
