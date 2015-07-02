package transactor

import (
	"github.com/Workiva/go-datastructures/trie/ctrie"
	"github.com/chop-dbhi/origins"
	"github.com/chop-dbhi/origins/dal"
	"github.com/chop-dbhi/origins/storage"
	"github.com/chop-dbhi/origins/view"
	"github.com/satori/go.uuid"
	"github.com/sirupsen/logrus"
)

const commitLogName = "commit"

// Stats contains information about a pipeline.
type Stats struct {
	// List of domains that were affected.
	Domains []string

	// Number of segments.
	Segments int

	// Total number of blocks.
	Blocks int

	// Total of number bytes.
	Bytes int

	// Total number of facts.
	Count int
}

// A Pipeline does the actual work of processing and writing facts to storage.
type Pipeline interface {
	// Init initializes the pipeline for the transaction.
	Init(*Transaction) error

	// Receive takes a fact and returns an error if the fact cannot be handled.
	Receive(*origins.Fact) error

	// Commit takes a storage transaction and writes any headers or indexes to make
	// the transacted facts visible. A storage transaction is passed in to enable
	// the writes to occur atomically which ensures consistency of the transacted
	// facts.
	Commit(storage.Tx) error

	// Abort aborts the pipeline and deletes any data written to storage.
	Abort(storage.Tx) error

	// Segments returns a slice of segments that were written to storage.
	Stats() *Stats
}

type DomainPipeline struct {
	Domain      string
	segment     *Segment
	engine      storage.Engine
	cache       *ctrie.Ctrie
	initialized bool
	dedupe      bool
}

func (p *DomainPipeline) String() string {
	return p.Domain
}

// Stats returns the the stats for the pipeline.
func (p *DomainPipeline) Stats() *Stats {
	return &Stats{
		Domains:  []string{p.segment.Domain},
		Segments: 1,
		Blocks:   p.segment.Blocks,
		Bytes:    p.segment.Bytes,
		Count:    p.segment.Count,
	}
}

func (p *DomainPipeline) initCache() error {
	logrus.Debugf("transactor.Pipeline(%s): initializing cache", p.Domain)

	p.initialized = true

	log, err := view.OpenLog(p.engine, p.Domain, commitLogName)

	// This denotes the domain is new.
	if err == view.ErrDoesNotExist {
		return nil
	} else if err != nil {
		return err
	}

	facts, err := origins.ReadAll(log.Asof(p.segment.Time))

	if err != nil {
		return err
	}

	// Sort facts by entity.
	origins.Timsort(facts, origins.EAVTComparator)

	// Group the facts by entity.
	giter := origins.Groupby(origins.NewBuffer(facts), func(f1, f2 *origins.Fact) bool {
		return f1.Entity.Is(f2.Entity)
	})

	// Initializing ctrie.
	cache := ctrie.New(nil)

	err = origins.MapFacts(giter, func(facts origins.Facts) error {
		cache.Insert([]byte(facts[0].Entity.Name), facts)
		return nil
	})

	p.cache = cache

	logrus.Debugf("transactor.Pipeline(%s): cache initialized", p.Domain)

	return err
}

func (p *DomainPipeline) Init(tx *Transaction) error {
	// Get the commit log for this domain.
	log, err := dal.GetLog(tx.Engine, p.Domain, commitLogName)

	if err != nil {
		return err
	}

	if log == nil {
		log = &dal.Log{}
	}

	// Initialize new segment pointing to the head of the log.
	p.segment = NewSegment(tx.Engine, p.Domain, tx.ID)
	p.segment.Base = log.Head
	p.segment.Next = log.Head
	p.segment.Time = tx.StartTime
	p.engine = tx.Engine
	p.dedupe = !tx.options.AllowDuplicates

	return nil
}

func (p *DomainPipeline) Receive(fact *origins.Fact) error {
	// Do not dedupe.
	if !p.dedupe {
		return p.segment.Write(fact)
	}

	// Initialize the cache.
	if !p.initialized {
		if err := p.initCache(); err != nil {
			return err
		}
	}

	// First transaction of the domain, write everything.
	if p.cache == nil {
		return p.segment.Write(fact)
	}

	var (
		ok    bool
		facts origins.Facts
		value interface{}
	)

	// Lookup the fact by name in the cache to get the facts.
	if value, ok = p.cache.Lookup([]byte(fact.Entity.Name)); !ok {
		return p.segment.Write(fact)
	}

	// Type assert to facts.
	facts = value.(origins.Facts)

	// Entity exists, check for an existing attribute.
	prev := origins.First(origins.NewBuffer(facts), func(f *origins.Fact) bool {
		return f.Attribute.Is(fact.Attribute)
	})

	// New attribute for entity.
	if prev == nil {
		return p.segment.Write(fact)
	}

	// Compare the values.
	if fact.Value.Is(prev.Value) && fact.Operation == prev.Operation {
		return nil
	}

	return p.segment.Write(fact)
}

func (p *DomainPipeline) Abort(tx storage.Tx) error {
	return p.segment.Abort(tx)
}

func (p *DomainPipeline) Commit(tx storage.Tx) error {
	var (
		err error
		log *dal.Log
	)

	// Write the remaining block of the segment.
	if err = p.segment.Commit(tx); err != nil {
		return err
	}

	// No new facts, remove the segment.
	if p.segment.Count == 0 {
		logrus.Debugf("pipeline: no facts written to %s, removing segment", p.Domain)
		p.segment.Abort(tx)
		return nil
	}

	logrus.Debugf("pipeline: %d facts written to %s", p.segment.Count, p.Domain)

	// Compare and swap ID on domain's commit log.
	if log, err = dal.GetLog(tx, p.Domain, commitLogName); err != nil {
		return err
	}

	// Existing commit log, check if the head is the same.
	if log != nil {
		// TODO: determine path to handle conflicts.
		if log.Head != nil && !uuid.Equal(*log.Head, *p.segment.Base) {
			return ErrCommitConflict
		}
	} else {
		log = &dal.Log{
			Name:   commitLogName,
			Domain: p.Domain,
		}
	}

	log.Head = p.segment.UUID

	_, err = dal.SetLog(tx, p.Domain, log)

	return err
}
