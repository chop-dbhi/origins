package view

import (
	"encoding/json"

	"github.com/chop-dbhi/origins"
	"github.com/chop-dbhi/origins/chrono"
	"github.com/sirupsen/logrus"
)

// An EventType denotes the type of event
type EventType int8

func (e EventType) String() string {
	switch e {
	case Add:
		return "add"
	case Change:
		return "change"
	case Remove:
		return "remove"
	}

	return ""
}

func (e EventType) MarshalJSON() ([]byte, error) {
	return json.Marshal(e.String())
}

const (
	// Event types.
	Add EventType = iota + 1
	Change
	Remove
)

// Order defines the order of an iterator of facts.
type Order int8

func (o Order) String() string {
	switch o {
	case Ascending:
		return "ascending"
	case Descending:
		return "descending"
	}

	return ""
}

const (
	// Order types.
	Ascending Order = iota + 1
	Descending
)

type Event struct {
	Type      EventType
	Entity    *origins.Ident
	Attribute *origins.Ident
	Before    *origins.Fact
	After     *origins.Fact
}

func (e *Event) MarshalJSON() ([]byte, error) {
	m := map[string]interface{}{
		"Type":      e.Type,
		"Entity":    e.Entity,
		"Attribute": e.Attribute,
	}

	if e.Before != nil {
		m["Before"] = map[string]interface{}{
			"Operation":   e.Before.Operation,
			"Domain":      e.Before.Domain,
			"Value":       e.Before.Value,
			"Time":        chrono.JSON(e.Before.Time),
			"Transaction": e.Before.Transaction,
		}
	}

	if e.After != nil {
		m["After"] = map[string]interface{}{
			"Operation":   e.After.Operation,
			"Domain":      e.After.Domain,
			"Value":       e.After.Value,
			"Time":        chrono.JSON(e.After.Time),
			"Transaction": e.After.Transaction,
		}
	}

	return json.Marshal(m)
}

// Timeline returns an ordered set of events derived from the fact iterator.
// The iterator is assumed to return facts in reverse order by time (newest first)
// which is what the Log view returns.
func Timeline(iter origins.Iterator, order Order) ([]*Event, error) {
	var (
		key              [4]string
		err              error
		fact, prev, next *origins.Fact
		events           []*Event
		event            *Event
		etype            EventType
	)

	// Map facts keyed by entity/attribute pairs.
	// TODO: benchmark compared to cached identity approach. This is more
	// resilient since it does not rely on pointers.
	facts := make(map[[4]string]*origins.Fact)

	for {
		if fact = iter.Next(); fact == nil {
			break
		}

		// Uniquely identities an entity-attribute pair. The fact domain
		// is not considered here since that can be controlled by the passed
		// iterator.
		key = [4]string{
			fact.Entity.Domain,
			fact.Entity.Name,
			fact.Attribute.Domain,
			fact.Attribute.Name,
		}

		// Swap depending on order.
		switch order {
		case Ascending:
			next = fact
			prev = facts[key]
		case Descending:
			prev = fact
			next = facts[key]
		default:
			logrus.Panicf("view: unknown order %v", order)
		}

		// Update the cache with the current fact.
		facts[key] = fact

		// No existing fact. Construct add event
		if prev == nil || next == nil {
			etype = Add

			// The next fact is a retraction. Since facts are ordered by
			// time, this assumes the retraction applies to the same value.
		} else if next.Operation == origins.Retraction {
			etype = Remove

			// If the value differs, mark as a change event.
		} else if !next.Value.Is(prev.Value) {
			etype = Change

			// Last condition assumes the fact is a duplicate.
		} else {
			continue
		}

		// Construct the event.
		event = &Event{
			Type:      etype,
			Entity:    fact.Entity,
			Attribute: fact.Attribute,
			Before:    prev,
			After:     next,
		}

		events = append(events, event)
	}

	if err = iter.Err(); err != nil {
		return nil, err
	}

	// TODO: The final set of facts represent the current state
	// (ascending) or initial state (descending) of the iterator.

	return events, nil
}
