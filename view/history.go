package view

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/chop-dbhi/origins"
	"github.com/chop-dbhi/origins/chrono"
	"github.com/chop-dbhi/origins/storage"
	"github.com/chop-dbhi/origins/transactor"
	"github.com/sirupsen/logrus"
)

// RevisionType declares a revision's type.
type RevisionType int8

func (e RevisionType) String() string {
	switch e {
	case Add:
		return "add"
	case Change:
		return "change"
	case Remove:
		return "remove"
	case Conflict:
		return "conflict"
	}

	return "noop"
}

func (e RevisionType) MarshalJSON() ([]byte, error) {
	return json.Marshal(e.String())
}

const (
	// Revision types.
	Noop RevisionType = iota
	Add
	Change
	Remove
	Conflict
)

// DetectType detects the revision type for two facts. The facts are assumed
// to be have the same entity and attribute.
func DetectType(a, b *origins.Fact) RevisionType {
	// No existing state.
	if a == nil {
		if b.Operation == origins.Assertion {
			return Add
		}

		// Retraction of the first fact. This is unlikely to occur, but
		// the initial state of a fact could be that it is no longer true.
		return Remove
	}

	// No new state.
	if b == nil {
		return Remove
	}

	// Compare against the previous state.
	if a.Operation == origins.Retraction {
		if b.Operation == origins.Assertion {
			// Retraction followed by an assertion in the same transaction.
			if a.Transaction == b.Transaction {
				return Change
			}

			// Attribute has been re-asserted.
			return Add
		}

		// Both are retractions of the same value.
		if a.Value.Is(b.Value) {
			return Noop
		}

		// Retractions of different values.
		// TODO: what does this mean?
		logrus.Warn("history: detected subsequent retractions of the same attribute with different values.")
		return Conflict
	}

	if b == nil {
		return Add
	}

	// Retracting an attribute.
	if b.Operation == origins.Retraction {
		// Retracting an existing value.
		if a.Value.Is(b.Value) {
			return Remove
		}

		// Retraction of different values.
		// NOTE: If this attribute has a cardinality of many, then this would be reasonable,
		// however this warning is here to detect how often this occurs.
		logrus.Warn("history: detected a retraction of a value different from the previous state.")
		return Conflict
	}

	// Both are assertions, but value is the same.
	if a.Value.Is(b.Value) {
		return Noop
	}

	// Values are different.
	return Change
}

// Revision denotes a single change in state of an entity. This includes an
// attribute being added, an attribute being removed, or a changed value for
// an existing attribute.
type Revision struct {
	Type        RevisionType
	Time        time.Time
	Transaction *transactor.Transaction
	Entity      *origins.Ident
	Attribute   *origins.Ident
	Before      *origins.Fact
	After       *origins.Fact
}

func (r *Revision) MarshalJSON() ([]byte, error) {
	m := map[string]interface{}{
		"Type":        r.Type,
		"Entity":      r.Entity,
		"Attribute":   r.Attribute,
		"Time":        chrono.JSON(r.Time),
		"Transaction": r.Transaction,
	}

	if r.Before != nil {
		m["Before"] = map[string]interface{}{
			"Operation":   r.Before.Operation,
			"Domain":      r.Before.Domain,
			"Value":       r.Before.Value,
			"Time":        chrono.JSON(r.Before.Time),
			"Transaction": r.Before.Transaction,
		}
	}

	if r.After != nil {
		m["After"] = map[string]interface{}{
			"Operation":   r.After.Operation,
			"Domain":      r.After.Domain,
			"Value":       r.After.Value,
			"Time":        chrono.JSON(r.After.Time),
			"Transaction": r.After.Transaction,
		}
	}

	return json.Marshal(m)
}

type Commit struct {
	Time        time.Time
	Transaction *transactor.Transaction
	Revisions   []*Revision
}

type history []*Revision

func (h history) Commits() []*Commit {
	var (
		rev     *Revision
		commit  *Commit
		commits []*Commit
	)

	for _, rev = range h {
		if rev.Type == Noop {
			continue
		}

		// Start a new commit.
		if commit == nil || rev.Transaction != commit.Transaction {
			commit = &Commit{
				Time:        rev.Time,
				Transaction: rev.Transaction,
				Revisions:   []*Revision{rev},
			}

			commits = append(commits, commit)
		} else {
			commit.Revisions = append(commit.Revisions, rev)
		}
	}

	return commits
}

// History returns an ordered set of revisions. The passed iterator is assumed
// to be derived from a log with the facts in descending order by time.
func History(engine storage.Engine, iter origins.Iterator) (history, error) {
	var (
		err               error
		key               [4]string
		fact, previous    *origins.Fact
		current, boundary *Revision
	)

	// Cache transactions.
	txes := loadTransactions(engine)

	// Map of entity-attribute keys to their previous fact.
	state := make(map[[4]string]*origins.Fact)
	boundaries := make(map[[4]string]*Revision)

	var h history

	for {
		// Facts are in ascending order within a transaction and descending or
		// across transaction boundaries. A fact within a transaction will be
		// considered to have occurred *after* a previous fact, however crossing
		// transaction boundary, the previous state will be *after* the current
		// state.
		if fact = iter.Next(); fact == nil {
			break
		}

		// Uniquely identities an entity-attribute pair.
		key = [4]string{
			fact.Entity.Domain,
			fact.Entity.Name,
			fact.Attribute.Domain,
			fact.Attribute.Name,
		}

		// Reference the previous revision for the entity-attribute pair.
		previous = state[key]
		boundary = boundaries[key]

		// Construct the current revision.
		current = &Revision{
			Time:        fact.Time,
			Transaction: txes[fact.Transaction],
			Entity:      fact.Entity,
			Attribute:   fact.Attribute,
			After:       fact,
		}

		// First fact, set the boundary.
		if previous == nil {
			boundaries[key] = current
		} else {
			// Existing fact, check if the transaction boundary is crossed. If not,
			// simply set the before state to the previous fact. If the boundary is
			// crossed, update the bounary revision's before state to the current fact
			// and update the boundary.
			if fact.Transaction == previous.Transaction {
				current.Before = previous
			} else {
				boundary.Before = fact
				boundary.Type = DetectType(boundary.Before, boundary.After)
				boundaries[key] = current
			}
		}

		current.Type = DetectType(current.Before, current.After)

		state[key] = fact

		h = append(h, current)
	}

	if err = iter.Err(); err != nil {
		return nil, err
	}

	return h, nil
}

func loadTransactions(engine storage.Engine) map[uint64]*transactor.Transaction {
	log, err := OpenLog(engine, origins.TransactionsDomain, "commit")

	if err != nil {
		logrus.Debug(err)
		return nil
	}

	txes := make(map[uint64]*transactor.Transaction)

	iter := log.Now()

	var (
		ok bool
		tx *transactor.Transaction
	)

	err = origins.Map(iter, func(f *origins.Fact) error {
		// Transaction data are transacted by themselves.
		if tx, ok = txes[f.Transaction]; !ok {
			tx = &transactor.Transaction{
				ID: f.Transaction,
			}

			txes[f.Transaction] = tx
		}

		if f.Attribute.Domain == origins.TransactionsDomain {
			switch f.Attribute.Name {
			case "startTime":
				if t, err := chrono.Parse(f.Value.Name); err == nil {
					tx.StartTime = t
				}

			case "endTime":
				if t, err := chrono.Parse(f.Value.Name); err == nil {
					tx.EndTime = t
				}

			case "error":
				tx.Error = errors.New(f.Value.Name)

			}
		}

		return nil
	})

	return txes
}
