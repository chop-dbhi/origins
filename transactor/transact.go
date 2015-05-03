package transactor

import (
	"fmt"
	"io"
	"strconv"
	"sync"
	"time"

	"github.com/chop-dbhi/origins"
	"github.com/chop-dbhi/origins/fact"
	"github.com/chop-dbhi/origins/identity"
	"github.com/chop-dbhi/origins/storage"
	"github.com/chop-dbhi/origins/view"
	"github.com/sirupsen/logrus"
)

const (
	// The domain prefix for managed transaction domains. Each domain has a transaction
	// domain associated with it that contains facts about the transactions that are
	// applied to the domain.
	domainTxPrefix = "origins.tx.%s"

	// Name of the managed "domains" domain which stores facts about all domains in the
	// database.
	domainsDomain = "origins.domains"

	// Name of the managed "macros" domain which contains entities that serve as macros
	// in facts.
	macrosDomain = "origins.macros"
)

type Transaction struct {
	// Time is the time the transaction started.
	Time int64

	// Store the transaction applies to.
	Store *storage.Store

	// View of the store at the transaction time.
	view *view.View

	txs map[string]*fact.Fact

	domains map[string]fact.Facts
}

// DomainFacts creates a fact about a domain.
func (tx *Transaction) DomainFacts(d string) fact.Facts {
	var e, a, v *identity.Ident

	// Assert the identity of the domain.
	e = identity.New("", d)
	a = identity.New("origins", "attr/ident")
	v = identity.New("", d)

	ident := fact.Assert(e, a, v)

	tx.EvaluateFact(ident, domainsDomain, false)

	// Assert the URI of this domain is local.
	a = identity.New("origins", "attr/uri")
	v = identity.New("", ".")

	uri := fact.Assert(e, a, v)

	tx.EvaluateFact(uri, domainsDomain, false)

	return fact.Facts{
		ident,
		uri,
	}
}

// TxFact creates a fact about the transaction for a domain.
func (tx *Transaction) TxFact(d string) *fact.Fact {
	var e, a, v *identity.Ident

	domain := fmt.Sprintf("origins.tx.%s", d)

	ident := strconv.FormatInt(tx.Time, 10)

	e = identity.New("", ident)
	a = identity.New("origins", "attr/ident")
	v = identity.New("", ident)

	f := fact.Assert(e, a, v)

	tx.EvaluateFact(f, domain, false)

	return f
}

// applyMacros check for macros used in a fact and evaluates them.
func (tx *Transaction) applyMacros(f *fact.Fact) (*fact.Fact, error) {
	// Macro domain. Fact is about the domain itself.
	if f.Entity.Domain == macrosDomain {
		switch f.Entity.Local {
		case "domain":
			f.Entity.Local = f.Domain
			f.Domain = domainsDomain
			f.Entity.Domain = domainsDomain
			break
		case "tx":
			f.Domain = fmt.Sprintf("origins.tx.%s", f.Domain)
			f.Entity.Domain = ""
			f.Entity.Local = strconv.FormatInt(tx.Time, 10)
			break
		default:
			return nil, fmt.Errorf("transactor: unknown entity macro: %s", f.Entity.Local)
		}
	}

	if f.Value.Domain == macrosDomain {
		switch f.Value.Local {
		case "now":
			f.Value.Domain = ""
			f.Value.Local = strconv.FormatInt(tx.Time, 10)
			break
		default:
			return nil, fmt.Errorf("transactor: unknown value macro: %s", f.Value.Local)
		}
	}

	return f, nil
}

// write performs the processing to the write the facts to storage. Whether they
// get physically written is determined by the `commit` flag.
func (tx *Transaction) write(commit bool) Results {
	l := len(tx.domains)

	rchan := make(chan *Result, l)

	wg := sync.WaitGroup{}
	wg.Add(l)

	// Write the facts to the store.
	for domain, facts := range tx.domains {
		go func(d string, f fact.Facts) {
			n, err := tx.Store.WriteSegment(d, tx.Time, f, commit)

			r := Result{
				Store:  tx.Store.String(),
				Domain: d,
				Time:   tx.Time,
				Count:  len(f),
				Bytes:  n,
				Error:  err,
				Faked:  !commit,
			}

			rchan <- &r

			wg.Done()
		}(domain, facts)
	}

	wg.Wait()

	close(rchan)

	results := make(Results, l)

	for r := range rchan {
		results[r.Domain] = r
	}

	return results
}

// evaluate evaluates a fact. It is compared against the previous version of the
// fact if available and the schema characteristics of the associated attribute.
func (tx *Transaction) EvaluateFact(f *fact.Fact, domain string, strict bool) (*fact.Fact, error) {
	var err error

	// TODO(bjr): protect reserved domains, e.g. origins*. This would require a flag
	// or alternate method for installing the protected domains themselves.

	// Set the default domain or assert the strict constraint.
	if f.Domain == "" {
		if domain == "" {
			err = fmt.Errorf("transactor: no fact domain specified")
			logrus.Error(err)
			return nil, err
		}

		f.Domain = domain
	} else if strict && domain != "" && f.Domain != domain {
		err = fmt.Errorf("transactor: fact domain %s does not match %s", f.Domain, domain)
		logrus.Error(err)
		return nil, err
	}

	// Apply macros
	f, err = tx.applyMacros(f)

	// Default to assertion.
	if f.Operation == "" {
		f.Operation = fact.AssertOp
	}

	// Default to fact domain.
	if f.Entity.Domain == "" {
		f.Entity.Domain = f.Domain
	}

	if f.Attribute.Domain == "" {
		f.Attribute.Domain = f.Domain
	}

	if f.Value.Domain == "" {
		f.Value.Domain = f.Domain
	}

	// Set the valid time if empty.
	if f.Time == 0 {
		f.Time = tx.Time
	}

	if err != nil {
		return nil, err
	}

	return f, nil
}

func (tx *Transaction) TransactFact(f *fact.Fact, domain string, strict bool) (*fact.Fact, error) {
	f, err := tx.EvaluateFact(f, domain, strict)

	if err != nil {
		return nil, err
	}

	// Skipped fact.
	if f == nil {
		return nil, nil
	}

	// Split facts by domain.
	if facts, ok := tx.domains[f.Domain]; !ok {
		// Assert transaction facts for the domain.
		txf := tx.TxFact(f.Domain)
		tx.txs[f.Domain] = txf

		dfacts := tx.DomainFacts(f.Domain)

		tx.domains[dfacts[0].Domain] = dfacts
		tx.domains[txf.Domain] = fact.Facts{txf}
		tx.domains[f.Domain] = fact.Facts{f}
	} else {
		tx.domains[f.Domain] = append(facts, f)
	}

	// Set the transaction entity.
	f.Transaction = tx.txs[f.Domain].Entity

	return f, nil
}

// Transact reads facts from reader and evaluates them them against a "current" view of the database.
// In addition to evaluation, facts are asserted for the domains and transactions.
func (tx *Transaction) Transact(reader fact.Reader, domain string, strict bool) error {
	var (
		n   int
		f   *fact.Fact
		err error
	)

	// Buffer to read from the reader.
	buf := make(fact.Facts, 10)

	for {
		// Fill the buffer.
		n, err = reader.Read(buf)

		// Real error; exit the transaction.
		if err != nil && err != io.EOF {
			logrus.Error(err)
			return err
		}

		// Iterate over the buffered facts and process them.
		for _, f = range buf[:n] {
			tx.TransactFact(f, domain, strict)
		}

		// No more facts to read.
		if err == io.EOF {
			break
		}
	}

	return nil
}

// Evaluates reads facts from reader and evaluates them against a "current" view of the database.
func (tx *Transaction) Evaluate(reader fact.Reader, domain string, strict bool) (fact.Facts, error) {
	var (
		i, n  int
		f     *fact.Fact
		facts = make(fact.Facts, 0)
		err   error
	)

	// Buffer to read from the reader.
	buf := make(fact.Facts, 10)

	for {
		// Fill the buffer.
		n, err = reader.Read(buf)

		// Real error; exit the transaction.
		if err != nil && err != io.EOF {
			logrus.Error(err)
			return nil, err
		}

		i = 0

		// Iterate over the buffered facts and process them.
		for _, f = range buf[:n] {
			f, err = tx.EvaluateFact(f, domain, strict)

			if err != nil {
				return nil, err
			}

			if f == nil {
				continue
			}

			buf[i] = f
			i += 1
		}

		facts = append(facts, buf[:i]...)

		// No more facts to read.
		if err == io.EOF {
			break
		}
	}

	return facts, nil
}

// Commit writes the transacted facts to storage.
func (tx *Transaction) Commit() Results {
	return tx.write(true)
}

// Test performs a fake write to storage.
func (tx *Transaction) Test() Results {
	return tx.write(false)
}

// New initializes a new transaction
func New(store *storage.Store) *Transaction {
	// Get the current time to isolate the view.
	now := origins.FromTime(time.Now())

	return &Transaction{
		Store:   store,
		Time:    now,
		view:    view.Asof(store, now),
		txs:     make(map[string]*fact.Fact),
		domains: make(map[string]fact.Facts),
	}
}

// Commit is a convenience function for transacting a single reader and writing the facts to storage.
func Commit(store *storage.Store, reader fact.Reader, domain string, strict bool) (Results, error) {
	tx := New(store)

	tx.Transact(reader, domain, strict)
	results := tx.write(true)

	return results, nil
}

// Test is a convenience function for transacting a single reader without writing the facts to storage.
// This is used for testing whether a set of facts will transact.
func Test(store *storage.Store, reader fact.Reader, domain string, strict bool) (Results, error) {
	tx := New(store)

	tx.Transact(reader, domain, strict)
	results := tx.write(false)

	return results, nil
}

// Evaluate
func Evaluate(store *storage.Store, reader fact.Reader, domain string, strict bool) (fact.Facts, error) {
	tx := New(store)
	return tx.Evaluate(reader, domain, strict)
}
