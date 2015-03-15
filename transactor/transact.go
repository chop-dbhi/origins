package transactor

import (
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/chop-dbhi/origins/fact"
	"github.com/chop-dbhi/origins/storage"
	"github.com/sirupsen/logrus"
)

const (
	attrsDomain       = "origins.attrs"
	typesDomain       = "origins.types"
	cardinalityDomain = "origins.cardinality"
)

func TransactionError(s string) error {
	return errors.New(fmt.Sprintf("transaction error: %s", s))
}

func TransactionErrorf(s string, v ...interface{}) error {
	msg := fmt.Sprintf(s, v...)
	return errors.New(fmt.Sprintf("transaction error: %s", msg))
}

type transaction struct {
	// Store the transaction applies to.
	store *storage.Store

	// Reader of facts
	reader fact.Reader

	// Default domain for unbound facts.
	domain string

	// Check to ensure only facts in the specified domain are being processed.
	strict bool

	// Time the transaction started. This is also used as the *valid time* of the
	// facts themselves.
	time int64

	// View of the store at the transaction time.
	//view *view.View
}

// evaluate evaluates a fact. It is compared against the previous version of the
// fact if available and the schema characteristics of the associated attribute.
func (tx *transaction) evaluate(f *fact.Fact) (fact.Facts, error) {
	var err error

	// Set the default domain or assert the strict constraint.
	if f.Domain == "" {
		if tx.domain == "" {
			err = TransactionError("no fact domain specified")
			logrus.Error(err)
			return nil, err
		}

		f.Domain = tx.domain
	} else if tx.strict && tx.domain != "" && f.Domain != tx.domain {
		err = TransactionErrorf("fact domain %s is not %s", f.Domain, tx.domain)
		logrus.Error(err)
		return nil, err
	}

	// Set the default time.
	if f.Time == 0 {
		f.Time = tx.time
	}

	return fact.Facts{f}, nil
}

// write takes a map of domains to facts and writes them to the store.
func (tx *transaction) write(domains map[string]fact.Facts, commit bool) []*Result {
	l := len(domains)

	rchan := make(chan *Result, l)

	wg := sync.WaitGroup{}
	wg.Add(l)

	// Write the facts to the store.
	for domain, facts := range domains {
		go func(d string, f fact.Facts) {
			n, err := tx.store.WriteSegment(d, tx.time, f, commit)

			r := Result{
				Store:  tx.store.String(),
				Domain: d,
				Time:   tx.time,
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

	i := 0
	results := make([]*Result, l)

	for r := range rchan {
		results[i] = r
		i += 1
	}

	return results
}

func (tx *transaction) Exec(commit bool) ([]*Result, error) {
	logrus.Debugf("begin transaction for store %s", tx.store)

	if tx.strict {
		logrus.Debug("strict transacting enabled")
	}

	// Facts by domain.
	domains := make(map[string]fact.Facts)

	var (
		n     int
		f     *fact.Fact
		facts fact.Facts
		err   error
	)

	// Buffer to read from the reader.
	buf := make(fact.Facts, 10)

	for {
		// Fill the buffer.
		n, err = tx.reader.Read(buf)

		// Real error; exit the transaction.
		if err != nil && err != io.EOF {
			logrus.Error(err)
			return nil, err
		}

		// Iterate over the buffered facts and process them.
		for i := 0; i < n; i++ {
			facts, err = tx.evaluate(buf[i])

			if err != nil {
				return nil, err
			}

			for _, f = range facts {
				// Split into separate fact sets per domain.
				if l, ok := domains[f.Domain]; !ok {
					domains[f.Domain] = fact.Facts{f}
				} else {
					domains[f.Domain] = append(l, f)
				}
			}
		}

		// No more facts to read.
		if err == io.EOF {
			break
		}
	}

	results := tx.write(domains, commit)

	if commit {
		logrus.Debugf("transaction committed to store %s", tx.store)
	} else {
		logrus.Debugf("transaction not committed to store %s", tx.store)
	}

	return results, nil
}

// Transact transacts facts into a store. A domain is passed which will be used as the
// default for facts without a specified domain. If strict is true, all facts must
// match domain. If commit is true, the facts will be written to the store.
func Transact(s *storage.Store, r fact.Reader, domain string, strict bool, commit bool) ([]*Result, error) {
	// Lock the store
	s.Lock()
	defer s.Unlock()

	// Get the current time to isolate the view.
	now := time.Now().Unix()

	tx := transaction{
		store:  s,
		reader: r,
		domain: domain,
		strict: strict,
		time:   now,
		//view:   view.Asof(s, now),
	}

	return tx.Exec(commit)
}
