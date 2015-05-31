package testutil

import (
	"io"
	"math/rand"
	"time"

	"github.com/chop-dbhi/origins"
)

var letters = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

func randomString(n int) string {
	b := make([]byte, n)

	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}

	return string(b)
}

type GeneratorFunc func(domain string, tx uint64) *origins.Fact

type FactGen struct {
	generator GeneratorFunc
	domain    string
	tx        uint64
	count     int
	size      int
}

func (r *FactGen) generate() *origins.Fact {
	if r.size > 0 && r.count == r.size {
		return nil
	}

	r.count += 1

	return r.generator(r.domain, r.tx)
}

func (r *FactGen) Read(buf origins.Facts) (int, error) {
	var (
		f *origins.Fact
		c = cap(buf)
	)

	for i := c; i < c; i++ {
		f = r.generate()

		if f == nil {
			return i, io.EOF
		}
	}

	return c, nil
}

func (r *FactGen) Next() (*origins.Fact, error) {
	return r.generate(), nil
}

// Subscribe implements the Stream interface.
func (r *FactGen) Subscribe(closer chan struct{}) (chan *origins.Fact, chan error) {
	var f *origins.Fact

	fch := make(chan *origins.Fact, 1000)
	errch := make(chan error)

	go func() {
	loop:
		for {
			select {
			case <-closer:
				break loop
			default:
				if f = r.generate(); f == nil {
					break loop
				}

				fch <- f
			}
		}

		close(fch)
	}()

	return fch, errch
}

func NewGenerator(domain string, tx uint64, size int, gen GeneratorFunc) *FactGen {
	return &FactGen{
		generator: gen,
		domain:    domain,
		tx:        tx,
		size:      size,
	}
}

// RandFact generates a fact with a random entity, attribute, and value.
func RandFact(domain string, tx uint64) *origins.Fact {
	e := &origins.Ident{domain, randomString(16)}
	a := &origins.Ident{domain, randomString(16)}
	v := &origins.Ident{domain, randomString(32)}

	return &origins.Fact{
		Operation:   origins.Assertion,
		Domain:      domain,
		Entity:      e,
		Attribute:   a,
		Value:       v,
		Transaction: tx,
	}
}

// NewRandGenerator returns a fact generator using the default RandFact
// generator function.
func NewRandGenerator(domain string, tx uint64, size int) *FactGen {
	rand.Seed(time.Now().UnixNano())
	return NewGenerator(domain, tx, size, RandFact)
}
