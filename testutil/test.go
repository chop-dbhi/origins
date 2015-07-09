package testutil

import (
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

type EAVDictionary struct {
	entity    []string
	attribute []string
	value     []string
}

// NewEAVDictionary generates a dictionary of E, A, V values.
func NewEAVDictionary(eLen, aLen, vLen int) *EAVDictionary {
	dict := EAVDictionary{
		entity:    make([]string, eLen),
		attribute: make([]string, aLen),
		value:     make([]string, vLen),
	}

	for i := range dict.entity {
		dict.entity[i] = randomString(16)
	}
	for i := range dict.attribute {
		dict.attribute[i] = randomString(16)
	}
	for i := range dict.value {
		dict.value[i] = randomString(32)
	}

	return &dict
}

type GeneratorFunc func(domains []string, tx uint64) *origins.Fact

type FactGen struct {
	generator GeneratorFunc
	domains   []string
	tx        uint64
	count     int
	size      int
}

func (r *FactGen) Next() *origins.Fact {
	if r.size > 0 && r.count == r.size {
		return nil
	}

	r.count += 1

	return r.generator(r.domains, r.tx)
}

func (r *FactGen) Err() error {
	return nil
}

func NewGenerator(domains []string, tx uint64, size int, gen GeneratorFunc) *FactGen {
	return &FactGen{
		generator: gen,
		domains:   domains,
		tx:        tx,
		size:      size,
	}
}

// RandFactMultiDomain generates a fact with a random entity, attribute, and value;
// the fact will belong to a domain randomly chosen from the specified list.
func RandFactMultiDomain(domains []string, tx uint64) *origins.Fact {
	d := domains[rand.Intn(len(domains))]
	return RandFact(d, tx)
}

// RandFactSingleDomain is a wrapper to RandFact that's needed only to
// get the single domain out of the domains array
func RandFactSingleDomain(domains []string, tx uint64) *origins.Fact {
	return RandFact(domains[0], tx)
}

// RandFact generates a fact with a random entity, attribute, and value
// for a given domain
func RandFact(domain string, tx uint64) *origins.Fact {
	e := &origins.Ident{
		Domain: domain,
		Name:   randomString(16),
	}

	a := &origins.Ident{
		Domain: domain,
		Name:   randomString(16),
	}

	v := &origins.Ident{
		Domain: domain,
		Name:   randomString(32),
	}

	return &origins.Fact{
		Operation:   origins.Assertion,
		Domain:      domain,
		Entity:      e,
		Attribute:   a,
		Value:       v,
		Transaction: tx,
	}
}

// RandDictionaryFact returns a GeneratorFunc for generating facts from a given dictionary of values
// for a single domain
func RandDictionaryFact(dictionary *EAVDictionary) GeneratorFunc {
	return func(domains []string, tx uint64) *origins.Fact {
		domain := domains[0]

		e := &origins.Ident{
			Domain: domain,
			Name:   dictionary.entity[rand.Intn(len(dictionary.entity))],
		}

		a := &origins.Ident{
			Domain: domain,
			Name:   dictionary.attribute[rand.Intn(len(dictionary.attribute))],
		}

		v := &origins.Ident{
			Domain: domain,
			Name:   dictionary.value[rand.Intn(len(dictionary.value))],
		}

		return &origins.Fact{
			Operation:   origins.Assertion,
			Domain:      domain,
			Entity:      e,
			Attribute:   a,
			Value:       v,
			Transaction: tx,
		}
	}
}

// NewRandGenerator returns a fact generator using the default RandFact
// generator function.
func NewRandGenerator(domain string, tx uint64, size int) *FactGen {
	rand.Seed(time.Now().UnixNano())
	d := []string{domain}
	return NewGenerator(d, tx, size, RandFactSingleDomain)
}

// NewDictionaryBasedGenerator returns a fact generator using the default
// RandDictionaryFact generator function.
func NewDictionaryBasedGenerator(dictionary *EAVDictionary, domain string, tx uint64, size int) *FactGen {
	rand.Seed(time.Now().UnixNano())
	d := []string{domain}
	return NewGenerator(d, tx, size, RandDictionaryFact(dictionary))
}

// NewMultiDomainGenerator returns a fact generator for multiple domains
func NewMultidomainGenerator(domains []string, tx uint64, size int) *FactGen {
	rand.Seed(time.Now().UnixNano())
	return NewGenerator(domains, tx, size, RandFactMultiDomain)
}
