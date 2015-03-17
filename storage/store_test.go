package storage

import (
	"testing"
	"time"

	"github.com/chop-dbhi/origins/fact"
	"github.com/chop-dbhi/origins/identity"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

// Turn on debug level
func init() {
	logrus.SetLevel(logrus.DebugLevel)
}

func TestStoreHeader(t *testing.T) {
	var (
		err   error
		data  []byte
		store *Store
	)

	engine, _ := open(nil)

	cfg := Config{
		Engine: engine,
	}

	if store, err = Init(&cfg); err != nil {
		t.Fatal(err)
	}

	// Confirm in storage.
	if data, _ = engine.Get(store.storeKey); data == nil {
		t.Fatal("store header not in storage")
	}
}

func TestStoreWriteSegment(t *testing.T) {
	var (
		err    error
		store  *Store
		output fact.Facts
		input  fact.Facts
	)

	engine, _ := open(nil)

	store, _ = Init(&Config{
		Engine: engine,
	})

	domain := "test"

	e := identity.New(domain, "bill")
	a := identity.New(domain, "is")
	v := identity.New("", "tall")

	input = fact.Facts{
		fact.Assert(e, a, v),
		fact.Assert(e, a, v),
		fact.Assert(e, a, v),
	}

	now := time.Now().Unix()

	n, err := store.WriteSegment(domain, uint64(now), input, true)

	if err != nil {
		t.Fatal(err)
	}

	output = make(fact.Facts, len(input))

	r, err := store.Reader(domain)

	n, err = r.Read(output)

	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, len(input), n)
	assert.Equal(t, len(input), len(output))

	assert.Equal(t, "test", output[0].Entity.Domain)
	assert.Equal(t, "bill", output[0].Entity.Local)
}

func benchWriteSegment(b *testing.B, n int) {
	logrus.SetLevel(logrus.WarnLevel)

	engine, _ := open(nil)

	store, err := Init(&Config{
		Engine: engine,
	})

	if err != nil {
		panic(err)
	}

	domain := "test"

	e := identity.New(domain, "bill")
	a := identity.New(domain, "is")
	v := identity.New("", "tall")

	input := make(fact.Facts, n)

	for i := 0; i < len(input); i++ {
		input[i] = fact.Assert(e, a, v)
	}

	now := uint64(time.Now().Unix())

	for i := 0; i < b.N; i++ {
		store.WriteSegment(domain, now+uint64(i), input, true)
	}
}

func BenchmarkMemoryStoreWriteSegment__1(b *testing.B) {
	benchWriteSegment(b, 1)
}

func BenchmarkMemoryStoreWriteSegment__10(b *testing.B) {
	benchWriteSegment(b, 10)
}

func BenchmarkMemoryStoreWriteSegment__100(b *testing.B) {
	benchWriteSegment(b, 100)
}

func BenchmarkMemoryStoreWriteSegment__1000(b *testing.B) {
	benchWriteSegment(b, 1000)
}

func BenchmarkMemoryStoreWriteSegment__10000(b *testing.B) {
	benchWriteSegment(b, 10000)
}
