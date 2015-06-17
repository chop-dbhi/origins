package dal

import (
	"testing"
	"time"

	"github.com/chop-dbhi/origins"
	"github.com/chop-dbhi/origins/chrono"
	"github.com/stretchr/testify/assert"
)

func TestMarshalFact(t *testing.T) {
	f := origins.Fact{
		Domain: "testing",

		Operation: origins.Assertion,

		Transaction: 5,

		Time: chrono.Norm(time.Now()),

		Entity: &origins.Ident{
			Domain: "testing",
			Name:   "field",
		},

		Attribute: &origins.Ident{
			Domain: "testing",
			Name:   "dataType",
		},

		Value: &origins.Ident{
			Name: "string",
		},
	}

	m := ProtoFact{}

	b, err := marshalFact(&m, &f)

	if err != nil {
		t.Error(err)
	}

	f2 := origins.Fact{}

	if err = unmarshalFact(&m, b, "testing", 5, &f2); err != nil {
		t.Error(err)
	}

	assert.Equal(t, f.Operation, f2.Operation)
	assert.Equal(t, f.Time, f2.Time)
	assert.True(t, f.Entity.Is(f2.Entity))
	assert.True(t, f.Value.Is(f2.Value))
}

func TestBlockEncoder(t *testing.T) {
	f := origins.Fact{
		Domain: "testing",

		Operation: origins.Assertion,

		Transaction: 5,

		Time: chrono.Norm(time.Now()),

		Entity: &origins.Ident{
			Domain: "testing",
			Name:   "field",
		},

		Attribute: &origins.Ident{
			Domain: "testing",
			Name:   "dataType",
		},

		Value: &origins.Ident{
			Name: "string",
		},
	}

	encoder := NewBlockEncoder()

	for i := 0; i < 5; i++ {
		if err := encoder.Write(&f); err != nil {
			t.Error(err)
		}
	}

	if encoder.Count != 5 {
		t.Error("expected 5, got %d", encoder.Count)
	}
}

func TestBlockDecoder(t *testing.T) {
	f := origins.Fact{
		Domain: "testing",

		Operation: origins.Assertion,

		Transaction: 5,

		Time: chrono.Norm(time.Now()),

		Entity: &origins.Ident{
			Domain: "testing",
			Name:   "field",
		},

		Attribute: &origins.Ident{
			Domain: "testing",
			Name:   "dataType",
		},

		Value: &origins.Ident{
			Name: "string",
		},
	}

	encoder := NewBlockEncoder()

	for i := 0; i < 5; i++ {
		encoder.Write(&f)
	}

	// Initialize decoder with encoded bytes.
	decoder := NewBlockDecoder(encoder.Bytes(), "testing", 5)

	var n int

	for {
		fact := decoder.Next()

		if fact == nil {
			break
		}

		n++
	}

	if err := decoder.Err(); err != nil {
		t.Error(err)
	}

	if n != encoder.Count {
		t.Error("expected 5, got %d", n)
	}
}

func BenchmarkMarshalFact(b *testing.B) {
	f := &origins.Fact{
		Domain: "testing",

		Operation: origins.Assertion,

		Transaction: 5,

		Time: chrono.Norm(time.Now()),

		Entity: &origins.Ident{
			Domain: "testing",
			Name:   "field",
		},

		Attribute: &origins.Ident{
			Domain: "testing",
			Name:   "dataType",
		},

		Value: &origins.Ident{
			Name: "string",
		},
	}

	m := &ProtoFact{}

	for i := 0; i < b.N; i++ {
		marshalFact(m, f)
	}
}

func BenchmarkUnmarshalFact(b *testing.B) {
	f := &origins.Fact{
		Domain: "testing",

		Operation: origins.Assertion,

		Transaction: 5,

		Time: chrono.Norm(time.Now()),

		Entity: &origins.Ident{
			Domain: "testing",
			Name:   "field",
		},

		Attribute: &origins.Ident{
			Domain: "testing",
			Name:   "dataType",
		},

		Value: &origins.Ident{
			Name: "string",
		},
	}

	f2 := &origins.Fact{}

	m := &ProtoFact{}

	bf, _ := marshalFact(m, f)

	for i := 0; i < b.N; i++ {
		unmarshalFact(m, bf, "testing", 5, f2)
	}
}

func BenchmarkBlockEncoder(b *testing.B) {
	f := origins.Fact{
		Domain: "testing",

		Operation: origins.Assertion,

		Transaction: 5,

		Time: chrono.Norm(time.Now()),

		Entity: &origins.Ident{
			Domain: "testing",
			Name:   "field",
		},

		Attribute: &origins.Ident{
			Domain: "testing",
			Name:   "dataType",
		},

		Value: &origins.Ident{
			Name: "string",
		},
	}

	encoder := NewBlockEncoder()

	for i := 0; i < b.N; i++ {
		encoder.Write(&f)
	}
}

func BenchmarkBlockDecoder(b *testing.B) {
	// Stop to do large initialization.
	b.StopTimer()

	f := &origins.Fact{
		Domain: "testing",

		Operation: origins.Assertion,

		Transaction: 5,

		Time: chrono.Norm(time.Now()),

		Entity: &origins.Ident{
			Domain: "testing",
			Name:   "field",
		},

		Attribute: &origins.Ident{
			Domain: "testing",
			Name:   "dataType",
		},

		Value: &origins.Ident{
			Name: "string",
		},
	}

	encoder := NewBlockEncoder()

	for i := 0; i < 2000000; i++ {
		encoder.Write(f)
	}

	// Initialize decoder with encoded bytes.
	decoder := NewBlockDecoder(encoder.Bytes(), "testing", 5)

	b.StartTimer()

	for i := 0; i < b.N; i++ {
		if f = decoder.Next(); f == nil {
			b.Error("decoder: ran out of facts")
			break
		}
	}
}
