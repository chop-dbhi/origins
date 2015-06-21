package dal

import (
	"testing"
	"time"

	"github.com/chop-dbhi/origins"
	"github.com/chop-dbhi/origins/chrono"
	"github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
)

func TestLogMethods(t *testing.T) {
	engine, _ := origins.Init("memory", nil)

	id := uuid.NewV4()

	l := Log{
		Name:   "commit",
		Domain: "testing",
		Head:   &id,
	}

	_, err := SetLog(engine, "testing", &l)

	if err != nil {
		t.Error(err)
	}

	l2, err := GetLog(engine, "testing", "commit")

	if err != nil {
		t.Error(err)
	}

	assert.Equal(t, l.Name, l2.Name)
	assert.Equal(t, l.Domain, l2.Domain)
	assert.Equal(t, *l.Head, *l2.Head)
}

func TestSegmentMethods(t *testing.T) {
	engine, _ := origins.Init("memory", nil)

	id := uuid.NewV4()
	next := uuid.NewV4()

	now := chrono.Norm(time.Now().UTC())

	s := Segment{
		UUID:        &id,
		Transaction: 10,
		Domain:      "testing",
		Time:        now,
		Blocks:      5,
		Count:       4800,
		Bytes:       460800,
		Next:        &next,
		Base:        &next,
	}

	_, err := SetSegment(engine, "testing", &s)

	if err != nil {
		t.Error(err)
	}

	s2, err := GetSegment(engine, "testing", &id)

	if err != nil {
		t.Error(err)
	}

	assert.Equal(t, s.Transaction, s2.Transaction)
	assert.Equal(t, *s.Next, *s2.Next)
}

func TestBlockMethods(t *testing.T) {
	engine, _ := origins.Init("memory", nil)

	id := uuid.NewV4()
	idx := 0

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

	for i := 0; i < 1000; i++ {
		encoder.Write(f)
	}

	_, err := SetBlock(engine, "testing", &id, idx, encoder.Bytes())

	if err != nil {
		t.Error(err)
	}

	_, err = GetBlock(engine, "testing", &id, idx)

	if err != nil {
		t.Error(err)
	}
}

func BenchmarkSetLog(b *testing.B) {
	engine, _ := origins.Init("memory", nil)

	id := uuid.NewV4()

	l := &Log{
		Name: "commit",
		Head: &id,
	}

	for i := 0; i < b.N; i++ {
		SetLog(engine, "testing", l)
	}
}

func BenchmarkGetLog(b *testing.B) {
	engine, _ := origins.Init("memory", nil)

	id := uuid.NewV4()

	l := &Log{
		Name: "commit",
		Head: &id,
	}

	SetLog(engine, "testing", l)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		GetLog(engine, "testing", "commit")
	}
}

func BenchmarkSetSegment(b *testing.B) {
	engine, _ := origins.Init("memory", nil)

	id := uuid.NewV4()
	next := uuid.NewV4()

	now := chrono.Norm(time.Now().UTC())

	s := &Segment{
		UUID:        &id,
		Transaction: 10,
		Domain:      "testing",
		Time:        now,
		Blocks:      5,
		Count:       4800,
		Bytes:       460800,
		Next:        &next,
		Base:        &next,
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		SetSegment(engine, "testing", s)
	}
}

func BenchmarkGetSegment(b *testing.B) {
	engine, _ := origins.Init("memory", nil)

	id := uuid.NewV4()
	next := uuid.NewV4()

	now := chrono.Norm(time.Now().UTC())

	s := &Segment{
		UUID:        &id,
		Transaction: 10,
		Domain:      "testing",
		Time:        now,
		Blocks:      5,
		Count:       4800,
		Bytes:       460800,
		Next:        &next,
		Base:        &next,
	}

	SetSegment(engine, "testing", s)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		GetSegment(engine, "testing", &id)
	}
}

func BenchmarkSetBlock(b *testing.B) {
	engine, _ := origins.Init("memory", nil)

	id := uuid.NewV4()
	idx := 0

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

	for i := 0; i < 1000; i++ {
		encoder.Write(f)
	}

	buf := encoder.Bytes()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		SetBlock(engine, "testing", &id, idx, buf)
	}
}

func BenchmarkGetBlock(b *testing.B) {
	engine, _ := origins.Init("memory", nil)

	id := uuid.NewV4()
	idx := 0

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

	for i := 0; i < 1000; i++ {
		encoder.Write(f)
	}

	SetBlock(engine, "testing", &id, idx, encoder.Bytes())

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		GetBlock(engine, "testing", &id, idx)
	}
}
