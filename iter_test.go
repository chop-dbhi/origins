package origins

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

var testData = `
entity,attribute,value
bob,city,Norristown
sue,city,Bethlehem
bob,city,Bethlehem
bob,city,Philadelphia
bob,city,Allentown
sue,city,Allentown
`

func buildIter(t *testing.T) Iterator {
	buf := bytes.NewBufferString(testData)

	return NewCSVReader(buf)
}

func TestEntities(t *testing.T) {
	iter := buildIter(t)

	idents, err := Entities(iter)

	if err != nil {
		t.Error(err)
	}

	if len(idents) != 2 {
		t.Errorf("expected 2 entities, got %d", len(idents))
	}
}

func TestAttributes(t *testing.T) {
	iter := buildIter(t)

	idents, err := Attributes(iter)

	if err != nil {
		t.Error(err)
	}

	if len(idents) != 1 {
		t.Errorf("expected 1 attribute, got %d", len(idents))
	}
}

func TestValues(t *testing.T) {
	iter := buildIter(t)

	idents, err := Values(iter)

	if err != nil {
		t.Error(err)
	}

	if len(idents) != 4 {
		t.Errorf("expected 4 values, got %d", len(idents))
	}
}

func TestTransasctions(t *testing.T) {
	iter := buildIter(t)

	idents, err := Transactions(iter)

	if err != nil {
		t.Error(err)
	}

	if len(idents) != 1 {
		t.Errorf("expected 1 transaction, got %d", len(idents))
	}
}

func TestGroupby(t *testing.T) {
	iter := buildIter(t)

	g := Groupby(iter, func(a, b *Fact) bool {
		return a.Entity.Is(b.Entity)
	})

	var groups []Facts

	MapFacts(g, func(fs Facts) error {
		groups = append(groups, fs)
		return nil
	})

	assert.Equal(t, 4, len(groups))
	assert.Equal(t, 1, len(groups[0]))
	assert.Equal(t, 1, len(groups[1]))
	assert.Equal(t, 3, len(groups[2]))
	assert.Equal(t, 1, len(groups[3]))
}
