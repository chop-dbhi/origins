package view

import (
	"bytes"
	"testing"

	"github.com/chop-dbhi/origins"
	"github.com/chop-dbhi/origins/io"
	"github.com/chop-dbhi/origins/transactor"
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

func buildIter(t *testing.T) origins.Iterator {
	buf := bytes.NewBufferString(testData)

	domain := "users"

	csv := io.NewCSVReader(buf)

	engine, err := origins.Init("memory", nil)

	tx, err := transactor.New(engine, transactor.Options{
		DefaultDomain: domain,
	})

	if err = tx.Consume(csv); err != nil {
		t.Fatal(err)
	}

	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}

	log, err := OpenLog(engine, domain, "commit")

	if err != nil {
		t.Fatal(err)
	}

	return log.Now()
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
