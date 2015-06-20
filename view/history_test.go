package view

import (
	"bytes"
	"testing"

	"github.com/chop-dbhi/origins"
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

func TestHistory(t *testing.T) {
	buf := bytes.NewBufferString(testData)

	domain := "users"

	csv := origins.NewCSVReader(buf)

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

	iter := log.Now()

	history, err := History(engine, iter)

	if err != nil {
		t.Fatal(err)
	}

	if len(history) != 6 {
		t.Errorf("expected 6 revisions, got %d", len(history))
	}
}
