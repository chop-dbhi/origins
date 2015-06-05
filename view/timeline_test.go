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

func TestTimeline(t *testing.T) {
	buf := bytes.NewBufferString(testData)

	csv := origins.CSVReader(buf)

	engine, err := origins.Init("memory", nil)

	tx, err := transactor.New(engine, transactor.Options{
		DefaultDomain: "users",
	})

	if err = tx.Consume(csv); err != nil {
		t.Fatal(err)
	}

	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}

	log, err := OpenLog(engine, "users", "commit")

	if err != nil {
		t.Fatal(err)
	}

	events, err := Timeline(log.Now(), Ascending)

	if err != nil {
		t.Fatal(err)
	}

	if len(events) != 6 {
		t.Errorf("expected 6 events, got %d", len(events))
	}
}
