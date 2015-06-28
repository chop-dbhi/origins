package view

import (
	"bytes"
	"testing"

	"github.com/chop-dbhi/origins"
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

	return origins.NewCSVReader(buf)
}

func TestTimeline(t *testing.T) {
	// See utils_test for function.
	iter := buildIter(t)

	events, err := Timeline(iter, Ascending)

	if err != nil {
		t.Fatal(err)
	}

	if len(events) != 6 {
		t.Errorf("expected 6 events, got %d", len(events))
	}
}
