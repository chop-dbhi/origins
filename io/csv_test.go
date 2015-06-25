package io

import (
	"bytes"
	"testing"
	"time"

	"github.com/chop-dbhi/origins"
	"github.com/stretchr/testify/assert"
)

func TestCSVReader(t *testing.T) {
	// Comments and spaces to test parser, mixed case in header, underscores and spaces.
	var csvString = `
		entity_domain,entity,ATTRIBUTE DOMAIN,attribute,value domain,value,operation,valid time

		test,bob,test,knows,test,alice
		test,bob,test,knows,test,joe,,2015-03-06
		test,alice,test,likes,test,bob

		# comment
		test,alice,test,likes,test,bob,retract
	`

	buf := bytes.NewBufferString(csvString)

	r := NewCSVReader(buf)

	facts, err := origins.ReadAll(r)

	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, len(facts), 4)

	bk := facts[1]

	assert.Equal(t, origins.Assertion, bk.Operation)
	assert.Equal(t, bk.Entity.Domain, "test")
	assert.Equal(t, bk.Entity.Name, "bob")

	assert.Equal(t, time.Date(2015, 3, 6, 0, 0, 0, 0, time.UTC), bk.Time)
	assert.Equal(t, origins.Retraction, facts[3].Operation)
}

// Benchmark parsing a single record. This is slightly misleading since subsequent
// iterations will use the `idents` cache, but this would be used during a parsing
// anyway.
func BenchmarkCSVParse(b *testing.B) {
	header, _ := parseHeader(csvHeader)

	reader := CSVReader{
		header: header,
	}

	record := []string{
		"people",
		"assert",
		"5",
		"2014-02-01",
		"people",
		"bob",
		"",
		"knows",
		"people",
		"jane",
	}

	for i := 0; i < b.N; i++ {
		reader.parse(record)
	}
}
