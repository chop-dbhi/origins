package io

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/chop-dbhi/origins"
	"github.com/stretchr/testify/assert"
)

func TestJSONStreamReader(t *testing.T) {
	records := []map[string]interface{}{
		map[string]interface{}{
			"entity": map[string]string{
				"domain": "test",
				"name":   "alice",
			},
			"attribute": map[string]string{
				"domain": "test",
				"name":   "likes",
			},
			"value": map[string]string{
				"domain": "test",
				"name":   "bob",
			},
			"time": "2013-02-03",
		},
		map[string]interface{}{
			"entity": map[string]string{
				"domain": "test",
				"name":   "bob",
			},
			"attribute": map[string]string{
				"domain": "test",
				"name":   "knows",
			},
			"value": map[string]string{
				"domain": "test",
				"name":   "alice",
			},
		},
		map[string]interface{}{
			"entity": map[string]string{
				"domain": "test",
				"name":   "bob",
			},
			"attribute": map[string]string{
				"domain": "test",
				"name":   "likes",
			},
			"value": map[string]string{
				"domain": "test",
				"name":   "jan",
			},
		},
		map[string]interface{}{
			"operation": "retract",
			"entity": map[string]string{
				"domain": "test",
				"name":   "alice",
			},
			"attribute": map[string]string{
				"domain": "test",
				"name":   "likes",
			},
			"value": map[string]string{
				"domain": "test",
				"name":   "bob",
			},
		},
	}

	// Construct the JSON payload
	buf := bytes.NewBuffer(nil)

	var (
		b    []byte
		l    = len(records)
		bdry = []byte{'\n'}
	)

	for i, v := range records {
		b, _ = json.Marshal(v)
		buf.Write(b)

		if i < l-1 {
			buf.Write(bdry)
		}
	}

	r := NewJSONStreamReader(buf)

	facts, err := origins.ReadAll(r)

	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, len(facts), 4)
}

func BenchmarkJSONStreamParse(b *testing.B) {
	reader := JSONStreamReader{
		buf: make([]byte, 200),
	}

	v := map[string]interface{}{
		"domain":    "test",
		"operation": "assert",
		"time":      "2013-02-03",
		"entity": map[string]string{
			"domain": "test",
			"name":   "alice",
		},
		"attribute": map[string]string{
			"domain": "",
			"name":   "likes",
		},
		"value_domain": "test",
		"value":        "bob",
	}

	doc, _ := json.Marshal(v)

	for i := 0; i < b.N; i++ {
		reader.parse(doc)
	}
}
