package fact

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestJSONStreamReader(t *testing.T) {
	records := []map[string]interface{}{
		map[string]interface{}{
			"entity": map[string]string{
				"domain": "test",
				"local":  "alice",
			},
			"attribute": map[string]string{
				"domain": "test",
				"local":  "likes",
			},
			"value": map[string]string{
				"domain": "test",
				"local":  "bob",
			},
			"time": "2013-02-03",
		},
		map[string]interface{}{
			"entity": map[string]string{
				"domain": "test",
				"local":  "bob",
			},
			"attribute": map[string]string{
				"domain": "test",
				"local":  "knows",
			},
			"value": map[string]string{
				"domain": "test",
				"local":  "alice",
			},
		},
		map[string]interface{}{
			"entity": map[string]string{
				"domain": "test",
				"local":  "bob",
			},
			"attribute": map[string]string{
				"domain": "test",
				"local":  "likes",
			},
			"value": map[string]string{
				"domain": "test",
				"local":  "jan",
			},
		},
		map[string]interface{}{
			"operation": "retract",
			"entity": map[string]string{
				"domain": "test",
				"local":  "alice",
			},
			"attribute": map[string]string{
				"domain": "test",
				"local":  "likes",
			},
			"value": map[string]string{
				"domain": "test",
				"local":  "bob",
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

	r := JSONStreamReader(buf)

	facts, err := ReadAll(r)

	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, len(facts), 4)
}
