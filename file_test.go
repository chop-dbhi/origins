package origins

import (
	"bytes"
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDetectFileFormat(t *testing.T) {
	tests := map[string]string{
		"foo.csv":            "csv",
		"foo.bar.csv":        "csv",
		"foo.bar.csv.gz":     "csv",
		"foo.jsonstream":     "jsonstream",
		"foo.bar.jsonstream": "jsonstream",
		"foo.jsonstream.gz":  "jsonstream",
		"foo.jsons":          "jsonstream",
		"foo.bar.jsons":      "jsonstream",
		"foo.jsons.gz":       "jsonstream",
	}

	for input, expected := range tests {
		assert.Equal(t, expected, DetectFileFormat(input))
	}
}

func TestUniversalReader(t *testing.T) {
	b := []byte("one\rtwo\n\rthree")
	buf := bytes.NewBuffer(b)

	r := NewUniversalReader(buf)

	b, _ = ioutil.ReadAll(r)
	assert.Equal(t, []byte("one\ntwo\n\nthree"), b)
}
