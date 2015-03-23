package fact

import (
	"testing"
	"time"
)

func TestParseTime(t *testing.T) {
	var err error

	vals := []interface{}{
		"1h",
		"1s",
		time.Now(),
		time.Minute,
		"2013-04-10",
	}

	for _, v := range vals {
		_, err = ParseTime(v)

		if err != nil {
			t.Errorf("failed to parse %v as time", v)
		}
	}

	// not supported; but maybe should be..
	vals = []interface{}{
		"1d",
		"1y",
	}

	for _, v := range vals {
		_, err = ParseTime(v)

		if err == nil {
			t.Errorf("did not expect to parse %v as time", v)
		}
	}
}
