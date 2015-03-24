package fact

import (
	"errors"
	"fmt"
	"time"
)

var timeLayouts = []string{
	"2006-01-02",
	"2006-01-02 3:04 PM",
	"Jan _2, 2006",
	"January _2, 2006",
	time.RFC3339,
	time.RFC1123,
	time.RFC1123Z,
	time.RFC822,
	time.RFC822Z,
	time.ANSIC,
}

// ParseTime parses a string, duration, or time into an int64 of nanoseconds.
func ParseTime(v interface{}) (int64, error) {
	switch x := v.(type) {
	case string:
		var (
			t   time.Time
			err error
		)

		// Absolute time
		for _, layout := range timeLayouts {
			t, err = time.Parse(layout, x)

			if err == nil {
				return t.UnixNano(), nil
			}
		}

		// Duration
		d, err := time.ParseDuration(x)

		if err == nil {
			n := time.Now()
			n = n.Add(-d)
			return n.UnixNano(), nil
		}
	case time.Duration:
		n := time.Now()
		n = n.Add(-x)
		return n.UnixNano(), nil
	case time.Time:
		return x.UnixNano(), nil
	}

	return 0, errors.New(fmt.Sprintf("could not parse %v as time", v))
}

// MustParseTime parses the passed time value or panics.
func MustParseTime(v interface{}) int64 {
	n, err := ParseTime(v)

	if err != nil {
		panic(err)
	}

	return n
}
