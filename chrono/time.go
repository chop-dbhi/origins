package chrono

import (
	"fmt"
	"strconv"
	"time"
)

// Constants for calculating time in microsecond resolution.
const (
	secondsPerDay   = 86400
	microsPerSecond = 1000000
	microsPerNano   = 1000
)

// Time is interpreted and stored at a microsecond-level resolution. This
// enables times ranging from January 1, 0001 (zero time) to January 10, 292278.
// Time is parsed as UTC.
var (
	zero = time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)

	// TimeLayouts is a list of time layouts that are used when parsing
	// a time string.
	timeLayouts = []string{
		"02-01-2006",
		"02-01-2006 3:04 PM",
		"02-01-2006 3:04 PM -0700",
		"02-01-2006 3:04 PM -07:00",
		"_2 January 2006",
		"_2 January 2006 3:04 PM",
		"_2 January 2006 3:04 PM -0700",
		"_2 January 2006 3:04 PM -07:00",
		"2006-01-02",
		"2006-01-02 3:04 PM",
		"2006-01-02 3:04 PM -0700",
		"2006-01-02 3:04 PM -07:00",
		time.RFC1123,
		time.RFC1123Z,
		time.RFC822,
		time.RFC822Z,
		"January _2, 2006",
		"January _2, 2006 3:04 PM",
		"January _2, 2006 3:04 PM -0700",
		"January _2, 2006 3:04 PM -07:00",
		"Jan _2, 2006",
		"Jan _2, 2006, 3:04 PM",
		"Jan _2, 2006 3:04 PM -0700",
		"Jan _2, 2006 3:04 PM -07:00",
		time.RFC3339,
		time.ANSIC,
	}
)

// ToTime coverts a microsecond resolution timestamp into a time.Time value.
func MicroTime(ts int64) time.Time {
	// Integer division will truncate the floating point.
	days := ts / secondsPerDay / microsPerSecond

	// Get the remaining microseconds
	micros := ts - days*secondsPerDay*microsPerSecond

	// Add the days
	t := zero.AddDate(0, 0, int(days))

	// Add remaining microseconds.
	return t.Add(time.Duration(micros) * time.Microsecond)
}

// fromTime converts a time.Time value in a microsecond resolution timestamp.
func TimeMicro(t time.Time) int64 {
	// Ensure the time is in UTC
	t = t.UTC()

	yr := int64(t.Year() - 1)

	// Elapsed taking in to account leap years.
	elapsedDays := int64(yr*365+yr/4-yr/100+yr/400) + int64(t.YearDay()) - 1

	// Remaining seconds.
	elapsedSeconds := (elapsedDays*secondsPerDay + int64(t.Hour())*3600 + int64(t.Minute())*60 + int64(t.Second()))

	return int64(elapsedSeconds*microsPerSecond + int64(t.Nanosecond())/microsPerNano)
}

// Format returns a string representation of the time.
func Format(t time.Time) string {
	if t.IsZero() {
		return ""
	}

	return t.Format(time.RFC3339)
}

// Parse parses a string into a time value. The string may represent an
// absolute time, duration relative to the current time, or a microsecond-resolution
// timestamp. All times are converted to UTC.
func Parse(s string) (time.Time, error) {
	var (
		t   time.Time
		d   time.Duration
		err error
	)

	// Duration
	d, err = time.ParseDuration(s)

	if err == nil {
		return time.Now().UTC().Add(d), nil
	}

	// Parse time.
	for _, layout := range timeLayouts {
		t, err = time.Parse(layout, s)

		if err == nil {
			return t.UTC(), nil
		}
	}

	// Timestamp; assume this is UTC time.
	i, err := strconv.ParseInt(s, 10, 64)

	if err == nil {
		return MicroTime(i), nil
	}

	return zero, fmt.Errorf("time: could not parse %s", s)
}

// MustParse parses the passed time string or panics.
func MustParse(s string) time.Time {
	t, err := Parse(s)

	if err != nil {
		panic(err)
	}

	return t
}

// JSON returns an interface value for a time value intended to be used
// when preparing a custom value for JSON encoding. It uses nil for zero
// time values.
func JSON(t time.Time) interface{} {
	if t.IsZero() {
		return nil
	}

	return t
}
