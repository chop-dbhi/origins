// Time is interpreted and stored at a microsecond-level resolution. This
// enables times ranging from January 1, 0001 (zero time) to January 10, 292278.
package origins

import (
	"errors"
	"fmt"
	"strconv"
	"time"
)

const (
	secondsPerDay = 86400
	usPerSecond   = 1000000
	usPerNano     = 1000
)

var (
	zeroTime = time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)

	timeLayouts = []string{
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
)

// DiffTime returns the difference between two timestamps in
// as a time.Duration to interop with the time package.
func DiffTime(t1, t2 int64) time.Duration {
	return time.Duration((t1 - t2) * 1000)
}

// ToTime coverts a microsecond resolution timestamp into a time.Time value.
func ToTime(ts int64) time.Time {
	// Integer division will truncate the floating point.
	days := ts / secondsPerDay / usPerSecond

	// Get the remaining microseconds
	us := ts - days*secondsPerDay*usPerSecond

	// Add the days
	t := zeroTime.AddDate(0, 0, int(days))

	// Add remaining microseconds. Convert to local time.
	return t.Add(time.Duration(us) * time.Microsecond).Local()
}

// FromTime converts a time.Time value in a microsecond resolution timestamp.
func FromTime(t time.Time) int64 {
	// Ensure the time is in UTC
	t = t.UTC()

	yr := int64(t.Year() - 1)

	// Elapsed taking in to account leap years.
	elapsedDays := int64(yr*365+yr/4-yr/100+yr/400) + int64(t.YearDay()) - 1

	// Remaining seconds.
	elapsedSeconds := (elapsedDays*secondsPerDay + int64(t.Hour())*3600 + int64(t.Minute())*60 + int64(t.Second()))

	return int64(elapsedSeconds*usPerSecond + int64(t.Nanosecond())/usPerNano)
}

// ParseTime parses a string into a timestamp. The string may represent an
// absolute time, duration, or timestamp.
func ParseTime(s string) (int64, error) {
	var (
		t   time.Time
		d   time.Duration
		err error
	)

	// Duration
	d, err = time.ParseDuration(s)

	if err == nil {
		// Apply duration relative to local time.
		t = time.Now().Add(d)
		return FromTime(t), nil
	}

	// Parse as local time.
	for _, layout := range timeLayouts {
		t, err = time.ParseInLocation(layout, s, time.Local)

		if err == nil {
			return FromTime(t), nil
		}
	}

	// Timestamp; assume this is UTC time.
	i, err := strconv.ParseInt(s, 10, 64)

	if err == nil {
		return i, nil
	}

	return 0, errors.New(fmt.Sprintf("time: could not parse %s", s))
}

// MustParseTime parses the passed time string or panics.
func MustParseTime(s string) int64 {
	t, err := ParseTime(s)

	if err != nil {
		panic(err)
	}

	return t
}
