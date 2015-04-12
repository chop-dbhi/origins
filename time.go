package origins

import "time"

const (
	secondsPerDay = 86400
	usPerSecond   = 1000000
	usPerNano     = 1000
)

var zeroTime = time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)

// ToTime coverts a microsecond resolution timestamp into a time.Time value.
func ToTime(ts int64) time.Time {
	// Truncate the floating point which will be the nanosecond resolution.
	days := int(ts / usPerSecond / secondsPerDay)

	return zeroTime.AddDate(0, 0, days)
}

// FromTime converts a time.Time value in a microsecond resolution timestamp.
func FromTime(t time.Time) int64 {
	yr := int64(t.Year() - 1)

	// Elapsed taking in to account leap years.
	elapsedDays := int64(yr*365+yr/4-yr/100+yr/400) + int64(t.YearDay()) - 1

	// Remaining seconds.
	elapsedSeconds := (elapsedDays*secondsPerDay + int64(t.Hour())*3600 + int64(t.Minute())*60 + int64(t.Second()))

	return int64(elapsedSeconds*usPerSecond + int64(t.Nanosecond())/usPerNano)
}
