package origins

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTime(t *testing.T) {
	times := map[int64]time.Time{
		// Unix epoch
		62135596800000000: time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),

		// Min time
		0: time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC),

		// Max time
		9223372022400000000: time.Date(292278, 1, 10, 0, 0, 0, 0, time.UTC),

		// Leap year
		63592300800000000: time.Date(2016, 2, 29, 0, 0, 0, 0, time.UTC),

		// Other
		61819977600000000: time.Date(1960, 1, 1, 0, 0, 0, 0, time.UTC),
		47065363200000000: time.Date(1492, 6, 11, 0, 0, 0, 0, time.UTC),
	}

	for ts, tm := range times {
		assert.Equal(t, ts, FromTime(tm))
		assert.Equal(t, tm, ToTime(ts))
	}
}

func TestParseTime(t *testing.T) {
	var (
		s    string
		i    int64
		x, v time.Time
		err  error
	)

	now := time.Now()

	times := map[string]time.Time{
		"5m":                now.Add(time.Duration(time.Minute * 5)),
		"-0h":               now.Add(-time.Duration(time.Hour * 0)),
		"-48h5m":            now.Add(-time.Duration(time.Hour*48 + time.Minute*5)),
		"2013-04-10":        time.Date(2013, 4, 10, 0, 0, 0, 0, time.UTC),
		"April 4, 2013":     time.Date(2013, 4, 4, 0, 0, 0, 0, time.UTC),
		"Apr 04, 2013":      time.Date(2013, 4, 4, 0, 0, 0, 0, time.UTC),
		"47065363200000000": time.Date(1492, 6, 11, 0, 0, 0, 0, time.UTC),
	}

	// Duration to truncate for comparison.
	td := time.Duration(time.Second)

	for s, x = range times {
		i, err = ParseTime(s)

		if err != nil {
			t.Errorf("time: failed to parse %s as time", s)
		} else {
			x = x.Truncate(td)
			v = ToTime(i).Truncate(td)

			if !v.Equal(x) {
				t.Errorf("time: expected %s, got %s", x, v)
			}
		}
	}
}

func BenchmarkToTime(b *testing.B) {
	t := FromTime(time.Date(1492, 6, 11, 0, 0, 0, 0, time.UTC))

	for i := 0; i < b.N; i++ {
		ToTime(t)
	}
}

func BenchmarkFromTime(b *testing.B) {
	t := time.Date(1492, 6, 11, 0, 0, 0, 0, time.UTC)

	for i := 0; i < b.N; i++ {
		FromTime(t)
	}
}

func BenchmarkParseTime__Time(b *testing.B) {
	t := "April 4, 2013"

	for i := 0; i < b.N; i++ {
		ParseTime(t)
	}
}

func BenchmarkParseTime__Duration(b *testing.B) {
	t := "-48h32m"

	for i := 0; i < b.N; i++ {
		ParseTime(t)
	}
}

func BenchmarkParseTime__Timestamp(b *testing.B) {
	t := "63592300800000000"

	for i := 0; i < b.N; i++ {
		ParseTime(t)
	}
}
