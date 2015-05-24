package chrono

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
		assert.Equal(t, ts, TimeMicro(tm))

		if !tm.Equal(MicroTime(ts)) {
			t.Errorf("time: expected %s, got %s", tm, MicroTime(ts))
		}
	}
}

func TestParse(t *testing.T) {
	var (
		s    string
		p, x time.Time
		err  error
	)

	// Local time
	now := time.Now()

	times := map[string]time.Time{
		"5m":     now.Add(time.Duration(time.Minute * 5)),
		"-0h":    now.Add(-time.Duration(time.Hour * 0)),
		"-48h5m": now.Add(-time.Duration(time.Hour*48 + time.Minute*5)),

		// UTC
		"2013-04-10":                     time.Date(2013, 4, 10, 0, 0, 0, 0, time.UTC),
		"April 4, 2013":                  time.Date(2013, 4, 4, 0, 0, 0, 0, time.UTC),
		"Apr 04, 2013":                   time.Date(2013, 4, 4, 0, 0, 0, 0, time.UTC),
		"47065363200000000":              time.Date(1492, 6, 11, 0, 0, 0, 0, time.UTC),
		"02-01-2006":                     time.Date(2006, 1, 2, 0, 0, 0, 0, time.UTC),
		"02-01-2006 2:04 PM":             time.Date(2006, 1, 2, 14, 4, 0, 0, time.UTC),
		"02-01-2006 2:04 PM -0700":       time.Date(2006, 1, 2, 21, 4, 0, 0, time.UTC),
		"02-01-2006 2:04 PM -07:00":      time.Date(2006, 1, 2, 21, 4, 0, 0, time.UTC),
		"2 January 2006":                 time.Date(2006, 1, 2, 0, 0, 0, 0, time.UTC),
		"2 January 2006 3:04 PM":         time.Date(2006, 1, 2, 15, 4, 0, 0, time.UTC),
		"2 January 2006 3:04 PM -0700":   time.Date(2006, 1, 2, 22, 4, 0, 0, time.UTC),
		"2 January 2006 3:04 PM -07:00":  time.Date(2006, 1, 2, 22, 4, 0, 0, time.UTC),
		"2006-01-02":                     time.Date(2006, 1, 2, 0, 0, 0, 0, time.UTC),
		"2006-01-02 3:04 PM":             time.Date(2006, 1, 2, 15, 4, 0, 0, time.UTC),
		"2006-01-02 3:04 PM -0700":       time.Date(2006, 1, 2, 22, 4, 0, 0, time.UTC),
		"2006-01-02 3:04 PM -07:00":      time.Date(2006, 1, 2, 22, 4, 0, 0, time.UTC),
		"January 2, 2006":                time.Date(2006, 1, 2, 0, 0, 0, 0, time.UTC),
		"January 2, 2006 3:04 PM":        time.Date(2006, 1, 2, 15, 4, 0, 0, time.UTC),
		"January 2, 2006 3:04 PM -0700":  time.Date(2006, 1, 2, 22, 4, 0, 0, time.UTC),
		"January 2, 2006 3:04 PM -07:00": time.Date(2006, 1, 2, 22, 4, 0, 0, time.UTC),
		"Jan 2, 2006":                    time.Date(2006, 1, 2, 0, 0, 0, 0, time.UTC),
		"Jan 2, 2006, 3:04 PM":           time.Date(2006, 1, 2, 15, 4, 0, 0, time.UTC),
		"Jan 2, 2006 3:04 PM -0700":      time.Date(2006, 1, 2, 22, 4, 0, 0, time.UTC),
		"Jan 2, 2006 3:04 PM -07:00":     time.Date(2006, 1, 2, 22, 4, 0, 0, time.UTC),
	}

	// Duration to truncate for comparison.
	td := time.Second

	for s, x = range times {
		p, err = Parse(s)

		if err != nil {
			t.Errorf("time: failed to parse %s as time", s)
		} else {
			x = x.Truncate(td)
			p = p.Truncate(td)

			if !p.Equal(x) {
				t.Errorf("time: expected %s, got %s", x, p)
			}
		}
	}
}

func BenchmarkMicroTime(b *testing.B) {
	t := TimeMicro(time.Date(1492, 6, 11, 0, 0, 0, 0, time.UTC))

	for i := 0; i < b.N; i++ {
		MicroTime(t)
	}
}

func BenchmarkTimeMicro(b *testing.B) {
	t := time.Date(1492, 6, 11, 0, 0, 0, 0, time.UTC)

	for i := 0; i < b.N; i++ {
		TimeMicro(t)
	}
}

func BenchmarkParse__Time(b *testing.B) {
	t := "April 4, 2013"

	for i := 0; i < b.N; i++ {
		Parse(t)
	}
}

func BenchmarkParse__Duration(b *testing.B) {
	t := "-48h32m"

	for i := 0; i < b.N; i++ {
		Parse(t)
	}
}

func BenchmarkParse__Timestamp(b *testing.B) {
	t := "63592300800000000"

	for i := 0; i < b.N; i++ {
		Parse(t)
	}
}
