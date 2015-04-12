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
