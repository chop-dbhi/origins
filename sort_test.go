package origins

import (
	"math/rand"
	"strconv"
	"time"
)

// Generates a set of facts consisting of n entities each with m attributes.
// Variability (r) defines how variable the attribute values are.
func generateSet(n, m int, r float32) Facts {
	if r <= 0 || r > 1 {
		panic("sort: variability must be between (0,1]")
	}

	// Fixed time
	t := time.Now().UTC()

	facts := make(Facts, n*m)

	vn := int(float32(n*m) * r)

	l := 0

	for i := 0; i < n; i++ {
		e := Ident{"test", strconv.Itoa(i)}

		// Shuffle order of attributes per entity to give it a bit
		// more permutation.
		for _, j := range rand.Perm(m) {
			a := Ident{"test", strconv.Itoa(j)}

			v := Ident{"test", strconv.Itoa(rand.Intn(vn))}

			f, _ := AssertForTime(e, a, v, t)
			facts[l] = f
			l += 1
		}
	}

	return facts
}
