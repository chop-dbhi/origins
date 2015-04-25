package fact

// Facts is a slice of facts.
type Facts []*Fact

// Concat takes multiple fact slices and concatenates them together.
func Concat(fs ...Facts) Facts {
	var total, off int

	for _, s := range fs {
		total += len(s)
	}

	dest := make(Facts, total)

	for _, s := range fs {
		// Copy into an offset slice.
		copy(dest[off:], s)
		off += len(s)
	}

	return dest
}
