package fact

// Facts is a slice of facts.
type Facts []*Fact

// Extend extends the slice by doubling the size and returns the new size.
func (fs Facts) Extend() int {
	p := &fs
	n := len(fs) * 2
	t := make(Facts, n)
	copy(t, fs)
	*p = t
	return n
}
