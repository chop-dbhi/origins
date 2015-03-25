package fact

// Facts is a slice of facts.
type Facts []*Fact

// FactsByValidTime is a slice type of facts that implements the Sorter interface
// for sorting facts by valid time.
type FactsByValidTime []*Fact

func (f FactsByValidTime) Len() int {
	return len(f)
}

func (f FactsByValidTime) Swap(i, j int) {
	f[i], f[j] = f[j], f[i]
}

func (f FactsByValidTime) Less(i, j int) bool {
	return f[i].Time < f[j].Time
}
