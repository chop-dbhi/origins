// Fact sorting is done using the Timsort algorithm which is hybrid
// algorithm of merge sort and insertion sort. This is chosen because facts
// are generally partially sorted by entity since facts are derived from higher
// level objects.
//
// For comparison, comparators for the default Quicksort algorithm
// are implemented for benchmarking purposes.
//
// Wikipedia: https://en.wikipedia.org/wiki/Timsort
// Comparison to quicksort: http://stackoverflow.com/a/19587279/407954
package origins

import (
	"sort"

	"github.com/psilva261/timsort"
)

type sortBy struct {
	facts Facts
	comp  Comparator
}

func (s *sortBy) Len() int {
	return len(s.facts)
}

func (s *sortBy) Swap(i, j int) {
	s.facts[i], s.facts[j] = s.facts[j], s.facts[i]
}

func (s *sortBy) Less(i, j int) bool {
	return s.comp(s.facts[i], s.facts[j])
}

// Sort performs an in-place sort of the facts using the built-in sort method.
func Sort(facts Facts, comp Comparator) {
	s := sortBy{
		facts: facts,
		comp:  comp,
	}

	sort.Sort(&s)
}

// interfaceFacts converts a slice of facts into a slice of interface values
// to be used with the timsort implementation.
func interfaceFacts(fs Facts) []interface{} {
	vs := make([]interface{}, len(fs), len(fs))

	for i, f := range fs {
		vs[i] = f
	}

	return vs
}

// interfaceComparator is a wrapper for a Comparator to take interface
// values for the Timsort implementation.
func interfaceComparator(c Comparator) func(v1, v2 interface{}) bool {
	return func(v1, v2 interface{}) bool {
		return c(v1.(*Fact), v2.(*Fact))
	}
}

// Timsort performs an in-place sort of the facts using the Timsort algorithm.
func Timsort(facts Facts, comp Comparator) {
	vals := interfaceFacts(facts)

	timsort.Sort(vals, interfaceComparator(comp))

	// Update original slice.
	for i, v := range vals {
		facts[i] = v.(*Fact)
	}
}
