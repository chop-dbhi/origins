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

// sortBy implements the sort.Interface to support sorting facts
// in place given a comparator.
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

// Timsort performs an in-place sort of the facts using the Timsort algorithm.
func Timsort(facts Facts, comp Comparator) {
	s := sortBy{
		facts: facts,
		comp:  comp,
	}

	timsort.TimSort(&s)
}
