package transactor

import "fmt"

type Results map[string]*Result

// Result contains information about facts being written to a store for a
// particular domain.
type Result struct {
	Store  string
	Domain string
	Time   int64
	Count  int
	Bytes  int
	Error  error
	Faked  bool
}

func (r *Result) String() string {
	var s string

	if r.Error != nil {
		s = fmt.Sprintf("[%s] Error: %s", r.Domain, r.Error)
	} else {
		s = fmt.Sprintf("[%s] %d facts (%d bytes) written", r.Domain, r.Count, r.Bytes)
	}

	if r.Faked {
		s += " (faked)"
	}

	return s
}
