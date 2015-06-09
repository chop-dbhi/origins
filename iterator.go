package origins

import "io"

// Iterator is an interface that defines the Next method. When the iterator
// is exhausted, the fact will be nil.
type Iterator interface {
	Next() (*Fact, error)
}

func ReadIterAll(iter Iterator) ([]*Fact, error) {
	var (
		err   error
		fact  *Fact
		facts Facts
	)

	for {
		fact, err = iter.Next()

		if fact != nil {
			facts = append(facts, fact)
		}

		if err != nil {
			if err == io.EOF {
				break
			}

			return nil, err
		}
	}

	return facts, nil
}
