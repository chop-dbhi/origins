package view

import "github.com/chop-dbhi/origins"

func uniqueIdents(iter origins.Iterator, identer func(*origins.Fact) *origins.Ident) (origins.Idents, error) {
	var (
		ok     bool
		err    error
		key    [2]string
		fact   *origins.Fact
		ident  *origins.Ident
		idents origins.Idents
	)

	seen := make(map[[2]string]struct{})

	for {
		if fact = iter.Next(); fact == nil {
			break
		}

		// Get the identity.
		ident = identer(fact)

		key[0] = ident.Domain
		key[1] = ident.Name

		if _, ok = seen[key]; !ok {
			seen[key] = struct{}{}
			idents = append(idents, ident)
		}
	}

	if err = iter.Err(); err != nil {
		return nil, err
	}

	return idents, nil
}

// Entities extract a unique set of entity identities from the iterator.
func Entities(iter origins.Iterator) (origins.Idents, error) {
	return uniqueIdents(iter, func(fact *origins.Fact) *origins.Ident {
		return fact.Entity
	})
}

// Attributes extract a unique set of attribute identities from the iterator.
func Attributes(iter origins.Iterator) (origins.Idents, error) {
	return uniqueIdents(iter, func(fact *origins.Fact) *origins.Ident {
		return fact.Attribute
	})
}

// Values extract a unique set of values identities from the iterator.
func Values(iter origins.Iterator) (origins.Idents, error) {
	return uniqueIdents(iter, func(fact *origins.Fact) *origins.Ident {
		return fact.Value
	})
}

// Transactions extract a unique set of transaction IDs from the iterator.
func Transactions(iter origins.Iterator) ([]uint64, error) {
	var (
		ok   bool
		err  error
		fact *origins.Fact
		tx   uint64
		txes []uint64
	)

	seen := make(map[uint64]struct{})

	for {
		if fact = iter.Next(); fact == nil {
			break
		}

		tx = fact.Transaction

		if _, ok = seen[tx]; !ok {
			seen[tx] = struct{}{}
			txes = append(txes, tx)
		}
	}

	if err = iter.Err(); err != nil {
		return nil, err
	}

	return txes, nil
}

type filterer struct {
	iter   origins.Iterator
	filter func(*origins.Fact) bool
}

func (f *filterer) Next() *origins.Fact {
	var fact *origins.Fact

	for {
		if fact = f.iter.Next(); fact == nil {
			break
		}

		// Fact matches, return.
		if f.filter(fact) {
			return fact
		}
	}

	return nil
}

func (f *filterer) Err() error {
	return f.iter.Err()
}

// FilterFunc is function type that takes a fact and returns true if the fact matches.
type FilterFunc func(*origins.Fact) bool

// Filter filters facts consumed from the iterator and returns an iterator.
func Filter(iter origins.Iterator, filter FilterFunc) origins.Iterator {
	return &filterer{
		iter:   iter,
		filter: filter,
	}
}

// Entity takes an iterator and returns the all facts about an entity.
func Entity(iter origins.Iterator, id *origins.Ident) origins.Iterator {
	return &filterer{
		iter: iter,
		filter: func(f *origins.Fact) bool {
			return f.Entity.Is(id)
		},
	}
}

// Exists takes an iterator and filter function and returns true if
// the predicate matches.
func Exists(iter origins.Iterator, filter FilterFunc) bool {
	f := Filter(iter, filter)

	fact := f.Next()

	return fact != nil
}
