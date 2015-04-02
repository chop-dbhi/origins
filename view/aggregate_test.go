package view_test

import (
	"testing"

	"github.com/chop-dbhi/origins/fact"
	"github.com/chop-dbhi/origins/storage"
	"github.com/chop-dbhi/origins/storage/memory"
	"github.com/chop-dbhi/origins/transactor"
	"github.com/chop-dbhi/origins/view"
	"github.com/stretchr/testify/assert"
)

func TestAggregate(t *testing.T) {
	r := fact.NewReader(fact.Facts{
		fact.Assert(joe, livesIn, ny),
		fact.Retract(joe, livesIn, ny),
		fact.Assert(joe, livesIn, ca),
		fact.Retract(joe, livesIn, ca),
		fact.Assert(joe, livesIn, pa),
	})

	engine, _ := memory.Open(&storage.Options{})

	store, _ := storage.Init(&storage.Config{
		Engine: engine,
	})

	domain := "test"

	_, err := transactor.Commit(store, r, domain, true)

	if err != nil {
		panic(err)
	}

	v := view.Now(store)

	dv := v.Domain(domain)
	agg := dv.Aggregate("joe")

	m := agg.Map()

	val := m[livesIn.String()][0]
	assert.Equal(t, pa.Local, val.Local)
}
