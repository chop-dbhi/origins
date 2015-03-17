package memory

import (
	"encoding/json"
	"testing"

	"github.com/chop-dbhi/origins/fact"
	"github.com/chop-dbhi/origins/identity"
)

func TestStore(t *testing.T) {
	store, _ := Open(nil)

	k := "data"

	facts := fact.Facts{
		fact.Assert(identity.MustParse("test:i1"), nil, nil),
		fact.Assert(identity.MustParse("test:i2"), nil, nil),
	}

	b, err := json.Marshal(&facts)

	if err != nil {
		t.Fatal(err)
	}

	if err := store.Set(k, b); err != nil {
		t.Fatal(err)
	}

	b, err = store.Get(k)

	if err != nil {
		t.Fatal(err)
	}

	if err := json.Unmarshal(b, &facts); err != nil {
		t.Fatal(err)
	}

	if len(facts) != 2 {
		t.Fatalf("length is %d", len(facts))
	}

	f1 := facts[0]
	f2 := facts[1]

	if f1.Entity.Local == "i2" && f2.Entity.Local != "i2" {
		t.Fatalf("bad data %v", facts)
	}
}
