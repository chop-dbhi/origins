package origins

import (
	"errors"
	"testing"
)

var (
	people    = "people"
	relations = "relations"

	bill  = &Ident{"people", "bill"}
	jane  = &Ident{"people", "jane"}
	likes = &Ident{"likes", "kale"}

	kale = &Ident{"", "kale"}

	f0 = Fact{
		Domain:    people,
		Entity:    bill,
		Attribute: likes,
		Value:     kale,
	}

	f1 = Fact{
		Domain:    people,
		Entity:    jane,
		Attribute: likes,
		Value:     kale,
	}

	f2 = Fact{
		Domain:    relations,
		Entity:    bill,
		Attribute: likes,
		Value:     jane,
	}
)

func TestRouter(t *testing.T) {
	hd := Transaction{}
	rt := NewRouter(&hd)

	var err error

	if err = rt.Route(&f0); err != nil {
		t.Errorf("router: %s", err)
	}

	if err = rt.Route(&f1); err != nil {
		t.Errorf("router: %s", err)
	}

	rt.Abort(errors.New("testing"))

	if err = rt.Route(&f2); err == nil {
		t.Errorf("router: expected timeout")
	}
}

func TestRouteReader(t *testing.T) {
	facts := Facts{&f0, &f1, &f2}

	r := NewBuffer(facts)

	hd := Transaction{}
	err := RouteReader(r, &hd)

	if err != nil {
		t.Errorf("router: %s", err)
	}
}
