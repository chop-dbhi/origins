package fact

import (
	"io"
	"strings"
	"testing"

	"github.com/chop-dbhi/origins/identity"
	"github.com/stretchr/testify/assert"
)

func TestFactReaderSmallBuf(t *testing.T) {
	e := identity.New("", "bill")
	a := identity.New("", "likes")
	v := identity.New("", "kale")

	facts := Facts{
		Assert(e, a, v),
		Assert(e, a, v),
		Assert(e, a, v),
	}

	r := NewReader(facts)

	buf := make(Facts, 2)

	var (
		n   int
		err error
	)

	n, err = r.Read(buf)

	assert.Equal(t, 2, n)
	assert.Equal(t, nil, err)

	n, err = r.Read(buf)

	assert.Equal(t, 1, n)

	if err != io.EOF {
		t.Error("expected EOF")
	}

	n, err = r.Read(buf)

	assert.Equal(t, 0, n)

	if err != io.EOF {
		t.Error("expected EOF")
	}
}

func TestFactReaderLargeBuf(t *testing.T) {
	e := identity.New("", "bill")
	a := identity.New("", "likes")
	v := identity.New("", "kale")

	facts := Facts{
		Assert(e, a, v),
		Assert(e, a, v),
		Assert(e, a, v),
	}

	r := NewReader(facts)

	buf := make(Facts, 4)

	var (
		n   int
		err error
	)

	n, err = r.Read(buf)

	assert.Equal(t, 3, n)

	if err != io.EOF {
		t.Error("expected EOF")
	}

	n, err = r.Read(buf)

	assert.Equal(t, 0, n)

	if err != io.EOF {
		t.Error("expected EOF")
	}
}

func TestReadAll(t *testing.T) {
	e := identity.New("", "bill")
	a := identity.New("", "likes")
	v := identity.New("", "kale")

	facts := Facts{
		Assert(e, a, v),
		Assert(e, a, v),
		Assert(e, a, v),
	}

	r := NewReader(facts)

	buf, err := ReadAll(r)

	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, 3, len(buf))
}

func TestReadFiltered(t *testing.T) {
	e := identity.New("", "bill")
	a := identity.New("", "likes")
	v1 := identity.New("", "kale")
	v2 := identity.New("", "pizza")
	v3 := identity.New("", "coffee")

	facts := Facts{
		Assert(e, a, v1),
		Assert(e, a, v2),
		Assert(e, a, v3),
	}

	r := NewReader(facts)

	r.Filter = func(f *Fact) bool {
		return strings.Contains(f.Value.Local, "e")
	}

	// kale and coffee..
	buf, err := ReadAll(r)

	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, 2, len(buf))
	assert.Equal(t, buf[0].Value.Local, "kale")
	assert.Equal(t, buf[1].Value.Local, "coffee")
}
