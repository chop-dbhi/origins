package fact

import (
	"io"
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
