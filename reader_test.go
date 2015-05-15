package origins

import (
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFactReaderSmallBuf(t *testing.T) {
	e, _ := NewIdent("", "bill")
	a, _ := NewIdent("", "likes")
	v, _ := NewIdent("", "kale")

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
	e, _ := NewIdent("", "bill")
	a, _ := NewIdent("", "likes")
	v, _ := NewIdent("", "kale")

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
	e, _ := NewIdent("", "bill")
	a, _ := NewIdent("", "likes")
	v, _ := NewIdent("", "kale")

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
	e, _ := NewIdent("", "bill")
	a, _ := NewIdent("", "likes")
	v1, _ := NewIdent("", "kale")
	v2, _ := NewIdent("", "pizza")
	v3, _ := NewIdent("", "coffee")

	facts := Facts{
		Assert(e, a, v1),
		Assert(e, a, v2),
		Assert(e, a, v3),
	}

	r := NewReader(facts)

	r.Filter = func(f *Fact) bool {
		return strings.Contains(f.Value.Name, "e")
	}

	// kale and coffee..
	buf, err := ReadAll(r)

	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, 2, len(buf))
	assert.Equal(t, buf[0].Value.Name, "kale")
	assert.Equal(t, buf[1].Value.Name, "coffee")
}

func TestMultiReader(t *testing.T) {
	e, _ := NewIdent("", "bill")
	a, _ := NewIdent("", "likes")
	v1, _ := NewIdent("", "kale")
	v2, _ := NewIdent("", "pizza")
	v3, _ := NewIdent("", "coffee")

	facts := Facts{
		Assert(e, a, v1),
		Assert(e, a, v2),
		Assert(e, a, v3),
	}

	r1 := NewReader(facts)
	r2 := NewReader(facts)
	r3 := NewReader(facts)

	r := MultiReader(r1, r2, r3)

	buf, err := ReadAll(r)

	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, 9, len(buf))
}
