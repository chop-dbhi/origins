package origins

import (
	"testing"
	"time"

	"github.com/chop-dbhi/origins/chrono"
	"github.com/stretchr/testify/assert"
)

type File struct {
	Name     string
	Size     int
	Modified time.Time
	Owner    string `origins:",,users"`
	Perm     int    `origins:"-"`
	other    bool
}

func TestReflect(t *testing.T) {
	now := time.Now()

	f := File{
		Name:     "test.csv",
		Size:     13432,
		Modified: now,
		Owner:    "joe",
		Perm:     0755,
		other:    true,
	}

	facts, err := Reflect(f)

	if err != nil {
		t.Fatal(err)
	}

	// Perm and other are ommitted.
	assert.Equal(t, 4, len(facts))

	// Inspect the facts.
	assert.Equal(t, "13432", facts[1].Value.Name)
	assert.Equal(t, chrono.Format(now), facts[2].Value.Name)
	assert.Equal(t, "owner", facts[3].Attribute.Name)
	assert.Equal(t, "users", facts[3].Value.Domain)
	assert.Equal(t, "joe", facts[3].Value.Name)
}
