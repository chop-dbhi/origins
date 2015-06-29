package schema

import (
	"bytes"
	"testing"

	"github.com/chop-dbhi/origins"
	"github.com/chop-dbhi/origins/storage"
	"github.com/chop-dbhi/origins/testutil"
	"github.com/chop-dbhi/origins/transactor"
	"github.com/stretchr/testify/assert"
)

func setup() storage.Engine {
	engine, _ := origins.Init("memory", nil)

	data, _ := testutil.Asset("assets/origins.csv")

	iter := origins.NewCSVReader(bytes.NewBuffer(data))

	tx, _ := transactor.New(engine, transactor.Options{})

	// Write the facts.
	if _, err := origins.Copy(iter, tx); err != nil {
		panic(err)
	}

	tx.Commit()

	return engine
}

func TestInitSchema(t *testing.T) {
	data, _ := testutil.Asset("assets/origins.csv")

	iter := origins.NewCSVReader(bytes.NewBuffer(data))

	schema := Init("origins.attrs", iter)

	attrs := schema.Attrs()

	assert.Equal(t, 18, len(attrs))
}

func TestLoadSchema(t *testing.T) {
	engine := setup()

	schema, err := Load(engine, "origins.attrs")

	if err != nil {
		t.Fatal(err)
	}

	attrs := schema.Attrs()

	assert.Equal(t, 6, len(attrs))
}
