package fact

import (
	"bytes"
	"os"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func init() {
	logrus.SetLevel(logrus.DebugLevel)
}

func TestCSVReader(t *testing.T) {
	// Comments and spaces to test parser, mixed case in header, underscores and spaces.
	csvString := `
		entity_domain,entity,ATTRIBUTE DOMAIN,attribute,value domain,value,operation,valid time

		test,bob,test,knows,test,alice
		test,bob,test,knows,test,joe,,2015-03-06
		test,alice,test,likes,test,bob

		# comment
		test,alice,test,likes,test,bob,retract

	`

	buf := bytes.NewBufferString(csvString)

	r, err := CSVReader(buf)

	if err != nil {
		t.Fatal(err)
	}

	facts, err := ReadAll(r)

	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, len(facts), 4)

	bk := facts[1]

	assert.Equal(t, bk.Entity.Domain, "test")
	assert.Equal(t, bk.Entity.Local, "bob")

	secs := time.Date(2015, 3, 6, 0, 0, 0, 0, time.UTC).Unix()

	assert.Equal(t, secs, bk.Time)
	assert.Equal(t, "retract", facts[3].Operation)
}

func TestCSVReaderFile(t *testing.T) {
	f, err := os.Open("../origins-domains.csv")

	if err != nil {
		t.Fatal(err)
	}

	defer f.Close()

	r, _ := CSVReader(f)
	facts, _ := ReadAll(r)

	assert.Equal(t, 72, len(facts))
}
