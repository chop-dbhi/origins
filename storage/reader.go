package storage

import (
	"encoding/binary"
	"io"

	"github.com/chop-dbhi/origins/fact"
	"github.com/sirupsen/logrus"
)

type Reader struct {
	// Time range
	Min int64
	Max int64

	// Reference to the store.
	store *Store

	// The partition being read from.
	reader *partitionReader
}

func (r *Reader) MinTime() int64 {
	return r.reader.MinTime()
}

func (r *Reader) MaxTime() int64 {
	return r.reader.MaxTime()
}

func (r *Reader) StartTime() int64 {
	return r.reader.StartTime()
}

func (r *Reader) EndTime() int64 {
	return r.reader.EndTime()
}

// Read satisfies the fact.Reader interface.
func (r *Reader) Read(facts fact.Facts) (int, error) {
	// Initialize
	if facts == nil {
		logrus.Panicf("Facts not initialized")
	}

	var (
		// Number of facts decoded.
		i int
		// Number of bytes read
		n int

		err error

		// Binary error code
		berr int

		// Size of the fact in bytes.
		size uint64

		// 2 byte length prefix per fact.
		prefix = make([]byte, 2, 2)

		// Fact buffer
		buf []byte
	)

	for ; i < len(facts); i++ {
		n, err = r.reader.Read(prefix)

		if err == io.EOF {
			return i, err
		}

		if err != nil {
			return 0, err
		}

		if n != framePrefixFactSize {
			logrus.Panicf("Frame prefix should be %d, got %d", framePrefixFactSize, n)
		}

		size, berr = binary.Uvarint(prefix)

		if berr < 0 {
			logrus.Panicf("Cannot decode frame prefix: uvarint code %d", berr)
		}

		buf = make([]byte, size)

		n, err = r.reader.Read(buf)

		if err == io.EOF {
			logrus.Panicf("Got unexpected EOF after %d bytes, expected %d", n, size)
		}

		if err != nil {
			return i, err
		}

		if size != uint64(n) {
			logrus.Panicf("Frame size should be %d, got %d", size, n)
		}

		f := fact.Fact{}

		if err = UnmarshalProto(buf, &f); err != nil {
			return i, err
		}

		facts[i] = &f
	}

	return i, nil
}
