package view

import (
	"bytes"
	"encoding/binary"

	"github.com/chop-dbhi/origins"
	"github.com/chop-dbhi/origins/chrono"
	"github.com/chop-dbhi/origins/pb"
	"github.com/golang/protobuf/proto"
	"github.com/sirupsen/logrus"
)

// A slice of facts use length-prefix framing to delimit facts.
// See http://eli.thegreenplace.net/2011/08/02/length-prefix-framing-for-protocol-buffers
const (
	// Maximum fact size in bytes. The size is encoded and used as the boundary
	// for length-prefix framing.
	maxFactSize = 64 * 1024

	// Size in bytes of the fact prefix which stores the length of the fact in bytes.
	factPrefixSize = 2
)

func unmarshalSegment(b []byte, s *segment) error {
	m := pb.Segment{}

	if err := proto.Unmarshal(b, &m); err != nil {
		return err
	}

	s.ID = m.GetID()
	s.Blocks = int(m.GetBlocks())
	s.Count = int(m.GetCount())
	s.Bytes = int(m.GetBytes())
	s.Next = m.GetNext()
	s.Base = m.GetBase()

	return nil
}

func unmarshalLog(b []byte, l *Log) error {
	m := pb.Log{}

	if err := proto.Unmarshal(b, &m); err != nil {
		return err
	}

	l.head = m.GetHead()

	return nil
}

// unmarshalFact decodes a fact from it's binary representation. The domain and
// transaction ID are passed in since they are not encoded with the fact itself.
// This is because facts are stored relative to a domain and a transaction.
func unmarshalFact(b []byte, d string, t uint64, f *origins.Fact) error {
	m := pb.Fact{}

	if err := proto.Unmarshal(b, &m); err != nil {
		return err
	}

	f.Domain = d
	f.Transaction = t

	f.Entity = &origins.Ident{
		Domain: m.GetEntityDomain(),
		Name:   m.GetEntity(),
	}

	f.Attribute = &origins.Ident{
		Domain: m.GetAttributeDomain(),
		Name:   m.GetAttribute(),
	}

	f.Value = &origins.Ident{
		Domain: m.GetValueDomain(),
		Name:   m.GetValue(),
	}

	f.Time = chrono.MicroTime(m.GetTime())

	if m.GetAdded() {
		f.Operation = origins.Assertion
	} else {
		f.Operation = origins.Retraction
	}

	return nil
}

// Unmarshal decodes facts from it's binary representation.
func unmarshalFacts(block []byte, d string, t uint64) (origins.Facts, error) {
	var (
		n       int
		err     error
		size    uint64
		errcode int
		fact    *origins.Fact
	)

	// Initialize a buffer of facts.
	facts := origins.NewBuffer(nil)

	// Initialize one buffer with the maximum fact size. A slice of the correct
	// size will be passed into Read to get the expected size.
	buf := make([]byte, maxFactSize)

	// Wrap in buffer to make it easier to read.
	reader := bytes.NewBuffer(block)

	for {
		n, err = reader.Read(buf[:factPrefixSize])

		// No more bytes.
		if n == 0 {
			break
		}

		if n != factPrefixSize {
			logrus.Panicf("unmarshal: fact expected %d bytes, got %d", factPrefixSize, n)
		}

		// Decode the size of the fact.
		if size, errcode = binary.Uvarint(buf[:factPrefixSize]); errcode <= 0 {
			logrus.Panic("unmarshal: could not decode fact size")
		}

		// Read the next bytes for the fact itself.
		n, err = reader.Read(buf[:size])

		if uint64(n) != size {
			logrus.Panicf("unmarshal: fact expected to read %d bytes, got %d", size, n)
		}

		fact = &origins.Fact{}

		if err = unmarshalFact(buf[:size], d, t, fact); err != nil {
			return nil, err
		}

		// Add it to the buffer.
		facts.Append(fact)
	}

	return facts.Facts(), nil
}
