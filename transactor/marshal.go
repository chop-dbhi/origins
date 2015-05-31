package transactor

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/chop-dbhi/origins"
	"github.com/chop-dbhi/origins/chrono"
	"github.com/chop-dbhi/origins/pb"
	"github.com/golang/protobuf/proto"
	"github.com/sirupsen/logrus"
)

// A slice of facts use length-prefix framing to delimit facts.
// See http://eli.thegreenplace.net/2011/08/02/length-prefix-framing-for-protocol-buffers
const (
	// Size in bytes of the number of facts that can be encoded.
	frameHeaderSize = 2

	// Size of the fact header for framing.
	frameFactHeaderSize = 2

	// Maximum fact size in bytes. The size is encoded and used as the boundary
	// for length-prefix framing.
	maxFactSize = 1 << 16
)

func marshalFact(f *origins.Fact) ([]byte, error) {
	m := pb.Fact{}

	m.EntityDomain = proto.String(f.Entity.Domain)
	m.Entity = proto.String(f.Entity.Name)

	m.AttributeDomain = proto.String(f.Attribute.Domain)
	m.Attribute = proto.String(f.Attribute.Name)

	m.ValueDomain = proto.String(f.Value.Domain)
	m.Value = proto.String(f.Value.Name)

	m.Time = proto.Int64(chrono.TimeMicro(f.Time))

	switch f.Operation {
	case origins.Assertion:
		m.Added = proto.Bool(true)
	case origins.Retraction:
		m.Added = proto.Bool(false)
	default:
		panic("fact: invalid op")
	}

	return proto.Marshal(&m)
}

// Marshal encodes facts into it's binary representation.
func marshalFacts(fs origins.Facts) ([]byte, error) {
	var (
		err error

		// Fact buffer
		fb []byte

		// Size of the fact in bytes.
		size int

		// 2 byte length prefix per fact.
		header = make([]byte, frameHeaderSize, frameHeaderSize)

		// The full segment of bytes.
		buf []byte
	)

	// Marshal each individual fact.
	for _, f := range fs {
		fb, err = marshalFact(f)

		if err != nil {
			return nil, err
		}

		size = len(fb)

		if size > maxFactSize {
			err = errors.New(fmt.Sprintf("fact: size %d exceeds maximum allowed %d", size, maxFactSize))
			logrus.Error(err)
			return nil, err
		}

		// Encode the size of the fact in the header of the fact.
		binary.PutUvarint(header, uint64(size))

		// Append the header, then the fact
		buf = append(buf, header...)
		buf = append(buf, fb...)
	}

	return buf, nil
}

func marshalSegment(s *Segment) ([]byte, error) {
	m := pb.Segment{
		ID:     proto.Uint64(s.ID),
		Blocks: proto.Int32(int32(s.Blocks)),
		Count:  proto.Int32(int32(s.Count)),
		Bytes:  proto.Int32(int32(s.Bytes)),
		Next:   proto.Uint64(s.Next),
		Base:   proto.Uint64(s.Base),
	}

	return proto.Marshal(&m)
}

func unmarshalSegment(b []byte, s *Segment) error {
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

func marshalLog(l *Log) ([]byte, error) {
	m := pb.Log{
		Head: proto.Uint64(l.Head),
	}

	return proto.Marshal(&m)
}

func unmarshalLog(b []byte, l *Log) error {
	m := pb.Log{}

	if err := proto.Unmarshal(b, &m); err != nil {
		return err
	}

	l.Head = m.GetHead()

	return nil
}

func marshalTx(tx *Transaction) ([]byte, error) {
	m := pb.Transaction{
		ID:        proto.Uint64(tx.ID),
		StartTime: proto.Int64(chrono.TimeMicro(tx.StartTime)),
		EndTime:   proto.Int64(chrono.TimeMicro(tx.EndTime)),
	}

	return proto.Marshal(&m)
}
