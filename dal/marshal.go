package dal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/chop-dbhi/origins"
	"github.com/chop-dbhi/origins/chrono"
	"github.com/golang/protobuf/proto"
	"github.com/satori/go.uuid"
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

// encodeUUID encodes a UUID value into bytes..
func encodeUUID(u *uuid.UUID) []byte {
	if u == nil {
		return nil
	}

	return u.Bytes()
}

// decodeUUID decodes bytes into a UUID value.
func decodeUUID(b []byte) *uuid.UUID {
	if b == nil {
		return nil
	}

	u, err := uuid.FromBytes(b)

	if err != nil {
		panic(err)
	}

	return &u
}

func marshalLog(l *Log) ([]byte, error) {
	m := ProtoLog{
		Head: encodeUUID(l.Head),
	}

	return proto.Marshal(&m)
}

func unmarshalLog(b []byte, l *Log) error {
	m := ProtoLog{}

	if err := proto.Unmarshal(b, &m); err != nil {
		return err
	}

	l.Head = decodeUUID(m.GetHead())

	return nil
}

func marshalSegment(s *Segment) ([]byte, error) {
	m := ProtoSegment{
		UUID:        encodeUUID(s.UUID),
		Transaction: proto.Uint64(s.Transaction),
		Time:        proto.Int64(chrono.TimeMicro(s.Time)),
		Blocks:      proto.Int32(int32(s.Blocks)),
		Count:       proto.Int32(int32(s.Count)),
		Bytes:       proto.Int32(int32(s.Bytes)),
		Next:        encodeUUID(s.Next),
		Base:        encodeUUID(s.Base),
	}

	return proto.Marshal(&m)
}

func unmarshalSegment(b []byte, s *Segment) error {
	m := ProtoSegment{}

	if err := proto.Unmarshal(b, &m); err != nil {
		return err
	}

	s.UUID = decodeUUID(m.GetUUID())
	s.Transaction = m.GetTransaction()
	s.Time = chrono.MicroTime(m.GetTime())
	s.Blocks = int(m.GetBlocks())
	s.Count = int(m.GetCount())
	s.Bytes = int(m.GetBytes())
	s.Next = decodeUUID(m.GetNext())
	s.Base = decodeUUID(m.GetBase())

	return nil
}

func marshalFact(f *origins.Fact) ([]byte, error) {
	m := ProtoFact{}

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

// unmarshalFact decodes a fact from it's binary representation. The domain and
// transaction ID are passed in since they are not encoded with the fact itself.
// This is because facts are stored relative to a domain and a transaction.
func unmarshalFact(b []byte, d string, t uint64, f *origins.Fact) error {
	m := ProtoFact{}

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

// Marshal encodes facts into it's binary representation.
func marshalBlock(fs origins.Facts) ([]byte, error) {
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

// unmarshalBlock decodes a block of facts from it's binary representation.
func unmarshalBlock(block []byte, d string, t uint64) (origins.Facts, error) {
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
		n, err = reader.Read(buf[:frameFactHeaderSize])

		// No more bytes.
		if n == 0 {
			break
		}

		if n != frameFactHeaderSize {
			logrus.Panicf("unmarshal: fact expected %d bytes, got %d", frameFactHeaderSize, n)
		}

		// Decode the size of the fact.
		if size, errcode = binary.Uvarint(buf[:frameFactHeaderSize]); errcode <= 0 {
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

func marshalTx(tx *Transaction) ([]byte, error) {
	m := ProtoTransaction{
		ID:        proto.Uint64(tx.ID),
		StartTime: proto.Int64(chrono.TimeMicro(tx.StartTime)),
		EndTime:   proto.Int64(chrono.TimeMicro(tx.EndTime)),
	}

	if tx.Error != nil {
		m.Error = proto.String(fmt.Sprint(tx.Error))
	}

	return proto.Marshal(&m)
}

func unmarshalTx(b []byte, tx *Transaction) error {
	m := ProtoTransaction{}

	if err := proto.Unmarshal(b, &m); err != nil {
		return err
	}

	tx.ID = m.GetID()
	tx.StartTime = chrono.MicroTime(m.GetStartTime())
	tx.EndTime = chrono.MicroTime(m.GetEndTime())

	if m.Error != nil {
		tx.Error = errors.New(m.GetError())
	}

	return nil
}
