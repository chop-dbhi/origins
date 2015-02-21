/*
The storage package defines an interface for implementing stores. A store
is used to read and writing sets of data including raw statements, parsed facts,
and materialized EAV structures.

This package contains packages that provide an implementation to the Store
interface.
*/
package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/chop-dbhi/origins/fact"
	"github.com/golang/protobuf/proto"
	"github.com/sirupsen/logrus"
)

const (
	// Version of the store. This is checked against the store header to
	// determine if a migration needs to occur.
	Version = 0

	// Takes the store name.
	storeKeyFmt = "origins.%s"

	// Default codec type.
	defaultCodec = "protobuf"

	// Size of the message framing prefix size for a fact in bytes.
	framePrefixFactSize = 2

	// Maximum fact size in bytes. The fixed size is prefixed to the fact to
	// for length-prefix framing.
	maxFactSize = 1 << (framePrefixFactSize * 8)
)

var headerCodec = codecs[defaultCodec]

type Store struct {
	// Name is the given name of the store. This is used to as the top-level
	// prefix of storage keys.
	Name string

	// Codec is the name of the codec used for writing facts to storage. This
	// is used to determin compatibility of the store with the program.
	Codec string

	// Version is the version of the store from the last usage. This
	// is used to determin compatibility of the store with the program.
	Version int

	// Fixed signature of the store
	// TODO(bjr): is it good practice to add this?
	// see https://en.wikipedia.org/wiki/List_of_file_signatures
	// Sig []byte

	storeKey string
	engine   Engine
	codec    Codec
	parts    map[string]*partition
}

func (s *Store) Proto() proto.Message {
	return &ProtoStore{}
}

func (s *Store) ToProto() (proto.Message, error) {
	return &ProtoStore{
		Name:    proto.String(s.Name),
		Codec:   proto.String(s.Codec),
		Version: proto.Int32(int32(s.Version)),
	}, nil
}

func (s *Store) FromProto(v proto.Message) error {
	m := v.(*ProtoStore)
	s.Name = m.GetName()
	s.Codec = m.GetCodec()
	s.Version = int(m.GetVersion())
	return nil
}

func (s *Store) init() error {
	// Key for the store header.
	s.storeKey = fmt.Sprintf(storeKeyFmt, s.Name)

	// Load the header if it exists.
	b, err := s.engine.Get(s.storeKey)

	if err != nil {
		return err
	}

	s.Codec = defaultCodec

	// Record of store does not exist.
	if b == nil {
		s.Version = Version

		if err = s.writeHeader(); err != nil {
			return err
		}

		logrus.Infof("initialized new store named '%s'", s.Name)
	} else {
		if err := headerCodec.Unmarshal(b, s); err != nil {
			return err
		}

		if Version != s.Version {
			logrus.Fatalf("Store version is '%s', but using '%s' version of the client.", s.Version, Version)
		}

		if defaultCodec != s.Codec {
			logrus.Fatalf("Store uses codec '%s', but the default is '%s'.", s.Codec, defaultCodec)
		}

		logrus.Infof("initialized existing store named '%s'", s.Name)
	}

	// Setup internal fields.
	s.codec = codecs[s.Codec]
	s.parts = make(map[string]*partition)

	return nil
}

// writeHeader writes the current state of the store to storage.
func (s *Store) writeHeader() error {
	b, err := headerCodec.Marshal(s)

	if err != nil {
		return err
	}

	return s.engine.Set(s.storeKey, b)
}

func (s *Store) initPartition(k string) (*partition, error) {
	// TODO(bjr) add lock, so the same partition cannot be created

	var (
		p   *partition
		ok  bool
		err error
	)

	// Check if in local cache.
	p, ok = s.parts[k]

	if ok {
		logrus.Debugf("found partition %v in local cache", k)
		return p, nil
	}

	// TODO(bjr) check in other cache (e.g. memcache)

	// Partition key
	key := fmt.Sprintf(partitionKeyFmt, s.storeKey, k)

	p, err = initPartition(key, s.engine)

	if err != nil {
		return nil, err
	}

	// Store in the local cache.
	s.parts[k] = p

	return p, nil
}

// Read populates the passed slice of facts in the specified domain.
func (s *Store) Read(domain string, facts fact.Facts) (int, error) {
	// Initialize
	if facts == nil {
		logrus.Panicf("facts not initialized")
	}

	var (
		p   *partition
		ok  bool
		err error
	)

	p, ok = s.parts[domain]

	if !ok {
		if p, err = s.initPartition(domain); err != nil {
			return 0, err
		}
	}

	var (
		// Number of facts decoded.
		i int
		// Number of bytes read
		n int
		// Binary error code
		berr int
		// Size of the fact in bytes.
		size uint64
		// 2 byte length prefix per fact.
		prefix = make([]byte, 2, 2)
		// Fact buffer
		buf []byte
	)

	// Initialize a partition reader.
	r := p.Reader()

	for ; i < len(facts); i++ {
		n, err = r.Read(prefix)

		if err == io.EOF {
			return i, nil
		}

		if err != nil {
			return 0, err
		}

		if n != framePrefixFactSize {
			logrus.Panicf("frame prefix should be %d, got %d", framePrefixFactSize, n)
		}

		size, berr = binary.Uvarint(prefix)

		if berr < 0 {
			logrus.Panicf("cannot decode frame prefix: uvarint code %d", berr)
		}

		buf = make([]byte, size)

		n, err = r.Read(buf)

		if err == io.EOF {
			logrus.Panicf("got unexpected EOF after %d bytes, expected %d", n, size)
		}

		if err != nil {
			return i, err
		}

		if size != uint64(n) {
			logrus.Panicf("frame size should be %d, got %d", size, n)
		}

		f := fact.Fact{}

		if err = s.codec.Unmarshal(buf, &f); err != nil {
			return i, err
		}

		facts[i] = &f
	}

	return i, nil
}

// WriteSegment writes a segment to storage. The number of bytes written are returned
// or an error.
func (s *Store) WriteSegment(domain string, key uint64, facts fact.Facts) (int, error) {
	if len(facts) == 0 {
		logrus.Panicf("no facts to write")
	}

	var (
		err  error
		part *partition
		ok   bool
	)

	part, ok = s.parts[domain]

	if !ok {
		logrus.Debugf("initializing partition for %s", domain)

		if part, err = s.initPartition(domain); err != nil {
			logrus.Error(err)
			return 0, err
		}
	}

	var (
		// Size of the fact in bytes.
		size int
		// Total size of the segment.
		total int
		// 2 byte length prefix per fact.
		prefix = make([]byte, 2, 2)
		// The full segment of bytes.
		segment = make([]byte, 0)
		// Fact buffer
		buf []byte
	)

	for _, f := range facts {
		buf, err = s.codec.Marshal(f)

		if err != nil {
			return 0, err
		}

		size = len(buf)

		if size > maxFactSize {
			err = errors.New(fmt.Sprintf("fact size %d exceeds maximum allowed %d", size, maxFactSize))
			logrus.Error(err)
			return 0, err
		}

		// Encode the prefix
		binary.PutUvarint(prefix, uint64(size))

		// Append the prefix, then the buffer
		segment = append(segment, prefix...)
		segment = append(segment, buf...)

		total += size
	}

	err = part.Write(key, segment)

	if err != nil {
		return 0, err
	}

	return total, nil
}
