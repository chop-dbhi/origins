package dal

import (
	"fmt"

	"github.com/chop-dbhi/origins/storage"
	"github.com/satori/go.uuid"
)

const (
	// Transactions are keyed by their unique ID. They are stored within
	// the reserved origins domain.
	txKey = "tx.%d"

	// Logs are keyed by their name. They are stored in a domain.
	logKey = "log.%s"

	// Segments are keyed by their UUID. They are stored in a domain.
	segmentKey = "segment.%s"

	// Blocks are keyed by the segment UUID they belong to and block index.
	// They are stored in a domain.
	blockKey = "block.%s.%d"
)

func GetLog(e storage.Tx, domain, name string) (*Log, error) {
	var (
		bytes []byte
		err   error
		key   string
	)

	key = fmt.Sprintf(logKey, name)

	if bytes, err = e.Get(domain, key); err != nil {
		return nil, err
	}

	if bytes == nil {
		return nil, nil
	}

	log := Log{
		Domain: domain,
		Name:   name,
	}

	if err = unmarshalLog(bytes, &log); err != nil {
		return nil, err
	}

	return &log, nil
}

func SetLog(e storage.Tx, domain string, log *Log) (int, error) {
	var (
		bytes []byte
		err   error
		key   string
	)

	if bytes, err = marshalLog(log); err != nil {
		return 0, err
	}

	key = fmt.Sprintf(logKey, log.Name)

	return len(bytes), e.Set(domain, key, bytes)
}

func DeleteLog(e storage.Tx, domain string, name string) error {
	key := fmt.Sprintf(logKey, name)

	return e.Delete(domain, key)
}

// GetSegment returns a segment from storage.
func GetSegment(e storage.Tx, domain string, id *uuid.UUID) (*Segment, error) {
	var (
		bytes []byte
		err   error
		key   string
	)

	key = fmt.Sprintf(segmentKey, id)

	if bytes, err = e.Get(domain, key); err != nil {
		return nil, err
	}

	if bytes == nil {
		return nil, nil
	}

	seg := Segment{
		UUID:   id,
		Domain: domain,
	}

	if err = unmarshalSegment(bytes, &seg); err != nil {
		return nil, err
	}

	return &seg, nil
}

func SetSegment(e storage.Tx, domain string, segment *Segment) (int, error) {
	var (
		bytes []byte
		err   error
		key   string
	)

	if bytes, err = marshalSegment(segment); err != nil {
		return 0, err
	}

	key = fmt.Sprintf(segmentKey, segment.UUID)

	return len(bytes), e.Set(domain, key, bytes)
}

func DeleteSegment(e storage.Tx, domain string, id *uuid.UUID) error {
	key := fmt.Sprintf(segmentKey, id)

	return e.Delete(domain, key)
}

// GetBlock returns a block from storage. The lookup requires the domain, ID of the segment
// the block is contained in, the index of the block in the segment, and the transaction
// that processed the segment.
func GetBlock(e storage.Tx, domain string, id *uuid.UUID, idx int) ([]byte, error) {
	var key string

	key = fmt.Sprintf(blockKey, id, idx)

	return e.Get(domain, key)
}

func SetBlock(e storage.Tx, domain string, id *uuid.UUID, idx int, bytes []byte) (int, error) {
	var key string

	key = fmt.Sprintf(blockKey, id, idx)

	return len(bytes), e.Set(domain, key, bytes)
}

func DeleteBlock(e storage.Tx, domain string, id *uuid.UUID, idx int) error {
	key := fmt.Sprintf(blockKey, id, idx)

	return e.Delete(domain, key)
}

func GetTransaction(e storage.Tx, id uint64) (*Transaction, error) {
	var (
		bytes []byte
		err   error
		key   string
	)

	key = fmt.Sprintf(txKey, id)

	if bytes, err = e.Get("origins", key); err != nil {
		return nil, err
	}

	if bytes == nil {
		return nil, nil
	}

	tx := Transaction{}

	if err = unmarshalTx(bytes, &tx); err != nil {
		return nil, err
	}

	return &tx, nil
}

func SetTransaction(e storage.Tx, tx *Transaction) (int, error) {
	var (
		bytes []byte
		err   error
		key   string
	)

	if bytes, err = marshalTx(tx); err != nil {
		return 0, err
	}

	key = fmt.Sprintf(txKey, tx.ID)

	return len(bytes), e.Set("origins", key, bytes)
}

func DeleteTransaction(e storage.Tx, id uint64) error {
	key := fmt.Sprintf(logKey, id)

	return e.Delete("origins", key)
}
