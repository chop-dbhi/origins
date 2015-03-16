package storage

import "github.com/golang/protobuf/proto"

// ProtoMessage defines methods for converting to and from a type's
// protocol buffer message type.
type ProtoMessage interface {
	// Proto returns an initialized value to decode byte into.
	Proto() proto.Message

	// ToProto returns a protobuf message.
	ToProto() (proto.Message, error)

	// FromProto takes a proto message and populates the value with the fields.
	FromProto(proto.Message) error
}

func MarshalProto(v ProtoMessage) ([]byte, error) {
	m, err := v.ToProto()

	if err != nil {
		return nil, err
	}

	return proto.Marshal(m)
}

func UnmarshalProto(b []byte, v ProtoMessage) error {
	m := v.Proto()

	if err := proto.Unmarshal(b, m); err != nil {
		return err
	}

	return v.FromProto(m)
}
