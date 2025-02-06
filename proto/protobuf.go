package proto

import "google.golang.org/protobuf/proto"

func Marshal(pb proto.Message) ([]byte, error) {
	return proto.Marshal(pb)
}

func Unmarshal(data []byte, pb proto.Message) error {
	return proto.Unmarshal(data, pb)
}
