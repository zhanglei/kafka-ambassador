package pb

import (
	"encoding/binary"
	"hash/crc32"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

func CrcSum(data []byte) uint32 {
	crc := crc32.New(crcTable)
	crc.Write(data)
	return crc.Sum32()
}

func Uint32ToBytes(crc uint32) []byte {
	b := make([]byte, binary.Size(crc))
	binary.LittleEndian.PutUint32(b, crc)
	return b
}

// SetPayload sets bytes payload, and calculates it's CRC
func (m *Record) SetPayload(bytes []byte) {
	m.Payload = bytes
	m.Crc = CrcSum(bytes)
}

// FromBytes parses record from []bytes
func (m *Record) FromBytes(data []byte) error {
	return proto.Unmarshal(data, m)
}

// ToBytes parses record from []bytes
func (m *Record) ToBytes() ([]byte, error) {
	return proto.Marshal(m)
}

// Now sets record timestmap to current timestamp
func (m *Record) Now() {
	m.Timestamp = ptypes.TimestampNow()
}

func (m *Record) GetTime() (time.Time, error) {
	return ptypes.Timestamp(m.Timestamp)
}
