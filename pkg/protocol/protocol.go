package protocol

import (
	"encoding/binary"
	"io"

	"github.com/pkg/errors"
)

const (
	TypeStreamData  = 'd'
	TypeStreamClose = 'c'
	TypePing        = 'i'
	TypePong        = 'o'
)

const HeaderSize = 9

// Type     │ ID       │ Length   │ Payload
// 1 byte   │ 4 bytes  │ 4 bytes  │ (Length bytes)
type Frame struct {
	Type     byte
	StreamID uint32
	Payload  []byte
}

// TODO: should we limit max payload size?
func writeFrame(w io.Writer, f Frame) error {
	header := make([]byte, HeaderSize)
	header[0] = f.Type
	binary.BigEndian.PutUint32(header[1:5], f.StreamID)
	binary.BigEndian.PutUint32(header[5:9], uint32(len(f.Payload)))

	if _, err := w.Write(header); err != nil {
		return errors.Wrap(err, "write header")
	}

	if len(f.Payload) > 0 {
		if _, err := w.Write(f.Payload); err != nil {
			return errors.Wrap(err, "write payload")
		}
	}

	return nil
}

// TODO: make this private, and use Tunnel.Read
// TODO: should we limit max payload size?
func ReadFrame(r io.Reader) (Frame, error) {
	header := make([]byte, HeaderSize)
	if _, err := io.ReadFull(r, header); err != nil {
		return Frame{}, errors.Wrap(err, "read header")
	}

	f := Frame{
		Type:     header[0],
		StreamID: binary.BigEndian.Uint32(header[1:5]),
	}

	length := binary.BigEndian.Uint32(header[5:9])
	if length > 0 {
		f.Payload = make([]byte, length)
		if _, err := io.ReadFull(r, f.Payload); err != nil {
			return Frame{}, errors.Wrap(err, "read payload")
		}
	}

	return f, nil
}
