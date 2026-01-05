package protocol

import (
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
)

const (
	MaxPayloadSize = 32 * 1024
)

type Tunnel struct {
	mu   sync.Mutex
	conn net.Conn
}

// TODO: should we add read with mutex too?
// currently we always read from a single goroutine
// probably should be separate mutex for read and write?

func (t *Tunnel) Write(frame Frame) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	return WriteFrame(t.conn, frame)
}

func NewTunnel(conn net.Conn) *Tunnel {
	return &Tunnel{
		conn: conn,
	}
}

type Stream struct {
	isClosed atomic.Bool
	ID       uint32
	net.Conn
}

func NewStream(id uint32, conn net.Conn) *Stream {
	return &Stream{
		ID:   id,
		Conn: conn,
	}
}

func (s *Stream) Close() error {
	if s.isClosed.Load() {
		return nil
	}

	err := s.Conn.Close()
	if err != nil {
		return err
	}

	s.isClosed.Store(true)
	return nil
}

func (s *Stream) IsClosed() bool {
	return s.isClosed.Load()
}

// ForwardToTunnel reads data from src in 32KB chunks and forwards them as Frames to the Tunnel.
// Closing the Stream will also send a StreamClose Frame to the Tunnel.
// TODO: idk if using *Stream and *Tunnel is a good idea here. Maybe interfaces would be better?
func ForwardToTunnel(stream *Stream, tunnel *Tunnel) {
	log := slog.With("streamID", stream.ID)

	buf := make([]byte, MaxPayloadSize)
	for {
		n, err := stream.Read(buf)
		if n > 0 {
			log.Debug("forwarding packet from stream to tunnel", "len", n)
			wErr := tunnel.Write(Frame{
				Type:     TypeStreamData,
				StreamID: stream.ID,
				Payload:  buf[:n],
			})
			if wErr != nil {
				log.Error("write to tunnel", "err", wErr)
			}
		}
		if err != nil {
			if !errors.Is(err, io.EOF) {
				log.Error("read from connection", "err", err)
			}

			if stream.IsClosed() {
				log.Debug("stream already closed")
				return
			}

			log.Info("closing stream")

			err := stream.Close()
			if err != nil {
				log.Error("close connection", "err", err)
			}

			err = tunnel.Write(Frame{
				Type:     TypeStreamClose,
				StreamID: stream.ID,
			})
			if err != nil {
				log.Error("write stream close", "err", err)
			}

			return
		}
	}
}
