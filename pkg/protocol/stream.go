package protocol

import (
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
)

const (
	MaxPayloadSize = 32 * 1024
)

type Tunnel struct {
	mu      sync.Mutex
	conn    net.Conn
	streams sync.Map // map from StreamID to *Stream
}

func NewTunnel(conn net.Conn) *Tunnel {
	return &Tunnel{
		conn: conn,
	}
}

// write is a thread-safe wrapper around writeFrame
func (t *Tunnel) write(frame Frame) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	return writeFrame(t.conn, frame)
}

// Close closes the tunnel connection and all associated streams
func (t *Tunnel) Close() error {
	t.streams.Range(func(key, value any) bool {
		if stream, ok := value.(*Stream); ok {
			stream.Close()
		}
		return true
	})

	return t.conn.Close()
}

// RegisterStream adds a stream to the tunnel's stream map
func (t *Tunnel) RegisterStream(id uint32, stream *Stream) {
	t.streams.Store(id, stream)
}

// UnregisterStream atomically removes and returns a stream from the tunnel
// Returns the stream and true if it was present, nil and false otherwise
func (t *Tunnel) UnregisterStream(id uint32) (*Stream, bool) {
	val, ok := t.streams.LoadAndDelete(id)
	if !ok {
		return nil, false
	}
	return val.(*Stream), true
}

// Stream retrieves a stream by ID without removing it
func (t *Tunnel) Stream(id uint32) (*Stream, bool) {
	val, ok := t.streams.Load(id)
	if !ok {
		return nil, false
	}
	return val.(*Stream), true
}

// SendData sends data for a specific stream
func (t *Tunnel) SendData(streamID uint32, data []byte) error {
	return t.write(Frame{
		Type:     TypeStreamData,
		StreamID: streamID,
		Payload:  data,
	})
}

// SendClose sends a StreamClose frame
func (t *Tunnel) SendClose(streamID uint32) error {
	return t.write(Frame{
		Type:     TypeStreamClose,
		StreamID: streamID,
	})
}

// SendPing sends a Ping frame
func (t *Tunnel) SendPing() error {
	return t.write(Frame{
		Type:     TypePing,
		StreamID: 0,
	})
}

// SendPong sends a Pong frame
func (t *Tunnel) SendPong() error {
	return t.write(Frame{
		Type:     TypePong,
		StreamID: 0,
	})
}

type Stream struct {
	ID uint32
	net.Conn
}

func NewStream(id uint32, conn net.Conn) *Stream {
	return &Stream{
		ID:   id,
		Conn: conn,
	}
}

// ForwardToTunnel reads data from a stream in 32KB chunks and forwards them as frames to the tunnel.
// When the stream ends or fails, it closes the stream and sends a StreamClose frame if needed.
// Whoever successfully unregisters the stream from the tunnel is
// responsible for sending the StreamClose notification. This prevents ping-pong close messages.
func ForwardToTunnel(stream *Stream, tunnel *Tunnel) {
	log := slog.With("streamID", stream.ID)

	defer func() {
		// Atomically try to unregister from tunnel
		// If we successfully remove it, WE are responsible for notifying the remote
		// If it's already gone, the remote closed first (we received StreamClose)
		if _, wasPresent := tunnel.UnregisterStream(stream.ID); wasPresent {
			log.Info("closing stream", "endpoint", stream.Conn.RemoteAddr().String())
			if err := stream.Close(); err != nil {
				log.Error("close stream connection", "err", err)
			}

			log.Debug("sending StreamClose to remote")
			if err := tunnel.SendClose(stream.ID); err != nil {
				log.Error("send StreamClose", "err", err)
			}
		} else {
			log.Debug("stream already unregistered by remote close")
		}
	}()

	buf := make([]byte, MaxPayloadSize)
	for {
		n, err := stream.Read(buf)
		if n > 0 {
			log.Debug("forwarding packet from stream to tunnel", "len", n)
			if wErr := tunnel.SendData(stream.ID, buf[:n]); wErr != nil {
				log.Error("write to tunnel failed, closing stream", "err", wErr)
				return
			}
		}
		if err != nil {
			// Don't log EOF or "connection closed" errors, these are expected
			if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
				log.Error("read from stream", "err", err)
			}
			return
		}
	}
}
