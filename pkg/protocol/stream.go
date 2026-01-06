// Package protocol implements a multiplexed tunneling protocol.
//
// Architecture:
//   - Tunnel: A persistent TCP connection between client and server that carries
//     multiple multiplexed channels.
//   - Channel: A logical bidirectional stream within a tunnel, identified by a
//     unique ChannelID. Each channel proxies one end-to-end connection.
//   - Frame: The protocol data unit used to send data and control messages.
//
// Flow:
//
//	Public Client → Server Channel → Tunnel → Client Channel → Backend Service
//
// This design is similar to SSH port forwarding, where a single control connection
// (tunnel) carries multiple forwarded connections (channels).
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
	mu       sync.Mutex
	conn     net.Conn
	channels sync.Map // map from ChannelID to *Channel
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

// Close closes the tunnel connection and all associated channels
func (t *Tunnel) Close() error {
	t.channels.Range(func(key, value any) bool {
		if channel, ok := value.(*Channel); ok {
			channel.Conn.Close()
		}
		return true
	})

	return t.conn.Close()
}

// RegisterChannel adds a channel to the tunnel's channel map
func (t *Tunnel) RegisterChannel(id uint32, channel *Channel) {
	t.channels.Store(id, channel)
}

// UnregisterChannel atomically removes and returns a channel from the tunnel
// Returns the channel and true if it was present, nil and false otherwise
func (t *Tunnel) UnregisterChannel(id uint32) (*Channel, bool) {
	val, ok := t.channels.LoadAndDelete(id)
	if !ok {
		return nil, false
	}
	return val.(*Channel), true
}

// Channel retrieves a channel by ID without removing it
func (t *Tunnel) Channel(id uint32) (*Channel, bool) {
	val, ok := t.channels.Load(id)
	if !ok {
		return nil, false
	}
	return val.(*Channel), true
}

// SendData sends data for a specific channel
func (t *Tunnel) SendData(channelID uint32, data []byte) error {
	return t.write(Frame{
		Type:      TypeChannelData,
		ChannelID: channelID,
		Payload:   data,
	})
}

// SendClose sends a ChannelClose frame
func (t *Tunnel) SendClose(channelID uint32) error {
	return t.write(Frame{
		Type:      TypeChannelClose,
		ChannelID: channelID,
	})
}

// SendPing sends a Ping frame
func (t *Tunnel) SendPing() error {
	return t.write(Frame{
		Type:      TypePing,
		ChannelID: 0,
	})
}

// SendPong sends a Pong frame
func (t *Tunnel) SendPong() error {
	return t.write(Frame{
		Type:      TypePong,
		ChannelID: 0,
	})
}

// Channel represents a multiplexed bidirectional connection over a Tunnel.
// Each channel has a unique ID and wraps an underlying network connection.
//
// On the server side: wraps a connection from a public client
// On the client side: wraps a connection to the backend service
type Channel struct {
	ID   uint32
	Conn net.Conn
}

func NewChannel(id uint32, conn net.Conn) *Channel {
	return &Channel{
		ID:   id,
		Conn: conn,
	}
}

// RelayThrough reads data from this channel's connection and forwards it
// through the tunnel to the remote peer. This is a unidirectional operation
// that runs until the connection closes or an error occurs.
//
// When the channel ends or fails, it closes the connection and sends a ChannelClose frame if needed.
// Whoever successfully unregisters the channel from the tunnel is
// responsible for sending the ChannelClose notification. This prevents ping-pong close messages.
func (ch *Channel) RelayThrough(tunnel *Tunnel) {
	log := slog.With("channelID", ch.ID)

	defer func() {
		// Atomically try to unregister from tunnel
		// If we successfully remove it, WE are responsible for notifying the remote
		// If it's already gone, the remote closed first (we received ChannelClose)
		if _, wasPresent := tunnel.UnregisterChannel(ch.ID); wasPresent {
			log.Info("closing channel", "endpoint", ch.Conn.RemoteAddr().String())
			if err := ch.Conn.Close(); err != nil {
				log.Error("close channel connection", "err", err)
			}

			log.Debug("sending ChannelClose to remote")
			if err := tunnel.SendClose(ch.ID); err != nil {
				log.Error("send ChannelClose", "err", err)
			}
		} else {
			log.Debug("channel already unregistered by remote close")
		}
	}()

	buf := make([]byte, MaxPayloadSize)
	for {
		n, err := ch.Conn.Read(buf)
		if n > 0 {
			log.Debug("forwarding packet from channel to tunnel", "len", n)
			if wErr := tunnel.SendData(ch.ID, buf[:n]); wErr != nil {
				log.Error("write to tunnel failed, closing channel", "err", wErr)
				return
			}
		}
		if err != nil {
			// Don't log EOF or "connection closed" errors, these are expected
			if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
				log.Error("read from channel", "err", err)
			}
			return
		}
	}
}
