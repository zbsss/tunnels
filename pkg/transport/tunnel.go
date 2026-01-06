// Package transport implements a multiplexed tunneling protocol.
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
package transport

import (
	"net"
	"sync"
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
