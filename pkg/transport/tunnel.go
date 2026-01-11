// Package transport implements a multiplexed tunneling protocol.
//
// Architecture:
//   - Tunnel: A persistent TCP connection between Proxy and Agent that carries
//     multiple multiplexed channels.
//   - Channel: A logical bidirectional stream within a tunnel, identified by a
//     unique ChannelID. Each channel proxies one end-to-end connection.
//   - Frame: The protocol data unit used to send data and control messages.
//
// Flow:
//
//	Public Client → Proxy Channel → Tunnel → Agent Channel → Backend Service
//
// This design is similar to SSH port forwarding, where a single control connection
// (tunnel) carries multiple forwarded connections (channels).
package transport

import (
	"context"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

type options struct {
	log *slog.Logger
}

func (o *options) withDefaults() *options {
	if o.log == nil {
		o.log = slog.Default()
	}
	return o
}

type Option func(p *options)

func WithLogger(log *slog.Logger) Option {
	return func(opts *options) {
		opts.log = log
	}
}

const (
	TunnelIdleTimeout   = 30 * time.Second
	ChannelMaxQueueSize = 256
)

type Tunnel struct {
	log *slog.Logger

	mu          sync.Mutex
	conn        net.Conn
	channels    sync.Map // map from ChannelID to *Channel
	channelDone sync.Map // map from ChannelID to chan struct{}

	lastActivity atomic.Value
}

func NewTunnel(conn net.Conn, opts ...Option) *Tunnel {
	o := new(options).withDefaults()
	for _, update := range opts {
		update(o)
	}

	return &Tunnel{
		conn: conn,
		log:  o.log.With("component", "tunnel", "tunnelLocal", conn.LocalAddr(), "tunnelRemote", conn.RemoteAddr()),
	}
}

func (t *Tunnel) Serve(ctx context.Context, handler func(frame Frame)) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	go t.watchIdleTimeout(ctx)

	channelQueues := make(map[uint32]chan Frame)

	t.Log().Debug("waiting for frames...")
	for {
		frame, err := t.read()
		if err != nil {
			if !isExpectedCloseErr(err) {
				t.Log().Error("failed to read frame from tunnel", "err", err)
			}
			return
		}

		queue, exists := channelQueues[frame.ChannelID]
		if !exists {
			queue = make(chan Frame, ChannelMaxQueueSize)
			channelQueues[frame.ChannelID] = queue

			// These goroutines are stopped when tunnel.UnregisterChannel is called
			done := make(chan struct{})
			t.channelDone.Store(frame.ChannelID, done)

			go func(done chan struct{}, q chan Frame) {
				select {
				case <-done:
					return
				case frame, ok := <-queue:
					if !ok {
						return
					}
					handler(frame)
				}
			}(done, queue)
		}

		queue <- frame
	}
}

func (t *Tunnel) watchIdleTimeout(ctx context.Context) {
	ticker := time.NewTicker(TunnelIdleTimeout / 2)
	defer ticker.Stop()

	t.refreshActivity()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			last := t.lastActivity.Load().(time.Time)
			if time.Since(last) > TunnelIdleTimeout {
				t.Log().Error("tunnel idle timeout, closing")
				t.Close()
				return
			}
		}
	}
}

func (t *Tunnel) Log() *slog.Logger {
	return t.log
}

func (t *Tunnel) read() (Frame, error) {
	defer t.refreshActivity()
	return readFrame(t.conn)
}

func (t *Tunnel) refreshActivity() {
	t.lastActivity.Swap(time.Now())
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
	t.channels.Clear()

	return t.conn.Close()
}

// RegisterChannel adds a channel to the tunnel's channel map
func (t *Tunnel) RegisterChannel(channel *Channel) {
	t.channels.Store(channel.ID, channel)
}

// UnregisterChannel atomically removes and returns a channel from the tunnel
// Returns the channel and true if it was present, nil and false otherwise
func (t *Tunnel) UnregisterChannel(id uint32) (*Channel, bool) {
	val, ok := t.channels.LoadAndDelete(id)
	if !ok {
		return nil, false
	}

	// signal to stop frame processor goroutine
	if done, ok := t.channelDone.LoadAndDelete(id); ok {
		close(done.(chan struct{}))
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
