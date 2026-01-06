package transport

import (
	"errors"
	"io"
	"log/slog"
	"net"
)

const (
	MaxPayloadSize = 32 * 1024
)

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
