package main

import (
	"flag"
	"log/slog"
	"net"
	"os"
	"time"

	"github.com/pkg/errors"
	"github.com/zbsss/tunnels/pkg/protocol"
)

type Client struct {
	serverAddr string
	destAddr   string

	tunnel *protocol.Tunnel
}

func (c *Client) run() error {
	conn, err := net.Dial("tcp", c.serverAddr)
	if err != nil {
		return errors.Wrap(err, "dial tunnel")
	}
	c.tunnel = protocol.NewTunnel(conn)

	log := slog.With("tunnelRemote", conn.RemoteAddr().String())
	log.Info("tunnel connected")

	defer func() {
		c.tunnel.Close()
		log.Info("tunnel disconnected")
	}()

	for {
		frame, err := protocol.ReadFrame(conn)
		if err != nil {
			return errors.Wrap(err, "read from tunnel")
		}

		switch frame.Type {
		case protocol.TypeChannelData:
			// forward payload to destination server
			channel, ok := c.tunnel.Channel(frame.ChannelID)
			if !ok {
				channel, err = c.openBackendChannel(frame.ChannelID)
				if err != nil {
					return errors.Wrapf(err, "open channel %d", frame.ChannelID)
				}
			}
			log.Debug("forwarding packet from tunnel to channel", "channelID", frame.ChannelID, "len", len(frame.Payload))
			if _, err := channel.Conn.Write(frame.Payload); err != nil {
				log.Error("write to channel", "channelID", frame.ChannelID, "err", err)
			}
		case protocol.TypePing:
			log.Debug("received ping")
			if err := c.tunnel.SendPong(); err != nil {
				log.Error("write pong", "err", err)
			}
		case protocol.TypeChannelClose:
			if channel, ok := c.tunnel.UnregisterChannel(frame.ChannelID); ok {
				log.Info("received ChannelClose, closing channel", "channelID", frame.ChannelID)
				if err := channel.Conn.Close(); err != nil {
					log.Error("close channel", "channelID", frame.ChannelID, "err", err)
				}
			}
		}
	}
}

func (c *Client) openBackendChannel(channelID uint32) (*protocol.Channel, error) {
	backendConn, err := net.Dial("tcp", c.destAddr)
	if err != nil {
		return nil, errors.Wrap(err, "dial dest")
	}

	channel := protocol.NewChannel(channelID, backendConn)
	c.tunnel.RegisterChannel(channelID, channel)
	slog.Info("opened new channel", "channelID", channelID, "to", backendConn.RemoteAddr().String())

	go channel.RelayThrough(c.tunnel)

	return channel, nil
}

func main() {
	debug := flag.Bool("debug", true, "enable debug logging")
	serverAddr := flag.String("server", ":8443", "tunnel server address")
	destAddr := flag.String("destination", ":42064", "destination service address")
	flag.Parse()

	level := slog.LevelInfo
	if *debug {
		level = slog.LevelDebug
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: level,
	})))

	client := Client{
		serverAddr: *serverAddr,
		destAddr:   *destAddr,
	}
	for {
		err := client.run()
		if err != nil {
			slog.Error("disconnected, reconnecting in 5s...", "err", err)
			time.Sleep(5 * time.Second)
		}
	}
}
