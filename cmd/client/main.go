package main

import (
	"flag"
	"log/slog"
	"net"
	"os"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/zbsss/tunnels/pkg/protocol"
)

type Client struct {
	serverAddr string
	destAddr   string

	tunnel *protocol.Tunnel

	streams sync.Map
}

func (c *Client) run() error {
	conn, err := net.Dial("tcp", c.serverAddr)
	if err != nil {
		return errors.Wrap(err, "dial tunnel")
	}
	defer conn.Close()
	c.tunnel = protocol.NewTunnel(conn)

	slog.Info("forwarding ready", "from", c.serverAddr, "to", c.destAddr)

	for {
		frame, err := protocol.ReadFrame(conn)
		if err != nil {
			return errors.Wrap(err, "read from tunnel")
		}

		switch frame.Type {
		case protocol.TypeStreamData:
			// forward payload to destination server
			stream, ok := c.streams.Load(frame.StreamID)
			if !ok {
				stream, err = c.openStream(frame.StreamID)
				if err != nil {
					return errors.Wrapf(err, "open stream %d", frame.StreamID)
				}
			}
			slog.Debug("forwarding packet from tunnel to stream", "streamID", frame.StreamID, "len", len(frame.Payload))
			stream.(*protocol.Stream).Write(frame.Payload)
		case protocol.TypePing:
			slog.Debug("received ping")
			c.tunnel.Write(protocol.Frame{
				Type:     protocol.TypePong,
				StreamID: frame.StreamID,
			})
		case protocol.TypeStreamClose:
			if st, ok := c.streams.LoadAndDelete(frame.StreamID); ok {
				slog.Debug("received StreamClose, closing stream...", "streamID", frame.StreamID)
				stream := st.(*protocol.Stream)
				if err := stream.Close(); err != nil {
					slog.Error("close stream connection", "streamID", frame.StreamID, "err", err)
				}
			}
		}
	}
}

func (c *Client) openStream(streamID uint32) (*protocol.Stream, error) {
	log := slog.With("streamID", streamID)

	destConn, err := net.Dial("tcp", c.destAddr)
	if err != nil {
		return nil, errors.Wrap(err, "dial dest")
	}

	stream := protocol.NewStream(streamID, destConn)
	c.streams.Store(streamID, stream)
	log.Info("opened new stream")

	go func() {
		protocol.ForwardToTunnel(stream, c.tunnel)
		c.streams.Delete(streamID)
	}()

	return stream, nil
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
