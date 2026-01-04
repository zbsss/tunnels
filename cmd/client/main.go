package main

import (
	"context"
	"flag"
	"io"
	"log/slog"
	"net"
	"os"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/zbsss/tunnels/pkg/protocol"
)

type Stream struct {
	conn   net.Conn
	cancel context.CancelFunc
}

type Client struct {
	serverAddr string
	destAddr   string

	mu   sync.Mutex
	conn net.Conn

	streams sync.Map
}

func (c *Client) Write(frame protocol.Frame) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return protocol.WriteFrame(c.conn, frame)
}

func (c *Client) run() error {
	tunnel, err := net.Dial("tcp", c.serverAddr)
	if err != nil {
		return errors.Wrap(err, "dial tunnel")
	}

	c.conn = tunnel
	defer tunnel.Close()

	slog.Info("forwarding ready", "from", c.serverAddr, "to", c.destAddr)

	for {
		frame, err := protocol.ReadFrame(tunnel)
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
			slog.Debug("forwarding data", "streamID", frame.StreamID, "len", len(frame.Payload))
			stream.(*Stream).conn.Write(frame.Payload)
		case protocol.TypePing:
			slog.Debug("received ping")
			c.Write(protocol.Frame{
				Type:     protocol.TypePong,
				StreamID: frame.StreamID,
			})
		case protocol.TypeStreamClose:
			if st, ok := c.streams.LoadAndDelete(frame.StreamID); ok {
				slog.Debug("received StreamClose, closing stream...", "streamID", frame.StreamID)
				stream := st.(*Stream)
				stream.cancel()
				if err := stream.conn.Close(); err != nil {
					slog.Error("close dest connection", "streamID", frame.StreamID, "err", err)
				}
			}
		}
	}
}

func (c *Client) openStream(streamID uint32) (*Stream, error) {
	log := slog.With("streamID", streamID)

	dest, err := net.Dial("tcp", c.destAddr)
	if err != nil {
		return nil, errors.Wrap(err, "dial dest")
	}

	ctx, cancel := context.WithCancel(context.Background())
	stream := &Stream{
		conn:   dest,
		cancel: cancel,
	}
	c.streams.Store(streamID, stream)
	log.Info("opened new stream")

	go func() {
		// read from the connection to dest and forward it to the Server
		buf := make([]byte, 32*1024)
		for {
			n, err := dest.Read(buf)
			if n > 0 {
				log.Debug("read from dest", "len", n)
				wErr := c.Write(protocol.Frame{
					Type:     protocol.TypeStreamData,
					StreamID: streamID,
					Payload:  buf[:n],
				})
				if wErr != nil {
					log.Error("write to server", "err", wErr)
				}
			}
			if err != nil {
				if ctx.Err() != nil {
					log.Debug("stream closed by tunnel server")
					return
				}

				if err != io.EOF {
					log.Error("read from dest", "err", err)
				}

				log.Info("closing stream")
				c.streams.Delete(streamID)
				err := dest.Close()
				if err != nil {
					log.Error("close dest", "err", err)
				}

				err = c.Write(protocol.Frame{
					Type:     protocol.TypeStreamClose,
					StreamID: streamID,
				})
				if err != nil {
					log.Error("write stream close", "err", err)
				}

				return
			}
		}
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

	// TODO: handle graceful shutdown, close all streams
}
