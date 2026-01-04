package main

import (
	"flag"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/zbsss/tunnels/pkg/protocol"
)

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
		case protocol.TypeStreamOpen:
			go c.handleStream(frame.StreamID)
		case protocol.TypeStreamData:
			// forward payload to destination server
			if conn, ok := c.streams.Load(frame.StreamID); ok {
				conn.(net.Conn).Write(frame.Payload)
			}
		case protocol.TypePing:
			c.Write(protocol.Frame{
				Type:     protocol.TypePong,
				StreamID: frame.StreamID,
			})
		case protocol.TypeStreamClose:
			if conn, ok := c.streams.LoadAndDelete(frame.StreamID); ok {
				conn.(net.Conn).Close()
			}
		}
	}
}

func (c *Client) handleStream(streamID uint32) {
	log := slog.With("streamID", streamID)
	defer func() {
		log.Info("closing stream")
		err := c.Write(protocol.Frame{
			Type:     protocol.TypeStreamClose,
			StreamID: streamID,
		})
		if err != nil {
			log.Error("write stream close", "err", err)
		}
	}()

	dest, err := net.Dial("tcp", c.destAddr)
	if err != nil {
		log.Error("dial dest", "err", err)
		return
	}
	defer dest.Close()

	c.streams.Store(streamID, dest)
	defer c.streams.Delete(streamID)

	log.Info("opened new stream")

	// read from the connection to dest and forward it to the Server
	buf := make([]byte, 32*1024)
	for {
		n, err := dest.Read(buf)
		if n > 0 {
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
			if err != io.EOF {
				log.Error("read from dest", "err", err)
			}

			return
		}
	}
}

func main() {
	serverAddr := flag.String("server", "localhost:8443", "tunnel server address")
	destAddr := flag.String("destination", "localhost:80", "destination service address")
	flag.Parse()

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
