package main

import (
	"errors"
	"flag"
	"io"
	"log"
	"log/slog"
	"net"
	"os"
	"sync/atomic"
	"time"

	"github.com/zbsss/tunnels/pkg/protocol"
)

type Server struct {
	publicAddr string
	tunnelAddr string

	prevStreamID atomic.Uint32
	tunnel       atomic.Pointer[protocol.Tunnel]
}

func (s *Server) listenTunnel() {
	lis, err := net.Listen("tcp", s.tunnelAddr)
	if err != nil {
		log.Fatalf("tunnel listen: %+v", err)
	}

	defer lis.Close()

	for {
		conn, err := lis.Accept()
		if err != nil {
			slog.Error("tunnel accept", "err", err)
			continue
		}

		go s.handleTunnel(conn)
	}
}

func (s *Server) handleTunnel(conn net.Conn) {
	tunnel := protocol.NewTunnel(conn)
	s.tunnel.Store(tunnel)
	defer func() {
		s.tunnel.Store(nil)
		tunnel.Close() // Closes connection and all streams
		slog.Info("tunnel disconnected")
	}()

	slog.Info("tunnel connected", "from", conn.RemoteAddr().String())

	// keep alive ping-pong
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-done:
				return
			case <-time.After(30 * time.Second):
				slog.Debug("sending ping")
				if err := tunnel.SendPing(); err != nil {
					slog.Error("write ping failed, stopping keepalive", "err", err)
					return
				}
			}
		}
	}()
	defer close(done)

	for {
		frame, err := protocol.ReadFrame(conn)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				slog.Error("read from tunnel", "err", err)
			}
			return
		}

		switch frame.Type {
		case protocol.TypeStreamData:
			if stream, ok := tunnel.Stream(frame.StreamID); ok {
				slog.Debug("forwarding packet from tunnel to stream", "streamID", frame.StreamID, "len", len(frame.Payload))
				if _, err := stream.Write(frame.Payload); err != nil {
					slog.Error("write to stream", "streamID", frame.StreamID, "err", err)
				}
			} else {
				slog.Warn("received data for unknown stream", "streamID", frame.StreamID)
			}
		case protocol.TypeStreamClose:
			if stream, ok := tunnel.UnregisterStream(frame.StreamID); ok {
				slog.Debug("received StreamClose, closing stream", "streamID", frame.StreamID)
				if err := stream.Close(); err != nil {
					slog.Error("close stream connection", "streamID", frame.StreamID, "err", err)
				}
			}
		case protocol.TypePong:
			slog.Debug("received pong")
		}
	}
}

func (s *Server) listenPublic() {
	lis, err := net.Listen("tcp", s.publicAddr)
	if err != nil {
		log.Fatalf("public listen: %+v", err)
	}

	defer lis.Close()

	for {
		conn, err := lis.Accept()
		if err != nil {
			slog.Error("public accept", "err", err)
			continue
		}

		go s.handlePublic(conn)
	}
}

func (s *Server) handlePublic(conn net.Conn) {
	tunnel := s.tunnel.Load()
	if tunnel == nil {
		slog.Error("no tunnel available")
		conn.Close()
		return
	}

	stream := protocol.NewStream(s.prevStreamID.Add(1), conn)
	tunnel.RegisterStream(stream.ID, stream)

	slog.Info("opened new stream", "streamID", stream.ID, "from", conn.RemoteAddr().String())
	protocol.ForwardToTunnel(stream, tunnel)
}

func main() {
	debug := flag.Bool("debug", true, "enable debug logging")
	publicAddr := flag.String("public", ":8080", "public listener address")
	tunnelAddr := flag.String("tunnel", ":8443", "tunnel listener address")
	flag.Parse()

	level := slog.LevelInfo
	if *debug {
		level = slog.LevelDebug
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: level,
	})))

	server := &Server{
		publicAddr: *publicAddr,
		tunnelAddr: *tunnelAddr,
	}

	// Server must accept two types of connections
	// 1. From the Client (creating the tunnel)
	// 2. From the Users (public)
	// These should be two different ports

	go server.listenTunnel()
	server.listenPublic()

	// TODO: handle graceful shutdowns
}
