package main

import (
	"flag"
	"io"
	"log"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zbsss/tunnels/pkg/protocol"
)

type Tunnel struct {
	mu sync.Mutex

	// connection to the Client
	conn net.Conn

	// map from StreamID to Public net.Conn
	streams sync.Map
}

func (t *Tunnel) Write(frame protocol.Frame) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	return protocol.WriteFrame(t.conn, frame)
}

type Server struct {
	publicAddr string
	tunnelAddr string

	nextStreamID atomic.Uint32
	tunnel       atomic.Pointer[Tunnel]
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
	tunnel := &Tunnel{
		conn: conn,
	}
	s.tunnel.Store(tunnel)

	// keep alive ping-pong
	go func() {
		for range time.Tick(30 * time.Second) {
			err := tunnel.Write(protocol.Frame{
				Type:     protocol.TypePing,
				StreamID: 0,
			})
			if err != nil {
				slog.Error("write ping", "err", err)
				continue
			}
		}
	}()

	for {
		frame, err := protocol.ReadFrame(conn)
		if err != nil {
			slog.Error("read from tunnel", "err", err)
			s.tunnel.Store(nil)
			return
		}

		switch frame.Type {
		case protocol.TypeStreamData:
			if conn, ok := s.tunnel.Load().streams.Load(frame.StreamID); ok {
				conn.(net.Conn).Write(frame.Payload)
			}
		case protocol.TypeStreamClose:
			if conn, ok := s.tunnel.Load().streams.LoadAndDelete(frame.StreamID); ok {
				conn.(net.Conn).Close()
			}
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
	defer conn.Close()

	tunnel := s.tunnel.Load()
	if tunnel == nil {
		slog.Error("no tunnel available")
		return
	}

	streamID := s.nextStreamID.Add(1)
	s.tunnel.Load().streams.Store(streamID, conn)
	defer s.tunnel.Load().streams.Delete(streamID)

	log := slog.With("streamID", streamID)

	// Send StreamOpen
	err := tunnel.Write(protocol.Frame{
		Type:     protocol.TypeStreamOpen,
		StreamID: streamID,
	})
	if err != nil {
		log.Error("write stream open", "err", err)
		return
	}
	log.Info("opened new stream")

	// Send StreamClose
	defer func() {
		log.Info("closing stream")
		err = tunnel.Write(protocol.Frame{
			Type:     protocol.TypeStreamClose,
			StreamID: streamID,
		})
		if err != nil {
			log.Error("write stream close", "err", err)
		}
	}()

	// Read 32KB chunk of the payload, create a frame and send it via Tunnel
	buf := make([]byte, 32*1024)
	for {
		n, err := conn.Read(buf)
		if n > 0 {
			wErr := tunnel.Write(protocol.Frame{
				Type:     protocol.TypeStreamData,
				StreamID: streamID,
				Payload:  buf[:n],
			})
			if wErr != nil {
				log.Error("write stream data", "err", wErr)
			}
		}

		if err != nil {
			if err != io.EOF {
				log.Error("read from public", "err", err)
			}

			return
		}
	}
}

func main() {
	publicAddr := flag.String("public", "", "public listener address")
	tunnelAddr := flag.String("tunnel", "", "tunnel listener address")
	flag.Parse()

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
}
