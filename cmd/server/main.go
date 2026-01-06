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

	prevChannelID atomic.Uint32
	tunnel        atomic.Pointer[protocol.Tunnel]
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
	log := slog.With("tunnelRemote", conn.RemoteAddr().String())

	tunnel := protocol.NewTunnel(conn)
	s.tunnel.Store(tunnel)
	log.Info("tunnel connected")

	defer func() {
		s.tunnel.Store(nil)
		tunnel.Close()
		log.Info("tunnel disconnected")
	}()

	// keep alive ping-pong
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-done:
				return
			case <-time.After(30 * time.Second):
				log.Debug("sending ping")
				if err := tunnel.SendPing(); err != nil {
					log.Error("write ping failed, stopping keepalive", "err", err)
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
				log.Error("read from tunnel", "err", err)
			}
			return
		}

		switch frame.Type {
		case protocol.TypeChannelData:
			if channel, ok := tunnel.Channel(frame.ChannelID); ok {
				log.Debug("forwarding packet from tunnel to channel", "channelID", frame.ChannelID, "len", len(frame.Payload))
				if _, err := channel.Conn.Write(frame.Payload); err != nil {
					log.Error("write to channel", "channelID", frame.ChannelID, "err", err)
				}
			} else {
				log.Warn("received data for unknown channel", "channelID", frame.ChannelID)
			}
		case protocol.TypeChannelClose:
			if channel, ok := tunnel.UnregisterChannel(frame.ChannelID); ok {
				log.Info("received ChannelClose, closing channel", "channelID", frame.ChannelID)
				if err := channel.Conn.Close(); err != nil {
					log.Error("close channel connection", "channelID", frame.ChannelID, "err", err)
				}
			}
		case protocol.TypePong:
			log.Debug("received pong")
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

		go s.handlePublicConnection(conn)
	}
}

func (s *Server) handlePublicConnection(conn net.Conn) {
	tunnel := s.tunnel.Load()
	if tunnel == nil {
		slog.Error("no tunnel available")
		conn.Close()
		return
	}

	channel := protocol.NewChannel(s.prevChannelID.Add(1), conn)
	tunnel.RegisterChannel(channel.ID, channel)

	slog.Info("opened new channel", "channelID", channel.ID, "from", conn.RemoteAddr().String())
	channel.RelayThrough(tunnel)
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
