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

	"github.com/zbsss/tunnels/pkg/transport"
)

type Proxy struct {
	publicAddr string
	tunnelAddr string

	prevChannelID atomic.Uint32
	tunnel        atomic.Pointer[transport.Tunnel]
}

func (p *Proxy) listenTunnel() {
	lis, err := net.Listen("tcp", p.tunnelAddr)
	if err != nil {
		log.Fatalf("tunnel listen: %+v", err)
	}

	defer lis.Close()
	slog.Info("listening for tunnel connections", "listenAddr", p.tunnelAddr)

	for {
		conn, err := lis.Accept()
		if err != nil {
			slog.Error("failed to accept tunnel connection", "err", err)
			continue
		}

		go p.handleTunnel(conn)
	}
}

func (p *Proxy) handleTunnel(conn net.Conn) {
	log := slog.With("component", "tunnel", "agentAddr", conn.RemoteAddr().String())

	tunnel := transport.NewTunnel(conn)
	p.tunnel.Store(tunnel)
	log.Info("tunnel established")

	defer func() {
		p.tunnel.Store(nil)
		tunnel.Close()
		log.Info("tunnel closed")
	}()

	// keep alive ping-pong
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-done:
				return
			case <-time.After(30 * time.Second):
				log.Debug("sending keepalive ping")
				if err := tunnel.SendPing(); err != nil {
					log.Error("failed to send keepalive ping", "err", err)
					return
				}
			}
		}
	}()
	defer close(done)

	for {
		frame, err := transport.ReadFrame(conn)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				log.Error("failed to read frame from tunnel", "err", err)
			}
			return
		}

		switch frame.Type {
		case transport.TypeChannelData:
			if channel, ok := tunnel.Channel(frame.ChannelID); ok {
				log.Debug("forwarding data tunnel -> client", "channelID", frame.ChannelID, "bytes", len(frame.Payload))
				if _, err := channel.Conn.Write(frame.Payload); err != nil {
					log.Error("failed to write to channel", "channelID", frame.ChannelID, "err", err)
				}
			} else {
				log.Warn("received data for unknown channel", "channelID", frame.ChannelID)
			}
		case transport.TypeChannelClose:
			if channel, ok := tunnel.UnregisterChannel(frame.ChannelID); ok {
				log.Info("closing channel on remote request", "channelID", frame.ChannelID)
				if err := channel.Conn.Close(); err != nil {
					log.Error("failed to close channel connection", "channelID", frame.ChannelID, "err", err)
				}
			}
		case transport.TypePong:
			log.Debug("received keepalive pong")
		}
	}
}

func (p *Proxy) listenPublic() {
	lis, err := net.Listen("tcp", p.publicAddr)
	if err != nil {
		log.Fatalf("public listen: %+v", err)
	}

	defer lis.Close()
	slog.Info("listening for public connections", "listenAddr", p.publicAddr)

	for {
		conn, err := lis.Accept()
		if err != nil {
			slog.Error("failed to accept public connection", "err", err)
			continue
		}

		go p.handlePublicConnection(conn)
	}
}

func (p *Proxy) handlePublicConnection(conn net.Conn) {
	tunnel := p.tunnel.Load()
	if tunnel == nil {
		slog.Warn("rejected connection, no tunnel available", "clientAddr", conn.RemoteAddr().String())
		conn.Close()
		return
	}

	channel := transport.NewChannel(p.prevChannelID.Add(1), conn)
	tunnel.RegisterChannel(channel.ID, channel)

	log := slog.With("component", "channel", "channelID", channel.ID, "clientAddr", conn.RemoteAddr().String())
	log.Info("channel opened for public connection")
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

	proxy := &Proxy{
		publicAddr: *publicAddr,
		tunnelAddr: *tunnelAddr,
	}

	// Proxy must accept two types of connections
	// 1. From the Agent (creating the tunnel)
	// 2. From the Clients (public)

	go proxy.listenTunnel()
	proxy.listenPublic()

	// TODO: handle graceful shutdowns
}
