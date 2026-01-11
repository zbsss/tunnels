package main

import (
	"errors"
	"flag"
	"io"
	"log"
	"log/slog"
	"net"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zbsss/tunnels/pkg/transport"
)

type Proxy struct {
	publicAddr string
	tunnelAddr string

	mu      sync.Mutex
	tunnels []*transport.Tunnel
	// used to generate unique channel IDs per tunnel
	tunnelCounters map[*transport.Tunnel]*atomic.Uint32
	// used to pick the next tunnel in round-robin fashion
	nextTunnelIdx int
}

func (p *Proxy) nextTunnel() *transport.Tunnel {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.tunnels) == 0 {
		return nil
	}

	idx := (p.nextTunnelIdx + 1) % len(p.tunnels)
	p.nextTunnelIdx = idx
	return p.tunnels[idx]
}

func (p *Proxy) addTunnel(t *transport.Tunnel) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.tunnels = append(p.tunnels, t)
	p.tunnelCounters[t] = new(atomic.Uint32)
}

func (p *Proxy) removeTunnel(t *transport.Tunnel) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if i := slices.Index(p.tunnels, t); i != -1 {
		p.tunnels = slices.Delete(p.tunnels, i, i+1)
	}
	delete(p.tunnelCounters, t)
}

func (p *Proxy) nextChannelID(t *transport.Tunnel) uint32 {
	p.mu.Lock()
	counter, ok := p.tunnelCounters[t]
	p.mu.Unlock()

	if !ok {
		// this shouldn't happen
		panic("tunnel not found in counters map")
	}

	return counter.Add(1)
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
	tunnel := transport.NewTunnel(conn)
	p.addTunnel(tunnel)
	tunnel.Log().Info("tunnel established")

	defer func() {
		p.removeTunnel(tunnel)
		tunnel.Close()
		tunnel.Log().Info("tunnel closed")
	}()

	// keep alive ping-pong
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-done:
				return
			case <-time.After(30 * time.Second):
				tunnel.Log().Debug("sending keepalive ping")
				if err := tunnel.SendPing(); err != nil {
					tunnel.Log().Error("failed to send keepalive ping", "err", err)
					return
				}
			}
		}
	}()
	defer close(done)

	for {
		// TODO: this should be moved to tunnel.Read(), this internally should handle retries
		// and should return a custom tunnel closed error
		frame, err := transport.ReadFrame(conn)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				tunnel.Log().Error("failed to read frame from tunnel", "err", err)
			}
			// TODO: should we close tunnel if single read attempt fails?
			// we should only close tunnel if the connection was closed.
			// how to know if it's closed? io.EOF or something else?
			return
		}

		go processFrame(tunnel, &frame)
	}
}

func processFrame(tunnel *transport.Tunnel, frame *transport.Frame) {
	log := tunnel.Log().With("channelID", frame.ChannelID)
	switch frame.Type {
	case transport.TypeChannelData:
		if channel, ok := tunnel.Channel(frame.ChannelID); ok {
			log.Debug("forwarding data tunnel -> client", "bytes", len(frame.Payload))
			if _, err := channel.Conn.Write(frame.Payload); err != nil {
				log.Error("failed to write to channel", "err", err)
			}
		} else {
			log.Warn("received data for unknown channel")
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
	tunnel := p.nextTunnel()
	if tunnel == nil {
		slog.Warn("rejected connection, no tunnel available", "clientAddr", conn.RemoteAddr().String())
		conn.Close()
		return
	}

	channelID := p.nextChannelID(tunnel)
	channel := transport.NewChannel(channelID, conn, transport.WithLogger(tunnel.Log()))
	tunnel.RegisterChannel(channel)

	channel.Log().Info("channel opened for public connection")
	channel.RelayThrough(tunnel)
}

func main() {
	debug := flag.Bool("debug", false, "enable debug logging")
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
		publicAddr:     *publicAddr,
		tunnelAddr:     *tunnelAddr,
		tunnelCounters: make(map[*transport.Tunnel]*atomic.Uint32),
	}

	// Proxy must accept two types of connections
	// 1. From the Agent (creating the tunnel)
	// 2. From the Clients (public)

	go proxy.listenTunnel()
	proxy.listenPublic()

	// TODO: handle graceful shutdowns
}
