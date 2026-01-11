package main

import (
	"context"
	"flag"
	"log/slog"
	"net"
	"os"
	"time"

	"github.com/pkg/errors"
	"github.com/zbsss/tunnels/internal/retry"
	"github.com/zbsss/tunnels/pkg/transport"
)

type Agent struct {
	proxyAddr   string
	backendAddr string

	tunnel *transport.Tunnel
}

func (a *Agent) run() error {
	conn, err := net.Dial("tcp", a.proxyAddr)
	if err != nil {
		return errors.Wrap(err, "dial tunnel")
	}
	a.tunnel = transport.NewTunnel(conn, transport.WithLogger(slog.With("proxyAddr", a.proxyAddr, "backendAddr", a.backendAddr)))
	a.tunnel.Log().Info("tunnel established")

	defer func() {
		a.tunnel.Close()
		a.tunnel.Log().Info("tunnel closed")
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	return a.tunnel.Serve(ctx, a.processFrame)
}

func (a *Agent) processFrame(frame transport.Frame) {
	log := a.tunnel.Log().With("channelID", frame.ChannelID)

	switch frame.Type {
	case transport.TypeChannelData:
		channel, ok := a.tunnel.Channel(frame.ChannelID)
		if !ok {
			var err error
			channel, err = a.openBackendChannel(frame.ChannelID)
			if err != nil {
				log.Error("failed to open connection to backend, sending channel close to tunnel", "err", err)
				err = a.tunnel.SendClose(frame.ChannelID)
				if err != nil {
					log.Error("failed to send channel close", "err", err)
				}
				return
			}
		}
		log.Debug("forwarding data tunnel -> backend", "bytes", len(frame.Payload))
		if _, err := channel.Conn.Write(frame.Payload); err != nil {
			log.Error("failed to write to backend", "err", err)
		}
	case transport.TypePing:
		log.Debug("received keepalive ping")
		if err := a.tunnel.SendPong(); err != nil {
			log.Error("failed to send keepalive pong", "err", err)
		}
	case transport.TypeChannelClose:
		if channel, ok := a.tunnel.UnregisterChannel(frame.ChannelID); ok {
			log.Info("closing channel on remote request", "channelID", frame.ChannelID)
			if err := channel.Conn.Close(); err != nil {
				log.Error("failed to close channel connection", "channelID", frame.ChannelID, "err", err)
			}
		}
	}
}

func (a *Agent) openBackendChannel(channelID uint32) (*transport.Channel, error) {
	var backendConn net.Conn
	err := retry.Do(5, 10*time.Second, func() error {
		var err error
		backendConn, err = net.Dial("tcp", a.backendAddr)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, errors.Wrapf(err, "open connection to backend %q", a.backendAddr)
	}

	channel := transport.NewChannel(channelID, backendConn, transport.WithLogger(a.tunnel.Log().With("backendAddr", a.backendAddr)))
	a.tunnel.RegisterChannel(channel)
	channel.Log().Info("channel opened to backend")

	go channel.RelayThrough(a.tunnel)

	return channel, nil
}

func main() {
	debug := flag.Bool("debug", true, "enable debug logging")
	proxyAddr := flag.String("proxy", ":8443", "tunnel proxy address")
	backendAddr := flag.String("backend", ":42064", "backend service address")
	flag.Parse()

	level := slog.LevelInfo
	if *debug {
		level = slog.LevelDebug
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: level,
	})))

	agent := Agent{
		proxyAddr:   *proxyAddr,
		backendAddr: *backendAddr,
	}

	slog.Info("starting tunnel agent", "proxyAddr", *proxyAddr, "backendAddr", *backendAddr)

	for {
		err := agent.run()
		slog.Error("tunnel connection lost, reconnecting in 5s", "err", err)
		time.Sleep(5 * time.Second)
	}
}
