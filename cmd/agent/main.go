package main

import (
	"flag"
	"log/slog"
	"net"
	"os"
	"time"

	"github.com/pkg/errors"
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
	a.tunnel = transport.NewTunnel(conn)

	tunnelLog := slog.With("component", "tunnel", "proxyAddr", a.proxyAddr, "backendAddr", a.backendAddr)
	tunnelLog.Info("tunnel established")

	defer func() {
		a.tunnel.Close()
		tunnelLog.Info("tunnel closed")
	}()

	for {
		frame, err := transport.ReadFrame(conn)
		if err != nil {
			return errors.Wrap(err, "read from tunnel")
		}

		switch frame.Type {
		case transport.TypeChannelData:
			channel, ok := a.tunnel.Channel(frame.ChannelID)
			if !ok {
				channel, err = a.openBackendChannel(frame.ChannelID)
				if err != nil {
					return errors.Wrapf(err, "open channel %d", frame.ChannelID)
				}
			}
			tunnelLog.Debug("forwarding data tunnel -> backend", "channelID", frame.ChannelID, "bytes", len(frame.Payload))
			if _, err := channel.Conn.Write(frame.Payload); err != nil {
				tunnelLog.Error("failed to write to backend", "channelID", frame.ChannelID, "err", err)
			}
		case transport.TypePing:
			tunnelLog.Debug("received keepalive ping")
			if err := a.tunnel.SendPong(); err != nil {
				tunnelLog.Error("failed to send keepalive pong", "err", err)
			}
		case transport.TypeChannelClose:
			if channel, ok := a.tunnel.UnregisterChannel(frame.ChannelID); ok {
				tunnelLog.Info("closing channel on remote request", "channelID", frame.ChannelID)
				if err := channel.Conn.Close(); err != nil {
					tunnelLog.Error("failed to close channel connection", "channelID", frame.ChannelID, "err", err)
				}
			}
		}
	}
}

func (a *Agent) openBackendChannel(channelID uint32) (*transport.Channel, error) {
	backendConn, err := net.Dial("tcp", a.backendAddr)
	if err != nil {
		return nil, errors.Wrap(err, "dial backend")
	}

	channel := transport.NewChannel(channelID, backendConn)
	a.tunnel.RegisterChannel(channelID, channel)

	channelLog := slog.With("component", "channel", "channelID", channelID, "backendAddr", a.backendAddr)
	channelLog.Info("channel opened to backend")

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
		if err != nil {
			slog.Error("tunnel connection lost, reconnecting in 5s", "err", err)
			time.Sleep(5 * time.Second)
		}
	}
}
