package transport

import (
	"errors"
	"io"
	"net"
)

func IsExpectedCloseErr(err error) bool {
	return errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed)
}
