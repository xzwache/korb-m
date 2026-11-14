package ws

import (
	"encoding/binary"
	"errors"
	"unicode/utf8"
)

// CloseCode is the status code carried in a close frame (RFC 6455 §7.4).
type CloseCode uint16

// Close codes used by korb-m.
const (
	CloseNormal          CloseCode = 1000
	CloseGoingAway       CloseCode = 1001
	CloseProtocolError   CloseCode = 1002
	CloseUnsupportedData CloseCode = 1003
	CloseNoStatus        CloseCode = 1005 // local only, never sent
	CloseInvalidPayload  CloseCode = 1007
	ClosePolicyViolation CloseCode = 1008
	CloseMessageTooBig   CloseCode = 1009
	CloseInternalError   CloseCode = 1011
	CloseServiceRestart  CloseCode = 1012
	CloseTryAgainLater   CloseCode = 1013
)

// Close frame errors.
var (
	ErrInvalidClosePayload = errors.New("ws: close payload of 1 byte")
	ErrInvalidCloseCode    = errors.New("ws: invalid close code")
)

// validOnWire reports whether c may appear in a received close frame.
func (c CloseCode) validOnWire() bool {
	switch {
	case c >= 1000 && c <= 1003, c >= 1007 && c <= 1014:
		return true
	case c >= 3000 && c <= 4999: // registered (3xxx) and private (4xxx) codes
		return true
	}
	return false
}

// ParseClose decodes a close frame payload. An empty payload means no
// status was given and returns CloseNoStatus.
func ParseClose(p []byte) (CloseCode, string, error) {
	switch len(p) {
	case 0:
		return CloseNoStatus, "", nil
	case 1:
		return 0, "", ErrInvalidClosePayload
	}
	code := CloseCode(binary.BigEndian.Uint16(p))
	if !code.validOnWire() {
		return 0, "", ErrInvalidCloseCode
	}
	reason := p[2:]
	if !utf8.Valid(reason) {
		return 0, "", ErrInvalidUTF8
	}
	return code, string(reason), nil
}

// AppendClose appends a close frame payload (code + reason) to dst.
func AppendClose(dst []byte, code CloseCode, reason string) []byte {
	dst = binary.BigEndian.AppendUint16(dst, uint16(code))
	return append(dst, reason...)
}

// CloseCodeFor maps a decoding error to the close code to send back.
func CloseCodeFor(err error) CloseCode {
	switch {
	case err == nil:
		return CloseNormal
	case errors.Is(err, ErrInvalidUTF8):
		return CloseInvalidPayload
	case errors.Is(err, ErrMessageTooBig):
		return CloseMessageTooBig
	}
	return CloseProtocolError
}
