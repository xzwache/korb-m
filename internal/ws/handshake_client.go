package ws

import (
	"crypto/rand"
	"encoding/base64"
	"errors"
	"strings"
)

// Handshake errors (client side).
var (
	ErrMalformedResponse = errors.New("ws: handshake: malformed response")
	ErrHandshakeRejected = errors.New("ws: handshake: server did not switch protocols")
	ErrBadAccept         = errors.New("ws: handshake: wrong Sec-WebSocket-Accept")
)

// NewKey returns a random Sec-WebSocket-Key.
func NewKey() string {
	var b [16]byte
	_, _ = rand.Read(b[:]) // never fails since Go 1.24
	return base64.StdEncoding.EncodeToString(b[:])
}

// AppendRequest appends a client upgrade request to dst.
func AppendRequest(dst []byte, host, path, key string) []byte {
	dst = append(dst, "GET "...)
	dst = append(dst, path...)
	dst = append(dst, " HTTP/1.1\r\nHost: "...)
	dst = append(dst, host...)
	dst = append(dst, "\r\nUpgrade: websocket\r\n"+
		"Connection: Upgrade\r\n"+
		"Sec-WebSocket-Version: 13\r\n"+
		"Sec-WebSocket-Key: "...)
	dst = append(dst, key...)
	return append(dst, "\r\n\r\n"...)
}

// ParseResponse checks the server's handshake response for the request
// sent with key and returns the size of the response head.
func ParseResponse(b []byte, key string) (int, error) {
	start, lines, n, err := readHead(b)
	if err != nil {
		if errors.Is(err, ErrHeadTooLarge) {
			return 0, ErrMalformedResponse
		}
		return 0, err
	}

	proto, rest, _ := strings.Cut(start, " ")
	if proto != "HTTP/1.1" {
		return 0, ErrMalformedResponse
	}
	if status, _, _ := strings.Cut(rest, " "); status != "101" {
		return 0, ErrHandshakeRejected
	}

	var upgrade, connection, accepted bool
	for _, line := range lines {
		name, value, err := splitHeader(line)
		if err != nil {
			return 0, ErrMalformedResponse
		}
		switch {
		case strings.EqualFold(name, "Upgrade"):
			upgrade = hasToken(value, "websocket")
		case strings.EqualFold(name, "Connection"):
			connection = hasToken(value, "upgrade")
		case strings.EqualFold(name, "Sec-WebSocket-Accept"):
			accepted = value == acceptKey(key)
		}
	}
	if !upgrade || !connection {
		return 0, ErrMalformedResponse
	}
	if !accepted {
		return 0, ErrBadAccept
	}
	return n, nil
}
