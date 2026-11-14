package ws

import (
	"bytes"
	"crypto/sha1" //nolint:gosec // SHA-1 is mandated by RFC 6455, not used for security
	"encoding/base64"
	"strconv"
	"strings"
)

const acceptGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

// MaxHeadSize limits the HTTP request/response head of a handshake.
const MaxHeadSize = 4096

// HandshakeError is a rejected upgrade request and the HTTP status to answer with.
type HandshakeError struct {
	Status int
	Reason string
}

func (e *HandshakeError) Error() string { return "ws: handshake: " + e.Reason }

// Handshake errors (server side).
var (
	ErrHeadTooLarge     = &HandshakeError{431, "request header fields too large"}
	ErrMalformedRequest = &HandshakeError{400, "malformed request"}
	ErrMethodNotAllowed = &HandshakeError{405, "method not allowed"}
	ErrMissingHost      = &HandshakeError{400, "missing Host header"}
	ErrNotUpgrade       = &HandshakeError{400, "not a websocket upgrade request"}
	ErrBadKey           = &HandshakeError{400, "invalid Sec-WebSocket-Key"}
	ErrBadVersion       = &HandshakeError{426, "unsupported Sec-WebSocket-Version"}
)

// Request is a parsed upgrade request. Fields point into the request head.
type Request struct {
	Path   string
	Host   string
	Origin string // empty for non-browser clients
	Key    string
}

// ParseRequest parses a websocket upgrade request at the start of b and
// returns it with the size of the request head. It returns ErrNeedMore
// until the full head ("\r\n\r\n") has arrived.
//
// TODO: subprotocols (Sec-WebSocket-Protocol) and permessage-deflate.
func ParseRequest(b []byte) (Request, int, error) {
	start, lines, n, err := readHead(b)
	if err != nil {
		return Request{}, 0, err
	}

	method, rest, ok1 := strings.Cut(start, " ")
	target, proto, ok2 := strings.Cut(rest, " ")
	if !ok1 || !ok2 || !strings.HasPrefix(target, "/") || proto != "HTTP/1.1" {
		return Request{}, 0, ErrMalformedRequest
	}
	if method != "GET" {
		return Request{}, 0, ErrMethodNotAllowed
	}

	var r Request
	r.Path = target
	var upgrade, connection, version bool
	for _, line := range lines {
		name, value, err := splitHeader(line)
		if err != nil {
			return Request{}, 0, ErrMalformedRequest
		}
		switch {
		case strings.EqualFold(name, "Host"):
			r.Host = value
		case strings.EqualFold(name, "Origin"):
			r.Origin = value
		case strings.EqualFold(name, "Upgrade"):
			upgrade = upgrade || hasToken(value, "websocket")
		case strings.EqualFold(name, "Connection"):
			connection = connection || hasToken(value, "upgrade")
		case strings.EqualFold(name, "Sec-WebSocket-Version"):
			version = value == "13"
		case strings.EqualFold(name, "Sec-WebSocket-Key"):
			if r.Key != "" {
				return Request{}, 0, ErrBadKey
			}
			r.Key = value
		}
	}

	switch {
	case r.Host == "":
		return Request{}, 0, ErrMissingHost
	case !upgrade || !connection:
		return Request{}, 0, ErrNotUpgrade
	case !version:
		return Request{}, 0, ErrBadVersion
	case !validKey(r.Key):
		return Request{}, 0, ErrBadKey
	}
	return r, n, nil
}

// AppendAcceptResponse appends the "101 Switching Protocols" response for key.
func AppendAcceptResponse(dst []byte, key string) []byte {
	dst = append(dst, "HTTP/1.1 101 Switching Protocols\r\n"+
		"Upgrade: websocket\r\n"+
		"Connection: Upgrade\r\n"+
		"Sec-WebSocket-Accept: "...)
	dst = append(dst, acceptKey(key)...)
	return append(dst, "\r\n\r\n"...)
}

// AppendErrorResponse appends an HTTP error response for a failed handshake.
func AppendErrorResponse(dst []byte, e *HandshakeError) []byte {
	dst = append(dst, "HTTP/1.1 "...)
	dst = strconv.AppendInt(dst, int64(e.Status), 10)
	dst = append(dst, ' ')
	dst = append(dst, e.Reason...)
	dst = append(dst, "\r\nConnection: close\r\nContent-Length: 0\r\n"...)
	if e == ErrBadVersion {
		dst = append(dst, "Sec-WebSocket-Version: 13\r\n"...)
	}
	return append(dst, "\r\n"...)
}

func acceptKey(key string) string {
	sum := sha1.Sum([]byte(key + acceptGUID)) //nolint:gosec // see import
	return base64.StdEncoding.EncodeToString(sum[:])
}

// validKey reports whether key is base64 of exactly 16 bytes.
func validKey(key string) bool {
	if len(key) != 24 {
		return false
	}
	var buf [18]byte
	n, err := base64.StdEncoding.Decode(buf[:], []byte(key))
	return err == nil && n == 16
}

// readHead finds the end of an HTTP head and splits it into the start line
// and header lines. n is the head size including the final "\r\n\r\n".
func readHead(b []byte) (start string, lines []string, n int, err error) {
	end := bytes.Index(b[:min(len(b), MaxHeadSize)], []byte("\r\n\r\n"))
	if end < 0 {
		if len(b) >= MaxHeadSize {
			return "", nil, 0, ErrHeadTooLarge
		}
		return "", nil, 0, ErrNeedMore
	}
	all := strings.Split(string(b[:end]), "\r\n")
	return all[0], all[1:], end + 4, nil
}

// splitHeader splits "Name: value" and trims spaces around the value.
func splitHeader(line string) (name, value string, err error) {
	name, value, ok := strings.Cut(line, ":")
	if !ok || name == "" || strings.ContainsAny(name, " \t") || strings.ContainsAny(line, "\r\n") {
		return "", "", ErrMalformedRequest
	}
	return name, strings.Trim(value, " \t"), nil
}

// hasToken reports whether a comma-separated header value contains token
// (case-insensitive), e.g. "keep-alive, Upgrade" contains "upgrade".
func hasToken(list, token string) bool {
	for part := range strings.SplitSeq(list, ",") {
		if strings.EqualFold(strings.TrimSpace(part), token) {
			return true
		}
	}
	return false
}
