// Package ws implements the WebSocket wire protocol (RFC 6455) over byte
// slices, without doing any I/O.
package ws

import (
	"encoding/binary"
	"errors"
)

// Frame header layout (RFC 6455 §5.2):
//
//	byte 0:  F R R R O O O O    F = FIN, R = reserved, O = opcode
//	byte 1:  M L L L L L L L    M = masked, L = length code
//	then:    2 or 8 bytes extended length  (if length code is 126 or 127)
//	then:    4 bytes masking key           (if M = 1)
//	then:    payload

// Opcode identifies the frame type.
type Opcode byte

// Opcodes defined by RFC 6455.
const (
	OpContinuation Opcode = 0x0
	OpText         Opcode = 0x1
	OpBinary       Opcode = 0x2
	OpClose        Opcode = 0x8
	OpPing         Opcode = 0x9
	OpPong         Opcode = 0xA
)

// IsControl reports whether o is close, ping or pong.
func (o Opcode) IsControl() bool { return o&0x8 != 0 }

func (o Opcode) valid() bool {
	switch o {
	case OpContinuation, OpText, OpBinary, OpClose, OpPing, OpPong:
		return true
	}
	return false
}

func (o Opcode) String() string {
	switch o {
	case OpContinuation:
		return "continuation"
	case OpText:
		return "text"
	case OpBinary:
		return "binary"
	case OpClose:
		return "close"
	case OpPing:
		return "ping"
	case OpPong:
		return "pong"
	}
	return "reserved"
}

// MaxHeaderSize is 2 fixed bytes + 8 length bytes + 4 mask bytes.
const MaxHeaderSize = 14

const maxControlPayload = 125

// Header is a decoded frame header.
type Header struct {
	Fin    bool
	Opcode Opcode
	Masked bool
	Mask   [4]byte
	Length int64
}

// ErrNeedMore means b holds only part of a header: read more and retry.
var ErrNeedMore = errors.New("ws: need more data")

// Protocol errors: the connection must be closed with code 1002.
var (
	ErrReservedBits      = errors.New("ws: reserved bits set")
	ErrReservedOpcode    = errors.New("ws: reserved opcode")
	ErrFragmentedControl = errors.New("ws: fragmented control frame")
	ErrControlTooLarge   = errors.New("ws: control frame payload > 125 bytes")
	ErrNonMinimalLength  = errors.New("ws: payload length not minimally encoded")
	ErrLengthOverflow    = errors.New("ws: payload length has most significant bit set")
)

// ParseHeader decodes the frame header at the start of b and returns it
// with the number of bytes it occupies. The caller checks masking, because
// the rule depends on the side (clients must mask, servers must not).
func ParseHeader(b []byte) (h Header, n int, err error) {
	if len(b) < 2 {
		return Header{}, 0, ErrNeedMore
	}
	b0, b1 := b[0], b[1]

	if b0&0x70 != 0 {
		return Header{}, 0, ErrReservedBits
	}
	h.Fin = b0&0x80 != 0
	h.Opcode = Opcode(b0 & 0x0F)
	if !h.Opcode.valid() {
		return Header{}, 0, ErrReservedOpcode
	}
	h.Masked = b1&0x80 != 0
	lenCode := b1 & 0x7F

	if h.Opcode.IsControl() {
		if !h.Fin {
			return Header{}, 0, ErrFragmentedControl
		}
		if lenCode > maxControlPayload {
			return Header{}, 0, ErrControlTooLarge
		}
	}

	n = 2
	switch lenCode {
	case 126:
		if len(b) < 4 {
			return Header{}, 0, ErrNeedMore
		}
		v := binary.BigEndian.Uint16(b[2:4])
		if v <= 125 {
			return Header{}, 0, ErrNonMinimalLength
		}
		h.Length = int64(v)
		n = 4
	case 127:
		if len(b) < 10 {
			return Header{}, 0, ErrNeedMore
		}
		v := binary.BigEndian.Uint64(b[2:10])
		if v>>63 != 0 {
			return Header{}, 0, ErrLengthOverflow
		}
		if v <= 0xFFFF {
			return Header{}, 0, ErrNonMinimalLength
		}
		h.Length = int64(v) //nolint:gosec // top bit checked above
		n = 10
	default:
		h.Length = int64(lenCode)
	}

	if h.Masked {
		if len(b) < n+4 {
			return Header{}, 0, ErrNeedMore
		}
		copy(h.Mask[:], b[n:n+4])
		n += 4
	}
	return h, n, nil
}

// AppendHeader appends the encoding of h to dst, using the shortest
// length form.
func AppendHeader(dst []byte, h Header) []byte {
	b0 := byte(h.Opcode) & 0x0F
	if h.Fin {
		b0 |= 0x80
	}
	var b1 byte
	if h.Masked {
		b1 = 0x80
	}

	switch {
	case h.Length <= 125:
		dst = append(dst, b0, b1|byte(h.Length))
	case h.Length <= 0xFFFF:
		dst = append(dst, b0, b1|126)
		dst = binary.BigEndian.AppendUint16(dst, uint16(h.Length))
	default:
		dst = append(dst, b0, b1|127)
		dst = binary.BigEndian.AppendUint64(dst, uint64(h.Length))
	}

	if h.Masked {
		dst = append(dst, h.Mask[:]...)
	}
	return dst
}

// Mask XORs b with key in place (masking and unmasking are the same
// operation). pos is the offset of b[0] within the payload, so a payload
// can be processed in chunks; the return value is the pos for the next chunk.
//
// TODO(perf): XOR 8 bytes at a time instead of 1; compare with BenchmarkMask.
func Mask(key [4]byte, pos int, b []byte) int {
	for i := range b {
		b[i] ^= key[(pos+i)&3]
	}
	return (pos + len(b)) & 3
}
