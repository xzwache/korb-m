package ws

import (
	"errors"
	"unicode/utf8"
)

// Side tells the decoder which end of the connection it runs on.
type Side int

// Server decoders require masked frames, client decoders require unmasked.
const (
	SideServer Side = iota
	SideClient
)

// Message errors.
var (
	ErrMaskMismatch           = errors.New("ws: wrong frame masking for this side")
	ErrUnexpectedContinuation = errors.New("ws: continuation frame without a message in progress")
	ErrExpectedContinuation   = errors.New("ws: new message started before previous one finished")
	ErrMessageTooBig          = errors.New("ws: message exceeds size limit")
	ErrInvalidUTF8            = errors.New("ws: invalid UTF-8 in text")
)

// Message is a complete data message or a single control frame.
type Message struct {
	Opcode  Opcode // OpText, OpBinary, OpClose, OpPing or OpPong
	Payload []byte // valid only until the next call to Decoder.Next
}

// Decoder turns a byte stream into messages. It keeps the state of a
// partially received frame, so input can arrive in chunks of any size.
// After any error other than ErrNeedMore the connection must be closed.
type Decoder struct {
	side    Side
	maxSize int64

	hdr       Header
	inFrame   bool  // header parsed, payload still arriving
	remaining int64 // payload bytes left in the current frame
	maskPos   int

	msgOp Opcode // opcode of the data message being assembled; 0 = none
	msg   []byte // data message payload so far
	ctl   []byte // current control frame payload (<= 125 bytes)
}

// NewDecoder returns a decoder for the given side that rejects data
// messages larger than maxSize bytes.
func NewDecoder(side Side, maxSize int64) *Decoder {
	return &Decoder{side: side, maxSize: maxSize}
}

// Next consumes bytes from b until it has a complete message, and returns
// the message and the number of bytes consumed. If b runs out first it
// returns ErrNeedMore: drop b[:n], read more, and call Next again.
//
// TODO(mem): release msg buffer after large messages so idle conns stay small.
func (d *Decoder) Next(b []byte) (Message, int, error) {
	n := 0
	for {
		if !d.inFrame {
			h, hn, err := ParseHeader(b[n:])
			if err != nil {
				return Message{}, n, err
			}
			if err := d.startFrame(h); err != nil {
				return Message{}, n, err
			}
			n += hn
		}

		take := min(int64(len(b)-n), d.remaining)
		chunk := b[n : n+int(take)]
		n += int(take)
		d.remaining -= take
		if d.hdr.Opcode.IsControl() {
			d.ctl = d.appendPayload(d.ctl, chunk)
		} else {
			d.msg = d.appendPayload(d.msg, chunk)
		}
		if d.remaining > 0 {
			return Message{}, n, ErrNeedMore
		}
		d.inFrame = false

		if d.hdr.Opcode.IsControl() {
			if d.hdr.Opcode == OpClose {
				if _, _, err := ParseClose(d.ctl); err != nil {
					return Message{}, n, err
				}
			}
			return Message{Opcode: d.hdr.Opcode, Payload: d.ctl}, n, nil
		}
		if !d.hdr.Fin {
			continue // more fragments to come
		}

		op := d.msgOp
		d.msgOp = 0
		if op == OpText && !utf8.Valid(d.msg) {
			return Message{}, n, ErrInvalidUTF8
		}
		return Message{Opcode: op, Payload: d.msg}, n, nil
	}
}

// startFrame validates a new frame header against the decoder state.
func (d *Decoder) startFrame(h Header) error {
	if h.Masked != (d.side == SideServer) {
		return ErrMaskMismatch
	}

	switch h.Opcode {
	case OpText, OpBinary:
		if d.msgOp != 0 {
			return ErrExpectedContinuation
		}
		d.msgOp = h.Opcode
		d.msg = d.msg[:0]
	case OpContinuation:
		if d.msgOp == 0 {
			return ErrUnexpectedContinuation
		}
	default: // control frame
		d.ctl = d.ctl[:0]
	}

	if !h.Opcode.IsControl() && int64(len(d.msg))+h.Length > d.maxSize {
		return ErrMessageTooBig
	}

	d.hdr = h
	d.inFrame = true
	d.remaining = h.Length
	d.maskPos = 0
	return nil
}

// appendPayload copies chunk to dst and unmasks the copy (not the input).
func (d *Decoder) appendPayload(dst, chunk []byte) []byte {
	start := len(dst)
	dst = append(dst, chunk...)
	if d.hdr.Masked {
		d.maskPos = Mask(d.hdr.Mask, d.maskPos, dst[start:])
	}
	return dst
}

// AppendFrame appends a complete unmasked frame (as sent by a server) to dst.
func AppendFrame(dst []byte, op Opcode, payload []byte) []byte {
	dst = AppendHeader(dst, Header{Fin: true, Opcode: op, Length: int64(len(payload))})
	return append(dst, payload...)
}
