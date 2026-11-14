package ws

import (
	"bytes"
	"errors"
	"testing"
)

var testKey = [4]byte{0x37, 0xfa, 0x21, 0x3d}

// clientFrame builds a masked frame, as a browser would send it.
func clientFrame(fin bool, op Opcode, payload string) []byte {
	b := AppendHeader(nil, Header{Fin: fin, Opcode: op, Masked: true, Mask: testKey, Length: int64(len(payload))})
	start := len(b)
	b = append(b, payload...)
	Mask(testKey, 0, b[start:])
	return b
}

func concat(frames ...[]byte) []byte { return bytes.Join(frames, nil) }

type result struct {
	op      Opcode
	payload string
}

// decodeAll feeds stream to a server decoder in chunks of chunkSize bytes,
// like a socket that returns small reads, and collects all messages.
func decodeAll(stream []byte, chunkSize int, maxSize int64) ([]result, error) {
	d := NewDecoder(SideServer, maxSize)
	var got []result
	var pending []byte
	for len(stream) > 0 || len(pending) > 0 {
		k := min(chunkSize, len(stream))
		pending = append(pending, stream[:k]...)
		stream = stream[k:]
		for {
			m, n, err := d.Next(pending)
			pending = pending[n:]
			if errors.Is(err, ErrNeedMore) {
				break
			}
			if err != nil {
				return got, err
			}
			got = append(got, result{m.Opcode, string(m.Payload)})
		}
		if k == 0 { // stream exhausted with a partial frame left
			return got, ErrNeedMore
		}
	}
	return got, nil
}

func TestDecoderMessages(t *testing.T) {
	tests := []struct {
		name   string
		stream []byte
		want   []result
	}{
		{
			name:   "single text",
			stream: clientFrame(true, OpText, "Hello"),
			want:   []result{{OpText, "Hello"}},
		},
		{
			name:   "empty binary",
			stream: clientFrame(true, OpBinary, ""),
			want:   []result{{OpBinary, ""}},
		},
		{
			name: "fragmented text",
			stream: concat(
				clientFrame(false, OpText, "Hel"),
				clientFrame(false, OpContinuation, ""),
				clientFrame(true, OpContinuation, "lo"),
			),
			want: []result{{OpText, "Hello"}},
		},
		{
			name: "ping between fragments",
			stream: concat(
				clientFrame(false, OpText, "Hel"),
				clientFrame(true, OpPing, "p"),
				clientFrame(true, OpContinuation, "lo"),
			),
			want: []result{{OpPing, "p"}, {OpText, "Hello"}},
		},
		{
			name:   "utf-8 rune split across fragments",
			stream: concat(clientFrame(false, OpText, "\xe2\x82"), clientFrame(true, OpContinuation, "\xac")),
			want:   []result{{OpText, "€"}},
		},
		{
			name: "several messages back to back",
			stream: concat(
				clientFrame(true, OpText, "a"),
				clientFrame(true, OpBinary, "bb"),
				clientFrame(true, OpPong, ""),
				clientFrame(true, OpClose, string(AppendClose(nil, CloseNormal, "bye"))),
			),
			want: []result{{OpText, "a"}, {OpBinary, "bb"}, {OpPong, ""}, {OpClose, "\x03\xe8bye"}},
		},
		{
			name:   "message exactly at size limit",
			stream: clientFrame(true, OpBinary, string(make([]byte, 64))),
			want:   []result{{OpBinary, string(make([]byte, 64))}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for _, chunk := range []int{1, 2, 3, 7, len(tc.stream)} {
				got, err := decodeAll(tc.stream, chunk, 64)
				if err != nil {
					t.Fatalf("chunk %d: %v", chunk, err)
				}
				if len(got) != len(tc.want) {
					t.Fatalf("chunk %d: got %d messages %+v, want %+v", chunk, len(got), got, tc.want)
				}
				for i := range got {
					if got[i] != tc.want[i] {
						t.Fatalf("chunk %d: message %d = %+v, want %+v", chunk, i, got[i], tc.want[i])
					}
				}
			}
		})
	}
}

func TestDecoderErrors(t *testing.T) {
	unmasked := AppendFrame(nil, OpText, []byte("hi"))
	tests := []struct {
		name   string
		stream []byte
		want   error
		code   CloseCode
	}{
		{"unmasked client frame", unmasked, ErrMaskMismatch, CloseProtocolError},
		{"continuation first", clientFrame(true, OpContinuation, "x"), ErrUnexpectedContinuation, CloseProtocolError},
		{
			"new message inside fragmented one",
			concat(clientFrame(false, OpText, "a"), clientFrame(true, OpText, "b")),
			ErrExpectedContinuation, CloseProtocolError,
		},
		{"single frame too big", clientFrame(true, OpBinary, string(make([]byte, 65))), ErrMessageTooBig, CloseMessageTooBig},
		{
			"fragments add up to too big",
			concat(clientFrame(false, OpBinary, string(make([]byte, 40))), clientFrame(true, OpContinuation, string(make([]byte, 25)))),
			ErrMessageTooBig, CloseMessageTooBig,
		},
		{"invalid utf-8 text", clientFrame(true, OpText, "\xff"), ErrInvalidUTF8, CloseInvalidPayload},
		{"close with 1-byte payload", clientFrame(true, OpClose, "\x03"), ErrInvalidClosePayload, CloseProtocolError},
		{"close with code 1005", clientFrame(true, OpClose, "\x03\xed"), ErrInvalidCloseCode, CloseProtocolError},
		{"close with invalid utf-8 reason", clientFrame(true, OpClose, "\x03\xe8\xff"), ErrInvalidUTF8, CloseInvalidPayload},
		{"frame-level error passes through", []byte{0xC1, 0x80}, ErrReservedBits, CloseProtocolError},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := decodeAll(tc.stream, len(tc.stream), 64)
			if !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
			if c := CloseCodeFor(err); c != tc.code {
				t.Errorf("close code = %d, want %d", c, tc.code)
			}
		})
	}
}

func TestDecoderClientSide(t *testing.T) {
	d := NewDecoder(SideClient, 64)
	m, n, err := d.Next(AppendFrame(nil, OpText, []byte("Hello")))
	if err != nil || n != 7 || m.Opcode != OpText || string(m.Payload) != "Hello" {
		t.Fatalf("got %v %d %q, err %v", m.Opcode, n, m.Payload, err)
	}
	if _, _, err := d.Next(clientFrame(true, OpText, "x")); !errors.Is(err, ErrMaskMismatch) {
		t.Fatalf("masked frame to client: err = %v, want ErrMaskMismatch", err)
	}
}

func TestDecoderDoesNotModifyInput(t *testing.T) {
	in := clientFrame(true, OpText, "Hello")
	orig := append([]byte{}, in...)
	if _, _, err := NewDecoder(SideServer, 64).Next(in); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(in, orig) {
		t.Fatal("decoder modified its input")
	}
}

func TestParseClose(t *testing.T) {
	code, reason, err := ParseClose(AppendClose(nil, CloseGoingAway, "restart"))
	if err != nil || code != CloseGoingAway || reason != "restart" {
		t.Fatalf("got %d %q %v", code, reason, err)
	}
	code, _, err = ParseClose(nil)
	if err != nil || code != CloseNoStatus {
		t.Fatalf("empty payload: got %d %v", code, err)
	}
	for _, c := range []CloseCode{999, 1004, 1005, 1006, 1015, 2999, 5000} {
		if _, _, err := ParseClose(AppendClose(nil, c, "")); !errors.Is(err, ErrInvalidCloseCode) {
			t.Errorf("code %d: err = %v, want ErrInvalidCloseCode", c, err)
		}
	}
}

// Decoding must give the same result no matter how the input is split.
func FuzzDecoder(f *testing.F) {
	f.Add(concat(clientFrame(false, OpText, "Hel"), clientFrame(true, OpPing, "p"), clientFrame(true, OpContinuation, "lo")), 1)
	f.Add(clientFrame(true, OpClose, "\x03\xe8ok"), 3)
	f.Add(clientFrame(true, OpBinary, string(make([]byte, 70))), 5)

	f.Fuzz(func(t *testing.T, stream []byte, chunk int) {
		if chunk <= 0 || chunk > len(stream) {
			chunk = max(1, len(stream))
		}
		whole, errWhole := decodeAll(stream, max(1, len(stream)), 64)
		split, errSplit := decodeAll(stream, chunk, 64)
		if !errors.Is(errSplit, errWhole) && !errors.Is(errWhole, errSplit) {
			t.Fatalf("errors differ: whole %v, chunk %d %v", errWhole, chunk, errSplit)
		}
		if len(whole) != len(split) {
			t.Fatalf("message count differs: %d vs %d", len(whole), len(split))
		}
		for i := range whole {
			if whole[i] != split[i] {
				t.Fatalf("message %d differs", i)
			}
		}
	})
}

func BenchmarkDecoderSmallText(b *testing.B) {
	frame := clientFrame(true, OpText, "hello, this is a typical chat message")
	d := NewDecoder(SideServer, 1<<20)
	b.SetBytes(int64(len(frame)))
	for b.Loop() {
		if _, _, err := d.Next(frame); err != nil {
			b.Fatal(err)
		}
	}
}
