package ws

import (
	"bytes"
	"errors"
	"testing"
)

// RFC 6455 §5.7 examples plus length-encoding boundaries.
var validHeaders = []struct {
	name string
	wire []byte // header bytes only
	want Header
}{
	{
		name: "rfc unmasked text Hello",
		wire: []byte{0x81, 0x05},
		want: Header{Fin: true, Opcode: OpText, Length: 5},
	},
	{
		name: "rfc masked text Hello",
		wire: []byte{0x81, 0x85, 0x37, 0xfa, 0x21, 0x3d},
		want: Header{Fin: true, Opcode: OpText, Masked: true, Mask: [4]byte{0x37, 0xfa, 0x21, 0x3d}, Length: 5},
	},
	{
		name: "rfc fragmented first frame",
		wire: []byte{0x01, 0x03},
		want: Header{Fin: false, Opcode: OpText, Length: 3},
	},
	{
		name: "rfc fragmented last frame",
		wire: []byte{0x80, 0x02},
		want: Header{Fin: true, Opcode: OpContinuation, Length: 2},
	},
	{
		name: "rfc unmasked ping",
		wire: []byte{0x89, 0x05},
		want: Header{Fin: true, Opcode: OpPing, Length: 5},
	},
	{
		name: "rfc 256 bytes binary (16-bit length)",
		wire: []byte{0x82, 0x7E, 0x01, 0x00},
		want: Header{Fin: true, Opcode: OpBinary, Length: 256},
	},
	{
		name: "rfc 64KiB binary (64-bit length)",
		wire: []byte{0x82, 0x7F, 0, 0, 0, 0, 0, 0x01, 0x00, 0x00},
		want: Header{Fin: true, Opcode: OpBinary, Length: 65536},
	},
	{
		name: "empty payload",
		wire: []byte{0x82, 0x00},
		want: Header{Fin: true, Opcode: OpBinary, Length: 0},
	},
	{
		name: "largest 7-bit length",
		wire: []byte{0x82, 0x7D},
		want: Header{Fin: true, Opcode: OpBinary, Length: 125},
	},
	{
		name: "smallest 16-bit length",
		wire: []byte{0x82, 0x7E, 0x00, 0x7E},
		want: Header{Fin: true, Opcode: OpBinary, Length: 126},
	},
	{
		name: "largest 16-bit length",
		wire: []byte{0x82, 0x7E, 0xFF, 0xFF},
		want: Header{Fin: true, Opcode: OpBinary, Length: 65535},
	},
	{
		name: "largest 64-bit length, masked (max header size)",
		wire: []byte{0x82, 0xFF, 0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 1, 2, 3, 4},
		want: Header{Fin: true, Opcode: OpBinary, Masked: true, Mask: [4]byte{1, 2, 3, 4}, Length: 1<<63 - 1},
	},
	{
		name: "close with largest control payload",
		wire: []byte{0x88, 0x7D},
		want: Header{Fin: true, Opcode: OpClose, Length: 125},
	},
}

func TestParseHeaderValid(t *testing.T) {
	for _, tc := range validHeaders {
		t.Run(tc.name, func(t *testing.T) {
			// trailing payload bytes must not be consumed
			buf := append(append([]byte{}, tc.wire...), 0xAA, 0xBB)

			h, n, err := ParseHeader(buf)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if n != len(tc.wire) {
				t.Errorf("n = %d, want %d", n, len(tc.wire))
			}
			if h != tc.want {
				t.Errorf("header = %+v, want %+v", h, tc.want)
			}
		})
	}
}

// Every prefix of a valid header must return ErrNeedMore.
func TestParseHeaderIncremental(t *testing.T) {
	for _, tc := range validHeaders {
		t.Run(tc.name, func(t *testing.T) {
			for i := range len(tc.wire) {
				_, n, err := ParseHeader(tc.wire[:i])
				if !errors.Is(err, ErrNeedMore) {
					t.Fatalf("prefix len %d: err = %v, want ErrNeedMore", i, err)
				}
				if n != 0 {
					t.Fatalf("prefix len %d: n = %d, want 0", i, n)
				}
			}
		})
	}
}

func TestParseHeaderErrors(t *testing.T) {
	tests := []struct {
		name string
		wire []byte
		want error
	}{
		{"rsv1 set", []byte{0xC1, 0x00}, ErrReservedBits},
		{"rsv2 set", []byte{0xA1, 0x00}, ErrReservedBits},
		{"rsv3 set", []byte{0x91, 0x00}, ErrReservedBits},
		{"reserved data opcode 0x3", []byte{0x83, 0x00}, ErrReservedOpcode},
		{"reserved control opcode 0xB", []byte{0x8B, 0x00}, ErrReservedOpcode},
		{"fragmented ping", []byte{0x09, 0x00}, ErrFragmentedControl},
		{"ping with 126 length code", []byte{0x89, 0x7E}, ErrControlTooLarge},
		{"close with 127 length code", []byte{0x88, 0x7F}, ErrControlTooLarge},
		{"16-bit length used for 125", []byte{0x82, 0x7E, 0x00, 0x7D}, ErrNonMinimalLength},
		{"16-bit length used for 0", []byte{0x82, 0x7E, 0x00, 0x00}, ErrNonMinimalLength},
		{"64-bit length used for 65535", []byte{0x82, 0x7F, 0, 0, 0, 0, 0, 0, 0xFF, 0xFF}, ErrNonMinimalLength},
		{"64-bit length with MSB set", []byte{0x82, 0x7F, 0x80, 0, 0, 0, 0, 0, 0, 0}, ErrLengthOverflow},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, n, err := ParseHeader(tc.wire)
			if !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
			if n != 0 {
				t.Errorf("n = %d, want 0 on error", n)
			}
		})
	}
}

func TestAppendHeader(t *testing.T) {
	for _, tc := range validHeaders {
		t.Run(tc.name, func(t *testing.T) {
			prefix := []byte{0xEE}
			got := AppendHeader(prefix, tc.want)
			if !bytes.Equal(got[1:], tc.wire) {
				t.Errorf("got % x, want % x", got[1:], tc.wire)
			}
			if got[0] != 0xEE {
				t.Error("AppendHeader overwrote existing dst bytes")
			}
		})
	}
}

func TestMaskRFCExample(t *testing.T) {
	key := [4]byte{0x37, 0xfa, 0x21, 0x3d}
	payload := []byte{0x7f, 0x9f, 0x4d, 0x51, 0x58} // masked "Hello"

	Mask(key, 0, payload)
	if string(payload) != "Hello" {
		t.Fatalf("unmasked = %q, want %q", payload, "Hello")
	}
	Mask(key, 0, payload)
	if !bytes.Equal(payload, []byte{0x7f, 0x9f, 0x4d, 0x51, 0x58}) {
		t.Fatalf("re-masked = % x", payload)
	}
}

func TestMaskChunked(t *testing.T) {
	key := [4]byte{0xde, 0xad, 0xbe, 0xef}
	orig := []byte("the quick brown fox jumps over the lazy dog")

	whole := append([]byte{}, orig...)
	Mask(key, 0, whole)

	for split1 := range len(orig) {
		for split2 := split1; split2 < len(orig); split2++ {
			buf := append([]byte{}, orig...)
			pos := Mask(key, 0, buf[:split1])
			pos = Mask(key, pos, buf[split1:split2])
			Mask(key, pos, buf[split2:])
			if !bytes.Equal(buf, whole) {
				t.Fatalf("splits %d/%d: got % x, want % x", split1, split2, buf, whole)
			}
		}
	}
}

// Run longer with: go test -fuzz=FuzzParseHeader ./internal/ws
func FuzzParseHeader(f *testing.F) {
	for _, tc := range validHeaders {
		f.Add(tc.wire)
	}
	f.Add([]byte{0x82, 0x7E, 0x00, 0x7D})
	f.Add([]byte{0x82, 0x7F, 0x80, 0, 0, 0, 0, 0, 0, 0})

	f.Fuzz(func(t *testing.T, b []byte) {
		h, n, err := ParseHeader(b)
		if err != nil {
			if n != 0 {
				t.Fatalf("n = %d with error %v", n, err)
			}
			return
		}
		if n < 2 || n > MaxHeaderSize || n > len(b) {
			t.Fatalf("n = %d out of bounds (len %d)", n, len(b))
		}
		if h.Length < 0 {
			t.Fatalf("negative length %d", h.Length)
		}
		if got := AppendHeader(nil, h); !bytes.Equal(got, b[:n]) {
			t.Fatalf("round trip: got % x, want % x", got, b[:n])
		}
	})
}

func BenchmarkParseHeader(b *testing.B) {
	wire := []byte{0x82, 0xFE, 0x01, 0x00, 1, 2, 3, 4} // masked, 16-bit length
	for b.Loop() {
		if _, _, err := ParseHeader(wire); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMask(b *testing.B) {
	key := [4]byte{1, 2, 3, 4}
	buf := make([]byte, 4096)
	b.SetBytes(int64(len(buf)))
	for b.Loop() {
		Mask(key, 0, buf)
	}
}
