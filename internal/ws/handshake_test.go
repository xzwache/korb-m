package ws

import (
	"errors"
	"strings"
	"testing"
)

const rfcKey = "dGhlIHNhbXBsZSBub25jZQ=="

func crlf(lines ...string) []byte { return []byte(strings.Join(lines, "\r\n") + "\r\n\r\n") }

var chromeRequest = crlf(
	"GET /ws?room=general HTTP/1.1",
	"Host: localhost:8080",
	"Connection: Upgrade",
	"Pragma: no-cache",
	"Cache-Control: no-cache",
	"User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
	"Upgrade: websocket",
	"Origin: http://localhost:8080",
	"Sec-WebSocket-Version: 13",
	"Accept-Encoding: gzip, deflate, br, zstd",
	"Accept-Language: en-US,en;q=0.9",
	"Sec-WebSocket-Key: "+rfcKey,
	"Sec-WebSocket-Extensions: permessage-deflate; client_max_window_bits",
)

var firefoxRequest = crlf(
	"GET /ws HTTP/1.1",
	"Host: localhost:8080",
	"User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:133.0) Gecko/20100101 Firefox/133.0",
	"Accept: */*",
	"Accept-Language: en-US,en;q=0.5",
	"Accept-Encoding: gzip, deflate, br, zstd",
	"Sec-WebSocket-Version: 13",
	"Origin: http://localhost:8080",
	"Sec-WebSocket-Extensions: permessage-deflate",
	"Sec-WebSocket-Key: "+rfcKey,
	"Connection: keep-alive, Upgrade",
	"Sec-Fetch-Dest: empty",
	"Sec-Fetch-Mode: websocket",
	"Sec-Fetch-Site: same-origin",
	"Pragma: no-cache",
	"Cache-Control: no-cache",
	"Upgrade: websocket",
)

func TestAcceptKeyRFC(t *testing.T) {
	if got := acceptKey(rfcKey); got != "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=" {
		t.Fatalf("acceptKey = %q", got)
	}
}

func TestParseRequestBrowsers(t *testing.T) {
	tests := []struct {
		name string
		req  []byte
		path string
	}{
		{"chrome", chromeRequest, "/ws?room=general"},
		{"firefox", firefoxRequest, "/ws"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// bytes after the head must not be consumed
			buf := append(append([]byte{}, tc.req...), 0x81)
			r, n, err := ParseRequest(buf)
			if err != nil {
				t.Fatal(err)
			}
			if n != len(tc.req) {
				t.Errorf("n = %d, want %d", n, len(tc.req))
			}
			want := Request{Path: tc.path, Host: "localhost:8080", Origin: "http://localhost:8080", Key: rfcKey}
			if r != want {
				t.Errorf("got %+v, want %+v", r, want)
			}
		})
	}
}

func TestParseRequestIncremental(t *testing.T) {
	for i := range len(chromeRequest) {
		if _, _, err := ParseRequest(chromeRequest[:i]); !errors.Is(err, ErrNeedMore) {
			t.Fatalf("prefix %d: err = %v, want ErrNeedMore", i, err)
		}
	}
}

func TestParseRequestErrors(t *testing.T) {
	base := []string{
		"GET /ws HTTP/1.1",
		"Host: x",
		"Upgrade: websocket",
		"Connection: Upgrade",
		"Sec-WebSocket-Version: 13",
		"Sec-WebSocket-Key: " + rfcKey,
	}
	with := func(i int, line string) []byte {
		l := append([]string{}, base...)
		l[i] = line
		return crlf(l...)
	}
	tests := []struct {
		name string
		req  []byte
		want *HandshakeError
	}{
		{"POST", with(0, "POST /ws HTTP/1.1"), ErrMethodNotAllowed},
		{"HTTP/1.0", with(0, "GET /ws HTTP/1.0"), ErrMalformedRequest},
		{"absolute target", with(0, "GET http://x/ws HTTP/1.1"), ErrMalformedRequest},
		{"garbage start line", with(0, "hello"), ErrMalformedRequest},
		{"no Host", with(1, "X-Foo: bar"), ErrMissingHost},
		{"no Upgrade", with(2, "Upgrade: h2c"), ErrNotUpgrade},
		{"no Connection upgrade", with(3, "Connection: keep-alive"), ErrNotUpgrade},
		{"old version", with(4, "Sec-WebSocket-Version: 8"), ErrBadVersion},
		{"short key", with(5, "Sec-WebSocket-Key: c2hvcnQ="), ErrBadKey},
		{"key not base64", with(5, "Sec-WebSocket-Key: !!!!!!!!!!!!!!!!!!!!!!=="), ErrBadKey},
		{"duplicate key", crlf(append(base, "Sec-WebSocket-Key: "+rfcKey)...), ErrBadKey},
		{"header without colon", with(1, "Host x"), ErrMalformedRequest},
		{"space in header name", with(1, "Ho st: x"), ErrMalformedRequest},
		{"folded header line", with(1, " Host: x"), ErrMalformedRequest},
		{"head too large", crlf(append(base, "X-Big: "+strings.Repeat("a", MaxHeadSize))...), ErrHeadTooLarge},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, n, err := ParseRequest(tc.req)
			if !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
			if n != 0 {
				t.Errorf("n = %d, want 0", n)
			}
		})
	}
}

func TestAcceptResponseRFC(t *testing.T) {
	got := string(AppendAcceptResponse(nil, rfcKey))
	want := string(crlf(
		"HTTP/1.1 101 Switching Protocols",
		"Upgrade: websocket",
		"Connection: Upgrade",
		"Sec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=",
	))
	if got != want {
		t.Fatalf("got\n%q\nwant\n%q", got, want)
	}
}

func TestErrorResponse(t *testing.T) {
	got := string(AppendErrorResponse(nil, ErrBadVersion))
	want := string(crlf(
		"HTTP/1.1 426 unsupported Sec-WebSocket-Version",
		"Connection: close",
		"Content-Length: 0",
		"Sec-WebSocket-Version: 13",
	))
	if got != want {
		t.Fatalf("got\n%q\nwant\n%q", got, want)
	}
	if strings.Contains(string(AppendErrorResponse(nil, ErrBadKey)), "Sec-WebSocket-Version") {
		t.Error("version header sent for non-version error")
	}
}

func TestHandshakeRoundTrip(t *testing.T) {
	key := NewKey()
	req := AppendRequest(nil, "example.com", "/ws", key)

	r, n, err := ParseRequest(req)
	if err != nil || n != len(req) {
		t.Fatalf("server: n=%d err=%v", n, err)
	}
	resp := AppendAcceptResponse(nil, r.Key)

	n, err = ParseResponse(resp, key)
	if err != nil || n != len(resp) {
		t.Fatalf("client: n=%d err=%v", n, err)
	}
}

func TestParseResponseErrors(t *testing.T) {
	ok := AppendAcceptResponse(nil, rfcKey)
	tests := []struct {
		name string
		resp []byte
		key  string
		want error
	}{
		{"wrong key", ok, NewKey(), ErrBadAccept},
		{"rejected", AppendErrorResponse(nil, ErrBadKey), rfcKey, ErrHandshakeRejected},
		{"not http", crlf("SSH-2.0-OpenSSH"), rfcKey, ErrMalformedResponse},
		{"missing upgrade", crlf("HTTP/1.1 101 Switching Protocols", "Sec-WebSocket-Accept: "+acceptKey(rfcKey)), rfcKey, ErrMalformedResponse},
		{"incomplete", ok[:len(ok)-1], rfcKey, ErrNeedMore},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := ParseResponse(tc.resp, tc.key); !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
		})
	}
}

func FuzzParseRequest(f *testing.F) {
	f.Add(chromeRequest)
	f.Add(firefoxRequest)
	f.Add(crlf("GET / HTTP/1.1", "Host: x"))

	f.Fuzz(func(t *testing.T, b []byte) {
		r, n, err := ParseRequest(b)
		if err != nil {
			if n != 0 {
				t.Fatalf("n = %d with error %v", n, err)
			}
			return
		}
		if n > len(b) || n > MaxHeadSize {
			t.Fatalf("n = %d out of bounds", n)
		}
		if !validKey(r.Key) || r.Host == "" || !strings.HasPrefix(r.Path, "/") {
			t.Fatalf("accepted invalid request %+v", r)
		}
	})
}

func BenchmarkParseRequest(b *testing.B) {
	for b.Loop() {
		if _, _, err := ParseRequest(chromeRequest); err != nil {
			b.Fatal(err)
		}
	}
}
