package main

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/xzwache/korb-m/internal/ws"
	"github.com/xzwache/korb-m/internal/wstest"
)

func TestBroadcastEndToEnd(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	addrc := make(chan net.Addr, 1)
	done := make(chan error, 1)
	go func() {
		args := []string{"-addr", "127.0.0.1:0", "-mode", "broadcast"}
		done <- run(ctx, args, io.Discard, io.Discard, func(a net.Addr) { addrc <- a })
	}()
	addr := (<-addrc).String()

	alice := wstest.Dial(t, addr)
	bob := wstest.Dial(t, addr)
	alice.Sync()
	bob.Sync()

	alice.Send(ws.OpText, "hi all")
	alice.Expect(ws.OpText, "hi all")
	bob.Expect(ws.OpText, "hi all")

	cancel() // like Ctrl+C
	for _, c := range []*wstest.Client{alice, bob} {
		c.ExpectClose(ws.CloseGoingAway)
		c.Send(ws.OpClose, string(ws.AppendClose(nil, ws.CloseGoingAway, "")))
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run: %v", err)
		}
	case <-time.After(wstest.Timeout):
		t.Fatal("run did not return after cancel")
	}
}

func TestOriginChecker(t *testing.T) {
	check := originChecker("http://localhost:3000, null")
	for origin, want := range map[string]bool{
		"http://localhost:3000": true,
		"null":                  true, // pages opened from file://
		"":                      true, // non-browser client
		"https://evil.example":  false,
	} {
		if got := check(ws.Request{Origin: origin}); got != want {
			t.Errorf("origin %q: got %v, want %v", origin, got, want)
		}
	}
	if originChecker("") != nil {
		t.Error("empty list should allow all (nil checker)")
	}
}

func TestBadFlags(t *testing.T) {
	if err := run(context.Background(), []string{"-mode", "nope"}, io.Discard, io.Discard, nil); err == nil {
		t.Error("expected error for unknown mode")
	}
}
