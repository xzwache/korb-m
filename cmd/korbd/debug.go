package main

import (
	"bufio"
	"bytes"
	"expvar"
	"fmt"
	"net/http"
	"net/http/pprof" //nolint:gosec // G108: served only on the opt-in debug address
	"os"
	"runtime"
	"runtime/debug"
	"strconv"
	"time"
)

// newDebugServer serves pprof, expvar and /debug/stats. It must never be
// exposed publicly: bind it to localhost or a private network.
func newDebugServer(conns func() int) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	mux.Handle("/debug/vars", expvar.Handler())
	mux.HandleFunc("/debug/stats", statsHandler(conns))
	return &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
}

// statsHandler prints memory numbers as "key value" lines.
// With ?gc=1 it runs a GC and returns freed memory to the OS first, so the
// numbers show live memory rather than garbage waiting to be collected.
func statsHandler(conns func() int) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Has("gc") {
			debug.FreeOSMemory()
		}
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		w.Header().Set("Content-Type", "text/plain")
		_, _ = fmt.Fprintf(w, "conns %d\ngoroutines %d\nheap_inuse %d\nstack_inuse %d\nsys %d\nrss %d\n",
			conns(), runtime.NumGoroutine(), m.HeapInuse, m.StackInuse, m.Sys, rss())
	}
}

// rss returns the resident set size in bytes (Linux only; 0 elsewhere).
func rss() int64 {
	data, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return 0
	}
	sc := bufio.NewScanner(bytes.NewReader(data))
	for sc.Scan() {
		if f := bytes.Fields(sc.Bytes()); len(f) >= 2 && string(f[0]) == "VmRSS:" {
			kb, _ := strconv.ParseInt(string(f[1]), 10, 64)
			return kb * 1024
		}
	}
	return 0
}
