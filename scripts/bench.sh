#!/usr/bin/env bash
# Measures memory per idle connection at several connection counts.
# Meant to run on Linux (see `make bench-linux`): uses /proc for RSS and
# kernel socket memory, and 127.0.0.x source IPs to avoid running out of
# ephemeral ports.
set -euo pipefail

STEPS="${STEPS:-10000 50000 100000}"
SOURCES="${SOURCES:-127.0.0.2,127.0.0.3,127.0.0.4,127.0.0.5}"
ADDR=127.0.0.1:8080
DEBUG=127.0.0.1:6060
BIN="$(mktemp -d)"

go build -o "$BIN/" ./cmd/korbd ./cmd/loadgen

stat() { curl -s "http://$DEBUG/debug/stats?gc=1" | awk -v k="$1" '$1 == k { print $2 }'; }
sock_pages() { awk '$1 == "TCP:" { for (i = 2; i < NF; i++) if ($i == "mem") print $(i+1) }' /proc/net/sockstat; }
kb() { echo $(( $1 / 1024 )); }

echo "| conns | RSS | heap | stacks | goroutines | kernel TCP mem | RSS / conn | heap / conn | stacks / conn | kernel / conn |"
echo "|---|---|---|---|---|---|---|---|---|---|"

for N in $STEPS; do
  "$BIN/korbd" -addr "$ADDR" -mode echo -debug-addr "$DEBUG" -log-level warn &
  KORBD=$!
  until curl -sf "http://$DEBUG/debug/stats" >/dev/null; do sleep 0.2; done

  rss0=$(stat rss); heap0=$(stat heap_inuse); stack0=$(stat stack_inuse); sock0=$(sock_pages)

  "$BIN/loadgen" -addr "$ADDR" -conns "$N" -rate 10000 -sources "$SOURCES" -report 5s >&2 &
  LOADGEN=$!

  for _ in $(seq 1 300); do
    [ "$(stat conns)" -ge "$N" ] && break
    sleep 1
  done
  sleep 3

  conns=$(stat conns); rss=$(stat rss); heap=$(stat heap_inuse); stack=$(stat stack_inuse)
  gor=$(stat goroutines); sock=$(sock_pages)
  # kernel TCP memory counts both ends (server + loadgen sockets), so halve it
  sock_bytes=$(( (sock - sock0) * 4096 / 2 ))

  echo "| $conns | $(kb "$rss") KB | $(kb "$heap") KB | $(kb "$stack") KB | $gor | $(kb "$sock_bytes") KB" \
       "| $(( (rss - rss0) / conns )) B | $(( (heap - heap0) / conns )) B | $(( (stack - stack0) / conns )) B" \
       "| $(( sock_bytes / conns )) B |"

  kill -INT "$LOADGEN" "$KORBD"
  wait "$LOADGEN" "$KORBD" 2>/dev/null || true
done
