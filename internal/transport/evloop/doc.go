// Package evloop is the event-loop transport: N loops (one per CPU), each
// a single goroutine serving many non-blocking sockets via epoll/kqueue.
//
// Only the owning loop touches a connection. Other goroutines talk to it
// through the loop's inbox (a locked slice) and poller.Wake.
//
// Step-by-step walkthrough: docs/evloop.md.
package evloop
