package main

import (
	"flag"
	"fmt"
	"net/http"
	"sync"
	"time"
)

type Waiter struct {
	ch chan string
}

type QueueBroker struct {
	mu      sync.Mutex
	queues  map[string][]string
	waiters map[string][]*Waiter
}

func NewQueueBroker() *QueueBroker {
	return &QueueBroker{
		queues:  make(map[string][]string),
		waiters: make(map[string][]*Waiter),
	}
}

func (qb *QueueBroker) Put(queue, message string) {
	qb.mu.Lock()
	defer qb.mu.Unlock()

	if len(qb.waiters[queue]) > 0 {
		waiter := qb.waiters[queue][0]
		qb.waiters[queue] = qb.waiters[queue][1:]
		waiter.ch <- message
		return
	}

	qb.queues[queue] = append(qb.queues[queue], message)
}

func (qb *QueueBroker) Get(queue string, timeout time.Duration) (string, bool) {
	qb.mu.Lock()

	if len(qb.queues[queue]) > 0 {
		msg := qb.queues[queue][0]
		qb.queues[queue] = qb.queues[queue][1:]
		qb.mu.Unlock()
		return msg, true
	}

	if timeout <= 0 {
		qb.mu.Unlock()
		return "", false
	}

	waiter := &Waiter{ch: make(chan string, 1)}
	qb.waiters[queue] = append(qb.waiters[queue], waiter)
	qb.mu.Unlock()

	select {
	case msg := <-waiter.ch:
		return msg, true
	case <-time.After(timeout):
		return "", false
	}
}

func main() {
	port := flag.String("port", "8080", "Port to listen on")
	flag.Parse()

	fmt.Printf("Queue broker starting on port %s\n", *port)
	http.ListenAndServe(":"+*port, nil)
}
