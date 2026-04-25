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
	idx := len(qb.waiters[queue])
	qb.waiters[queue] = append(qb.waiters[queue], waiter)
	qb.mu.Unlock()

	select {
	case msg := <-waiter.ch:
		return msg, true
	case <-time.After(timeout):
		qb.mu.Lock()
		for i, w := range qb.waiters[queue] {
			if w == waiter {
				qb.waiters[queue] = append(qb.waiters[queue][:i], qb.waiters[queue][i+1:]...)
				break
			}
		}
		qb.mu.Unlock()
		return "", false
	}
}

func (qb *QueueBroker) handleRequest(w http.ResponseWriter, r *http.Request) {
	queue := r.URL.Path[1:]
	if queue == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if r.Method == http.MethodPut {
		v := r.URL.Query().Get("v")
		if v == "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		qb.Put(queue, v)
		w.WriteHeader(http.StatusOK)
		return
	}

	if r.Method == http.MethodGet {
		timeout := 0
		if t := r.URL.Query().Get("timeout"); t != "" {
			fmt.Sscanf(t, "%d", &timeout)
		}
		msg, found := qb.Get(queue, time.Duration(timeout)*time.Second)
		if !found {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(msg))
		return
	}

	w.WriteHeader(http.StatusMethodNotAllowed)
}

func main() {
	port := flag.String("port", "8080", "Port to listen on")
	flag.Parse()

	broker := NewQueueBroker()
	http.HandleFunc("/", broker.handleRequest)

	fmt.Printf("Queue broker starting on port %s\n", *port)
	http.ListenAndServe(":"+*port, nil)
}
