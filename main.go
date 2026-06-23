package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"
)

type waiter struct {
	ch chan string
}

type broker struct {
	mu      sync.Mutex
	queues  map[string][]string
	waiters map[string][]waiter
}

func newBroker() *broker {
	return &broker{
		queues:  make(map[string][]string),
		waiters: make(map[string][]waiter),
	}
}

func (b *broker) put(queue, msg string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if len(b.waiters[queue]) > 0 {
		w := b.waiters[queue][0]
		b.waiters[queue] = b.waiters[queue][1:]
		w.ch <- msg
		return
	}
	b.queues[queue] = append(b.queues[queue], msg)
}

func (b *broker) get(queue string, timeout time.Duration) (string, bool) {
	b.mu.Lock()

	if len(b.queues[queue]) > 0 {
		msg := b.queues[queue][0]
		b.queues[queue] = b.queues[queue][1:]
		b.mu.Unlock()
		return msg, true
	}

	if timeout <= 0 {
		b.mu.Unlock()
		return "", false
	}

	w := waiter{ch: make(chan string, 1)}
	b.waiters[queue] = append(b.waiters[queue], w)
	b.mu.Unlock()

	select {
	case msg := <-w.ch:
		return msg, true
	case <-time.After(timeout):
		b.mu.Lock()
		for i, ww := range b.waiters[queue] {
			if ww.ch == w.ch {
				b.waiters[queue] = append(b.waiters[queue][:i], b.waiters[queue][i+1:]...)
				break
			}
		}
		b.mu.Unlock()
		select {
		case msg := <-w.ch:
			return msg, true
		default:
			return "", false
		}
	}
}

func main() {
	port := flag.Int("port", 8080, "server port")
	flag.Parse()

	b := newBroker()

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		queue := r.URL.Path[1:]

		switch r.Method {
		case http.MethodPut:
			v := r.URL.Query().Get("v")
			if v == "" {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			b.put(queue, v)

		case http.MethodGet:
			var timeout time.Duration
			if t := r.URL.Query().Get("timeout"); t != "" {
				if sec, err := strconv.Atoi(t); err == nil {
					timeout = time.Duration(sec) * time.Second
				}
			}
			msg, ok := b.get(queue, timeout)
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			fmt.Fprint(w, msg)

		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})

	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", *port), nil))
}
