/*
Package broadcast provides pubsub of messages over channels.

A provider has a Broadcaster into which it Submits messages and into
which subscribers Register to pick up those messages.
*/
package broadcast

import "sync"

type broadcaster struct {
	input chan interface{}
	reg   chan chan<- interface{}
	unreg chan chan<- interface{}

	outputs map[chan<- interface{}]bool

	stopCh    chan struct{}
	wg        sync.WaitGroup
	sync.Once // close once
}

// The Broadcaster interface describes the main entry points to
// broadcasters.
type Broadcaster interface {
	// Register a new channel to receive broadcasts
	Register(chan<- interface{})
	// Unregister a channel so that it no longer receives broadcasts.
	Unregister(chan<- interface{})
	// Shut this broadcaster down.
	Close() error
	// Submit a new object to all subscribers
	Submit(interface{})
	// Try Submit a new object to all subscribers return false if input chan is fill
	TrySubmit(interface{}) bool
}

func (b *broadcaster) broadcast(m interface{}) {
	for ch := range b.outputs {
		select {
		case ch <- m:
		case <-b.stopCh:
		}
	}
}

func (b *broadcaster) run() {
	defer b.wg.Done()
	for {
		select {
		case <-b.stopCh:
			return
		case m := <-b.input:
			b.broadcast(m)
		case ch, ok := <-b.reg:
			if ok {
				b.outputs[ch] = true
			} else {
				return
			}
		case ch := <-b.unreg:
			delete(b.outputs, ch)
		}
	}
}

// NewBroadcaster creates a new broadcaster with the given input
// channel buffer length.
func NewBroadcaster(buflen int) Broadcaster {
	b := &broadcaster{
		input:   make(chan interface{}, buflen),
		reg:     make(chan chan<- interface{}),
		unreg:   make(chan chan<- interface{}),
		outputs: make(map[chan<- interface{}]bool),
		stopCh:  make(chan struct{}),
	}

	b.wg.Add(1)
	go b.run()

	return b
}

func (b *broadcaster) Register(newch chan<- interface{}) {
	b.reg <- newch
}

func (b *broadcaster) Unregister(newch chan<- interface{}) {
	b.unreg <- newch
}

func (b *broadcaster) Close() error {
	b.Do(func() {
		close(b.stopCh)
		b.wg.Wait()
		close(b.reg)               // not allowed to register anymore.
		close(b.unreg)             // not allowed to unregister anymore.
		close(b.input)             // not allowed to submit anymore.
		for v := range b.outputs { // close all registered channel.
			close(v)
		}
	})
	return nil
}

// Submit an item to be broadcast to all listeners.
func (b *broadcaster) Submit(m interface{}) {
	if b != nil {
		b.input <- m
	}
}

// TrySubmit attempts to submit an item to be broadcast, returning
// true iff it the item was broadcast, else false.
func (b *broadcaster) TrySubmit(m interface{}) bool {
	if b == nil {
		return false
	}
	select {
	case b.input <- m:
		return true
	default:
		return false
	}
}
