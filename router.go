package origins

import (
	"io"
	"sync"
)

type RouteHandler interface {
	Handle(*Fact) error
	Close(error)
}

type Transaction struct{}

func (tx *Transaction) Handle(f *Fact) error {
	return nil
}

func (tx *Transaction) Close(err error) {
}

type Router struct {
	handler RouteHandler
	routes  map[string]chan *Fact
	signal  chan struct{}
	err     error
	wg      *sync.WaitGroup
}

func (rt *Router) handle(ch chan *Fact) {
	var (
		f    *Fact
		open = true
		err  error
	)

	// Loop while the channel is open.
	for open {
		select {
		case f, open = <-ch:
			if !open {
				break
			}

			if err = rt.handler.Handle(f); err != nil {
				rt.Abort(err)
			}
		case <-rt.signal:
			open = false
			rt.handler.Close(rt.err)
		}
	}

	rt.wg.Done()
}

// Route takes a fact sends it over a channel for the domain. The first time
// a domain is seen, a channel and goroutine are initialized.
func (rt *Router) Route(f *Fact) error {
	if rt.err != nil {
		return rt.err
	}

	var (
		ok bool
		ch chan *Fact
	)

	// Initialize a channel for each incoming domain. Start a goroutine
	// that consumes that channel.
	if ch, ok = rt.routes[f.Domain]; !ok {
		ch = make(chan *Fact)
		rt.routes[f.Domain] = ch
		rt.wg.Add(1)
		go rt.handle(ch)
	}

	ch <- f

	return nil
}

// Abort sets the error on the router and calls done to signal
// the goroutines.
func (rt *Router) Abort(err error) {
	rt.err = err
	rt.Done()
}

// Done signals the goroutines to end their work and waits for
// all of them to complete.
func (rt *Router) Done() {
	close(rt.signal)
	rt.wg.Wait()
}

func NewRouter(handler RouteHandler) *Router {
	return &Router{
		handler: handler,
		routes:  make(map[string]chan *Fact),
		signal:  make(chan struct{}),
		wg:      &sync.WaitGroup{},
	}
}

func RouteReader(reader Reader, handler RouteHandler) error {
	rt := NewRouter(handler)

	defer rt.Done()

	var (
		n   int
		f   *Fact
		err error
	)

	buf := make(Facts, 10)

	for {
		n, err = reader.Read(buf)

		if err != nil && err != io.EOF {
			rt.Abort(err)
			return err
		}

		// Iterate over the buffer up to n.
		for _, f = range buf[:n] {
			rt.Route(f)
		}

		if err == io.EOF {
			return nil
		}
	}
}
