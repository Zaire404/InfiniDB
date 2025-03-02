package util

import (
	"context"
	"sync"
)

var dummyCloserChan <-chan struct{}

type Closer struct {
	waiting sync.WaitGroup

	ctx    context.Context
	cancel context.CancelFunc
}

// NewCloser constructs a new Closer, with an initial count on the WaitGroup
func NewCloser(initial int) *Closer {
	ret := &Closer{}
	ret.ctx, ret.cancel = context.WithCancel(context.Background())
	ret.waiting.Add(initial)
	return ret
}

// AddRunning Add()'s delta to the WaitGroup
func (c *Closer) AddRunning(delta int) {
	c.waiting.Add(delta)
}

// Ctx can be used to get a context, which would automatically get cancelled when Signal is called
func (c *Closer) Ctx() context.Context {
	if c == nil {
		return context.Background()
	}
	return c.ctx
}

// Done calls Done() on the WaitGroup.
func (c *Closer) Done() {
	if c == nil {
		return
	}
	c.waiting.Done()
}

// HasBeenClosed gets signaled when Signal() is called
func (c *Closer) HasBeenClosed() <-chan struct{} {
	if c == nil {
		return dummyCloserChan
	}
	return c.ctx.Done()
}

// Close signals that the Closer is closed
func (c *Closer) Close() {
	if c == nil {
		return
	}
	c.cancel()
}
