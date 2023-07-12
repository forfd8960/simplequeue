package simplequeue

import "sync"

type waitGroup struct {
	wg sync.WaitGroup
}

// Wrap ...
func (g *waitGroup) Wrap(fn func()) {
	g.wg.Add(1)
	go func() {
		fn()
		g.wg.Done()
	}()
}
