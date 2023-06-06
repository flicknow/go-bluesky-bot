package ticker

import (
	"time"
)

type Ticker struct {
	stop   chan bool
	ticker *time.Ticker
	C      chan time.Time
}

func (t *Ticker) Reset(d time.Duration) {
	t.ticker.Reset(d)
}

func (t *Ticker) Stop() {
	if t.stop != nil {
		t.stop <- true
		close(t.stop)
		t.stop = nil
	}
	if t.ticker != nil {
		t.ticker.Stop()
		t.ticker = nil
	}
}

func (t *Ticker) relay() {
	dstCh := t.C
	srcCh := t.ticker.C
	stopCh := t.stop

	for {
		select {
		case tick := <-srcCh:
			dstCh <- tick
		case <-stopCh:
			close(dstCh)
			return
		}
	}
}

func NewTicker(d time.Duration) *Ticker {
	ticker := &Ticker{}
	if d == 0 {
		ticker.C = make(chan time.Time, 1)
		close(ticker.C)
		return ticker
	}

	ticker.stop = make(chan bool, 1)
	ticker.ticker = time.NewTicker(d)
	ticker.C = make(chan time.Time, 1)
	go ticker.relay()

	return ticker
}
