package sleeper

import (
	"context"
	"time"

	"github.com/flicknow/go-bluesky-bot/pkg/ticker"
)

type Sleeper struct {
	ctx context.Context
}

func (s *Sleeper) Sleep(d time.Duration) {
	t := ticker.NewTicker(d)
	c := t.C
	doneCh := s.ctx.Done()

	select {
	case <-c:
		return
	case <-doneCh:
		t.Stop()
		return
	}
}

func NewSleeper(ctx context.Context) *Sleeper {
	return &Sleeper{ctx: ctx}
}
