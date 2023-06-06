package clock

import "time"

type Clock interface {
	NowString() string
	NowUnix() int64
	NowUnixMilli() int64
}

type defaultClock struct {
}

func (c *defaultClock) NowString() string {
	return time.Now().UTC().Format(time.RFC3339)
}
func (c *defaultClock) NowUnix() int64 {
	return time.Now().UTC().Unix()
}
func (c *defaultClock) NowUnixMilli() int64 {
	return time.Now().UTC().UnixMilli()
}

func NewClock() *defaultClock {
	return &defaultClock{}
}

type mockClock struct {
	now time.Time
}

func (c *mockClock) NowString() string {
	return c.now.UTC().Format(time.RFC3339)
}
func (c *mockClock) NowUnix() int64 {
	return c.now.UTC().Unix()
}
func (c *mockClock) NowUnixMilli() int64 {
	return c.now.UTC().UnixMilli()
}

func (c *mockClock) SetNow(now int64) time.Time {
	old := c.now
	c.now = time.Unix(now, 0)
	return old
}

func NewMockClock() *mockClock {
	return &mockClock{
		now: time.Now(),
	}
}
