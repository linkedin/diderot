package internal

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
)

var _ handlerLimiter = (*TestRateLimiter)(nil)

func NewTestHandlerLimiter() *TestRateLimiter {
	return &TestRateLimiter{
		cond: sync.NewCond(new(sync.Mutex)),
	}
}

type TestRateLimiter struct {
	cond *sync.Cond
	ch   chan time.Time
	wg   sync.WaitGroup
}

func (l *TestRateLimiter) reserve() (<-chan time.Time, func()) {
	l.cond.L.Lock()
	defer l.cond.L.Unlock()

	if l.ch == nil {
		l.ch = make(chan time.Time)
		l.cond.Signal()
	}
	return l.ch, func() {}
}

func (l *TestRateLimiter) Release() {
	l.cond.L.Lock()
	if l.ch == nil {
		l.cond.Wait()
	}
	ch := l.ch
	l.ch = nil
	l.cond.L.Unlock()

	l.wg.Add(1)
	ch <- time.Now()
	l.wg.Wait()
}

// WaitForReserve waits for another goroutine to call reserve (if one hasn't already)
func (l *TestRateLimiter) WaitForReserve() {
	l.cond.L.Lock()
	defer l.cond.L.Unlock()

	if l.ch == nil {
		l.cond.Wait()
	}
}

func (l *TestRateLimiter) Done() {
	l.wg.Done()
}

func TestHandlerLimiter(t *testing.T) {
	l := (*rateLimiterWrapper)(rate.NewLimiter(10, 1))
	start := time.Now()
	ch1, _ := l.reserve()
	ch2, _ := l.reserve()

	const delta = float64(5 * time.Millisecond)
	require.InDelta(t, 0, (<-ch1).Sub(start), delta)
	require.InDelta(t, 100*time.Millisecond, (<-ch2).Sub(start), delta)
}

var closedReservation = func() chan time.Time {
	ch := make(chan time.Time)
	close(ch)
	return ch
}()

type NoopLimiter struct{}

func (n NoopLimiter) reserve() (reservation <-chan time.Time, cancel func()) {
	return closedReservation, func() {}
}
