package internal

import (
	"time"

	"golang.org/x/time/rate"
)

// handlerLimiter is an interface used by the handler implementation. It exists for the sole purpose
// of testing and is trivially implemented by rate.Limiter using rateLimiterWrapper. It is not
// exposed in this package's public API.
type handlerLimiter interface {
	// reserve returns a channel that will receive the current time (or be closed) once the rate limit
	// clears. Callers should wait until this occurs before acting. The returned cancel function should
	// be invoked if the caller did not wait for the rate limit to clear, though it is safe to call even
	// if after the rate limit cleared. In other words, it is safe to invoke in a deferred expression.
	reserve() (reservation <-chan time.Time, cancel func())
}

var _ handlerLimiter = (*rateLimiterWrapper)(nil)

// rateLimiterWrapper implements handlerLimiter using a rate.Limiter
type rateLimiterWrapper rate.Limiter

func (w *rateLimiterWrapper) reserve() (reservation <-chan time.Time, cancel func()) {
	r := (*rate.Limiter)(w).Reserve()
	timer := time.NewTimer(r.Delay())
	return timer.C, func() {
		// Stopping the timer cleans up any goroutines or schedules associated with this timer. Invoking this
		// after the timer fires is a noop.
		timer.Stop()
	}
}
