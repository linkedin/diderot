package internal

import (
	"sync"
	"testing"
	"time"

	"github.com/linkedin/diderot/ads"
	"github.com/linkedin/diderot/testutils"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Benchmarks the actual notification loop. There was a problem where the loop was leaking something
// to heap, causing it to add GC pressure.
func BenchmarkNotificationLoop(b *testing.B) {
	v := NewValue[*timestamppb.Timestamp]("foo", 1)
	for b.Loop() {
		v.notificationLoop()
	}
}

// This benchmarks the worst case scenario, where a goroutine is re-created every time to deliver the
// notification, instead of being reused because the resource update caused the loop to start over.
func BenchmarkValueSetClear(b *testing.B) {
	SetTimeProvider(func() (t time.Time) { return t })
	b.Cleanup(func() {
		SetTimeProvider(time.Now)
	})

	var done sync.WaitGroup

	done.Add(1)
	simpleHandler := testutils.NewSubscriptionHandler(
		func(name string, r *ads.Resource[*timestamppb.Timestamp], _ ads.SubscriptionMetadata) {
			done.Done()
		},
	)
	v := NewValue[*timestamppb.Timestamp]("foo", 1)
	v.Subscribe(simpleHandler)
	done.Wait()

	r := ads.NewResource("foo", "0", timestamppb.Now())
	for b.Loop() {
		done.Add(1)
		v.Set(0, r, time.Time{})
		done.Wait()

		done.Add(1)
		v.Clear(0, time.Time{})
		done.Wait()
	}
}
