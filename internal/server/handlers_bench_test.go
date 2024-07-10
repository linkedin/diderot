package internal

import (
	"fmt"
	"strconv"
	"sync"
	"testing"

	"github.com/linkedin/diderot/ads"
	"github.com/linkedin/diderot/testutils"
	"google.golang.org/protobuf/types/known/anypb"
)

func benchmarkHandlers(tb testing.TB, count, subscriptions int) {
	valueNames := make([]string, subscriptions)
	for i := range valueNames {
		valueNames[i] = strconv.Itoa(i)
	}

	ctx := testutils.Context(tb)

	var finished sync.WaitGroup
	finished.Add(subscriptions)
	const finalVersion = "done"
	h := newHandler(
		ctx,
		NoopLimiter{},
		NoopLimiter{},
		new(customStatsHandler),
		false,
		func(resources map[string]entry) error {
			for _, r := range resources {
				if r.Resource.Version == finalVersion {
					finished.Done()
				}
			}
			return nil
		},
	)

	for _, name := range valueNames {
		go func(name string) {
			resource := testutils.MustMarshal(tb, ads.NewResource(name, "0", new(anypb.Any)))
			for i := 0; i < count-1; i++ {
				h.Notify(name, resource, ads.SubscriptionMetadata{})
			}
			h.Notify(
				name,
				&ads.RawResource{Name: name, Version: finalVersion, Resource: resource.Resource},
				ads.SubscriptionMetadata{},
			)
		}(name)
	}
	finished.Wait()
}

var increments = []int{1, 10, 100, 1000, 10_000}

func BenchmarkHandlers(b *testing.B) {
	for _, subscriptions := range increments {
		b.Run(fmt.Sprintf("%5d subs", subscriptions), func(b *testing.B) {
			benchmarkHandlers(b, b.N, subscriptions)
		})
	}
}

func TestHandlers(t *testing.T) {
	benchmarkHandlers(t, 1000, 1000)
}
