package internal

import (
	"maps"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/linkedin/diderot/ads"
	"github.com/linkedin/diderot/internal/utils"
	"github.com/linkedin/diderot/testutils"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// TestHandlerDebounce checks the following:
//  1. That the handler does not invoke send as long as the debouncer has not allowed it to.
//  2. That updates that come in while send is being invoked do not get missed.
//  3. That if multiple updates for the same resource come in, only the latest one is respected.
func TestHandlerDebounce(t *testing.T) {
	var released atomic.Bool
	l := NewTestHandlerLimiter()

	var enterSendWg, continueSendWg sync.WaitGroup
	continueSendWg.Add(1)

	actualResources := map[string]entry{}

	h := newHandler(
		testutils.Context(t),
		NoopLimiter{},
		l,
		new(customStatsHandler),
		false,
		func(resources map[string]entry) error {
			require.True(t, released.Swap(false), "send invoked without being released")
			require.NotEmpty(t, resources)
			enterSendWg.Done()
			continueSendWg.Wait()
			defer l.Done()
			for k, e := range resources {
				actualResources[k] = e
			}
			return nil
		},
	)

	// declare the various times upfront and ensure they are all unique, which will allow validating the interactions
	// with the handler
	var (
		fooSubscribedAt   = time.Now()
		fooCreateMetadata = ads.SubscriptionMetadata{
			SubscribedAt: fooSubscribedAt,
			ModifiedAt:   fooSubscribedAt.Add(2 * time.Hour),
			CachedAt:     fooSubscribedAt.Add(3 * time.Hour),
		}
		fooDeleteMetadata = ads.SubscriptionMetadata{
			SubscribedAt: fooSubscribedAt,
			ModifiedAt:   time.Time{},
			CachedAt:     fooSubscribedAt.Add(4 * time.Hour),
		}

		barCreateMetadata = ads.SubscriptionMetadata{
			SubscribedAt: fooSubscribedAt.Add(5 * time.Hour),
			ModifiedAt:   fooSubscribedAt.Add(6 * time.Hour),
			CachedAt:     fooSubscribedAt.Add(7 * time.Hour),
		}
	)

	const foo, bar = "foo", "bar"
	barR := new(ads.RawResource)

	h.Notify(foo, new(ads.RawResource), fooCreateMetadata)
	h.Notify(foo, nil, fooDeleteMetadata)

	enterSendWg.Add(1)
	go func() {
		enterSendWg.Wait()
		h.Notify(bar, barR, barCreateMetadata)
		continueSendWg.Done()
	}()

	released.Store(true)
	l.Release()
	require.Equal(t,
		map[string]entry{
			foo: {
				Resource: nil,
				metadata: fooDeleteMetadata,
			},
		},
		actualResources)
	delete(actualResources, foo)

	enterSendWg.Add(1)
	released.Store(true)
	l.Release()
	require.Equal(
		t,
		map[string]entry{
			bar: {
				Resource: barR,
				metadata: barCreateMetadata,
			},
		},
		actualResources,
	)
}

func TestHandlerBatching(t *testing.T) {
	var released atomic.Bool
	ch := make(chan map[string]entry)
	granular := NewTestHandlerLimiter()
	h := newHandler(
		testutils.Context(t),
		granular,
		NoopLimiter{},
		new(customStatsHandler),
		false,
		func(resources map[string]entry) error {
			// Double check that send isn't invoked before it's expected
			if !released.Load() {
				t.Fatalf("send invoked before release!")
			}
			ch <- maps.Clone(resources)
			return nil
		},
	)
	expectedEntries := make(map[string]entry)
	notify := func() {
		name := strconv.Itoa(len(expectedEntries))
		e := entry{
			Resource: nil,
			metadata: ads.SubscriptionMetadata{},
		}
		h.Notify(name, nil, e.metadata)
		expectedEntries[name] = e
	}

	h.StartNotificationBatch()
	notify()

	for i := 0; i < 100; i++ {
		notify()
	}
	released.Store(true)
	h.EndNotificationBatch()

	require.Equal(t, expectedEntries, <-ch)

	released.Store(false)

	clear(expectedEntries)
	notify()
	granular.WaitForReserve()

	released.Store(true)
	// Check that EndNotificationBatch skips the granular limiter
	h.EndNotificationBatch()

	require.Equal(t, expectedEntries, <-ch)
}

func TestHandlerDoesNothingOnEmptyBatch(t *testing.T) {
	h := newHandler(
		testutils.Context(t),
		// Make both limiters nil, if the handler interacts with them at all the test should fail
		nil,
		nil,
		new(customStatsHandler),
		false,
		func(_ map[string]entry) error {
			require.Fail(t, "notify called")
			return nil
		},
	)
	h.StartNotificationBatch()
	h.EndNotificationBatch()
}

var ignoredMetadata = ads.SubscriptionMetadata{}

func TestPseudoDeltaSotWHandler(t *testing.T) {
	typeUrl := utils.GetTypeURL[*wrapperspb.BoolValue]()
	// This test relies on Bool being a pseudo delta resource type, so fail the test early otherwise
	require.True(t, utils.IsPseudoDeltaSotW(typeUrl))

	l := NewTestHandlerLimiter()
	var lastRes *ads.SotWDiscoveryResponse
	h := newSotWHandler(
		testutils.Context(t),
		NoopLimiter{},
		l,
		new(customStatsHandler),
		typeUrl,
		func(res *ads.SotWDiscoveryResponse) error {
			defer l.Done()
			lastRes = res
			return nil
		},
	)

	const foo, bar, baz = "foo", "bar", "baz"
	fooR := ads.NewResource(foo, "0", wrapperspb.Bool(true))
	barR := ads.NewResource(bar, "0", wrapperspb.Bool(false))
	bazR := ads.NewResource(baz, "0", wrapperspb.Bool(false))
	h.Notify(foo, testutils.MustMarshal(t, fooR), ignoredMetadata)

	l.Release()
	require.Equal(t, typeUrl, lastRes.TypeUrl)
	require.ElementsMatch(t, []*anypb.Any{testutils.MustMarshal(t, fooR).Resource}, lastRes.Resources)

	const wait = 500 * time.Millisecond
	// PseudoDeltaSotW doesn't have a notion of deletions. A deleted resource simply never shows up again unless
	// it's recreated. The next call to Release should therefore block until the handler invokes l.reserve(), which it
	// should _not_ do until a resource is created. This test checks that that's the case by deleting foo then waiting
	// creating bar 500ms before creating bar, then checking how long Release blocked, which should be roughly 500ms.
	h.Notify(foo, nil, ignoredMetadata)
	go func() {
		time.Sleep(wait)
		h.Notify(bar, testutils.MustMarshal(t, barR), ignoredMetadata)
	}()

	start := time.Now()
	l.Release()
	require.WithinDuration(t, time.Now(), start.Add(wait), 10*time.Millisecond)
	require.ElementsMatch(t, []*anypb.Any{testutils.MustMarshal(t, bazR).Resource}, lastRes.Resources)
}
