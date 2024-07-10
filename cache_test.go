package diderot_test

import (
	"fmt"
	"maps"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/linkedin/diderot"
	"github.com/linkedin/diderot/ads"
	internal "github.com/linkedin/diderot/internal/cache"
	"github.com/linkedin/diderot/internal/utils"
	"github.com/linkedin/diderot/testutils"
	"github.com/stretchr/testify/require"
	. "google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func newCache() diderot.Cache[*Timestamp] {
	return diderot.NewCache[*Timestamp]()
}

func newResource(name, version string) *ads.Resource[*Timestamp] {
	return ads.NewResource(name, version, Now())
}

const (
	name1 = "r1"
	name2 = "r2"
	name3 = "r3"
)

var noTime time.Time

func TestCacheCrud(t *testing.T) {
	c := newCache()

	require.Nil(t, c.Get(name1))

	version := "1"

	r1 := c.Set(name1, version, Now(), noTime)
	require.Same(t, r1, c.Get(name1))

	c.Clear(name1, noTime)
	require.Nil(t, c.Get(name1))

	c.SetResource(r1, noTime)

	checkEntries := func(expected ...string) {
		var entries []string
		c.EntryNames(func(name string) bool {
			entries = append(entries, name)
			return true
		})
		sort.Strings(entries)
		sort.Strings(expected)
		require.Equal(t, expected, entries)
	}

	checkEntries(name1)
	c.Set(name2, version, Now(), noTime)
	checkEntries(name1, name2)
}

func TestCacheSubscribe(t *testing.T) {
	c := newCache()

	updates := make(testutils.ChanSubscriptionHandler[*Timestamp], 1)
	wildcard := make(testutils.ChanSubscriptionHandler[*Timestamp], 1)
	c.Subscribe(ads.WildcardSubscription, wildcard)

	// Subscribe should always feed the initial value of the resource to the stream, even if it does not currently exist
	c.Subscribe(name1, updates)
	updates.WaitForDelete(t, name1)
	select {
	case <-updates:
		t.Fatalf("Received unexpected update for %q", name1)
	case <-time.After(10 * time.Millisecond):
	}

	// Test that explicitly re-subscribing to the cache even if the entry doesn't exist delivers the
	// notification again.
	c.Subscribe(name1, updates)
	updates.WaitForDelete(t, name1)

	// conversely, creating the nil entry should _not_ update the wildcard subscriber until the entry is actually
	// updated
	select {
	case <-wildcard:
		t.Fatalf("Received update for %q even though entry does not actually exist", name1)
	case <-time.After(10 * time.Millisecond):
	}

	r2 := c.Set(name2, "2", Now(), noTime)
	c.Subscribe(name2, updates)
	updates.WaitForUpdate(t, r2)
	wildcard.WaitForUpdate(t, r2)

	// Test that explicitly re-subscribing to the cache delivers the resource again.
	c.Subscribe(name2, updates)
	updates.WaitForUpdate(t, r2)

	r1 := c.Set(name1, "1", Now(), noTime)
	updates.WaitForUpdate(t, r1)
	wildcard.WaitForUpdate(t, r1)

	c.Clear(name1, noTime)
	updates.WaitForDelete(t, name1)
	wildcard.WaitForDelete(t, name1)

	// Test that the cache ignores double deletes by re-clearing r1, then updating it. We should only see the update
	c.Clear(name1, noTime)

	c.Unsubscribe(name1, updates)

	c.Clear(name2, noTime)
	// Because the cache updates are happening from this thread and all the methods return only after having pushed the
	// updates to all the subscribers, we know that if we receive the update for name2 on the stream then the cache
	// correctly unsubscribed the stream from r1.
	updates.WaitForDelete(t, name2)
	wildcard.WaitForDelete(t, name2)
	c.Unsubscribe(name1, updates)
	c.Unsubscribe(name2, updates)

	c.EntryNames(func(name string) bool {
		t.Fatal("Cache should be empty!")
		return true
	})

	r1 = c.Set(name1, "3", Now(), noTime)
	wildcard.WaitForUpdate(t, r1)
	r2 = c.Set(name2, "4", Now(), noTime)
	wildcard.WaitForUpdate(t, r2)

	// Ensure a double wildcard subscription notifies of all existing values (note that this needs to run a separate
	// routine since it'll push 2 values onto the channel, which only has a capacity of 1)
	go c.Subscribe(ads.WildcardSubscription, wildcard)
	for i := 0; i < 2; i++ {
		r := <-wildcard
		if r.Name == name1 {
			require.Same(t, r1, r.Resource)
		} else {
			require.Same(t, r2, r.Resource)
		}
	}

	// Explicit subscription to r1 for wildcard
	c.Subscribe(name1, wildcard)
	// An explicit subscription will always yield the current value, even if the handler has already seen it
	wildcard.WaitForUpdate(t, r1)

	// Remove remaining wildcard subscriptions
	c.Unsubscribe(ads.WildcardSubscription, wildcard)
	r1 = c.Set(name1, "5", Now(), noTime)
	// Check that the old subscription was preserved
	wildcard.WaitForUpdate(t, r1)

	// but that the handler is not automatically added to new entries
	c.IsSubscribedTo(name3, wildcard)
	r3 := c.Set(name3, "6", Now(), noTime)
	select {
	case <-wildcard:
		t.Fatalf("Received update for %v even though there was no subscription for it", r3)
	case <-time.After(10 * time.Millisecond):
	}
	// Nor old entries
	c.IsSubscribedTo(name2, wildcard)
	c.Set(name2, "7", Now(), noTime)
	select {
	case <-wildcard:
		t.Fatalf("Received update for %q even though there was no subscription for it", name2)
	case <-time.After(10 * time.Millisecond):
	}
}

func TestWildcardSubscriptionOnNonEmptyCache(t *testing.T) {
	testutils.WithTimeout(t, "real entries", 5*time.Second, func(t *testing.T) {
		c := newCache()

		r1 := c.Set(name1, "1", Now(), noTime)
		r2 := c.Set(name2, "2", Now(), noTime)

		wildcard := make(testutils.ChanSubscriptionHandler[*Timestamp])
		go c.Subscribe(ads.WildcardSubscription, wildcard)

		remaining := utils.NewSet(name1, name2)
		for r := range wildcard {
			require.True(t, remaining.Remove(r.Name))
			switch r.Name {
			case name1:
				require.Same(t, r1, r.Resource)
			case name2:
				require.Same(t, r2, r.Resource)
			}
			if len(remaining) == 0 {
				break
			}
		}
	})
	// This tests that entries that were created by a call to Subscribe but never set (aka "fake" entries) are never
	// shown to a wildcard subscriber
	testutils.WithTimeout(t, "fake entries", 5*time.Second, func(t *testing.T) {
		c := newCache()

		h := testutils.NewSubscriptionHandler[*Timestamp](
			func(name string, r *ads.Resource[*Timestamp], _ ads.SubscriptionMetadata) {
				require.Nil(t, r)
			},
		)
		c.Subscribe(name1, h)
		c.Subscribe(name2, h)

		wildcard := testutils.NewSubscriptionHandler[*Timestamp](
			func(string, *ads.Resource[*Timestamp], ads.SubscriptionMetadata) {
				require.Fail(t, "Wildcard handler should not be called")
			},
		)
		for i := 0; i < 10; i++ {
			c.Subscribe(ads.WildcardSubscription, wildcard)
		}
	})
	// Tests that once an entry has been deleted, it is not shown to a wildcard subscriber, even if it wildcard
	// subscribes again.
	testutils.WithTimeout(t, "deleted entries", 5*time.Second, func(t *testing.T) {
		c := newCache()

		notified := make(chan struct{}, 1)
		wildcard := testutils.NewSubscriptionHandler[*Timestamp](
			func(_ string, r *ads.Resource[*Timestamp], _ ads.SubscriptionMetadata) {
				if r != nil {
					notified <- struct{}{}
				} else {
					close(notified)
				}
			},
		)

		c.Subscribe(ads.WildcardSubscription, wildcard)

		c.Set(name1, "0", Now(), noTime)
		<-notified
		c.Clear(name1, noTime)
		<-notified
		for i := 0; i < 10; i++ {
			c.Subscribe(ads.WildcardSubscription, wildcard)
			<-notified
		}
	})
}

func TestCachePriority(t *testing.T) {
	c := diderot.NewPrioritizedCache[*Timestamp](4)

	handlers := make([]testutils.ChanSubscriptionHandler[*Timestamp], len(c))
	for i := range handlers {
		handlers[i] = make(testutils.ChanSubscriptionHandler[*Timestamp], 1)
		c[i].Subscribe(name1, handlers[i])
		notification := handlers[i].WaitForDelete(t, name1)
		require.Equal(t, len(c)-1, notification.Metadata.Priority)
	}

	resources := make([]*ads.Resource[*Timestamp], 5)
	for i := range resources {
		resources[i] = newResource(name1, "0")
	}

	for i := 2; i >= 0; i-- {
		c[i].SetResource(resources[i], noTime)
		require.Same(t, resources[i], c[i].Get(name1))
		for _, h := range handlers {
			notification := h.WaitForUpdate(t, resources[i])
			require.Equal(t, i, notification.Metadata.Priority)
		}
	}

	// This should be ignored
	c[2].SetResource(resources[3], noTime)
	for _, c := range c {
		require.Same(t, resources[0], c.Get(name1))
	}

	// This should also be ignored
	c[2].Clear(name1, noTime)
	for _, c := range c {
		require.Same(t, resources[0], c.Get(name1))
	}

	// This should bring us back to resources[1]
	c[0].Clear(name1, noTime)
	for i, h := range handlers {
		require.Same(t, resources[1], c[i].Get(name1))
		notification := h.WaitForUpdate(t, resources[1])
		require.Equal(t, 1, notification.Metadata.Priority)
	}

	// This should fully delete the resource
	c[1].Clear(name1, noTime)
	for i, h := range handlers {
		require.Nil(t, c[i].Get(name1))
		h.WaitForDelete(t, name1)
	}
}

func TestNotifyMetadata(t *testing.T) {
	c := diderot.NewCache[*Timestamp]()
	h := make(testutils.ChanSubscriptionHandler[*Timestamp], 1)

	subscribedAtStart := time.Now()
	c.Subscribe(name1, h)
	subscribedAtEnd := time.Now()

	notification := h.WaitForDelete(t, name1)
	// Since go provides no convenient way to mock the system clock, the only way to test the various timestamps
	// is to check that they occur within the start and end of the invocation of the function being tested. This is
	// preferable to require.WithinDuration, which is inherently flaky as it requires assuming that whatever is being
	// measured happens within a predetermined duration (e.g. event "b" happened at most 1ms after "a").
	require.WithinRange(t, notification.Metadata.SubscribedAt, subscribedAtStart, subscribedAtEnd)
	require.Equal(t, noTime, notification.Metadata.ModifiedAt)
	require.Equal(t, noTime, notification.Metadata.CachedAt)

	for i := 0; i < 10; i++ {
		modifiedAt := time.Now()
		cachedAtStart := time.Now()
		r := c.Set(name1, "0", Now(), modifiedAt)
		cachedAtEnd := time.Now()

		notification = h.WaitForUpdate(t, r)
		require.Equal(t, notification.Metadata.ModifiedAt, modifiedAt)
		require.WithinRange(t, notification.Metadata.CachedAt, cachedAtStart, cachedAtEnd)
	}
}

// TestWatchableValueUpdateCancel tests a very specific edge case where an entry is updated during the subscription
// loop. The loop should abort and not call the remaining subscribers. It should instead restart and run through each
// subscriber with the updated value.
func TestWatchableValueUpdateCancel(t *testing.T) {
	c := newCache()

	r1 := newResource(name1, "0")

	var r1Wg, r2Wg sync.WaitGroup
	r1Wg.Add(1)
	r2Wg.Add(2)
	notify := func(name string, r *ads.Resource[*Timestamp], _ ads.SubscriptionMetadata) {
		// r is nil during the initial invocation of notify since the resource does not yet exist.
		if r == nil {
			return
		}

		if r == r1 {
			c.Set(name1, "1", Now(), noTime)
			// if notify is invoked with r1 more than once, this will panic
			r1Wg.Done()
		} else {
			r2Wg.Done()
		}
	}

	c.Subscribe(name1, testutils.NewSubscriptionHandler(notify))
	c.Subscribe(name1, testutils.NewSubscriptionHandler(notify))

	c.SetResource(r1, noTime)

	r1Wg.Wait()
	r2Wg.Wait()
}

// TestCacheEntryDeletion specifically checks for entry deletion. There is no explicit way to check whether an entry is
// still in the cache, but it can be implicitly checked by calling EntryNames
func TestCacheEntryDeletion(t *testing.T) {
	h := make(testutils.ChanSubscriptionHandler[*Timestamp], 1)

	inCache := func(c diderot.Cache[*Timestamp]) bool {
		inCache := false
		c.EntryNames(func(name string) bool {
			if name == name1 {
				inCache = true
				return false
			}
			return true
		})
		return inCache
	}
	checkEntryExists := func(t *testing.T, c diderot.Cache[*Timestamp]) {
		require.Truef(t, inCache(c), "%q not in cache!", name1)
	}
	checkEntryDoesNotExist := func(t *testing.T, c diderot.Cache[*Timestamp]) {
		require.Falsef(t, inCache(c), "%q in cache!", name1)
	}

	setup := func(t *testing.T) diderot.Cache[*Timestamp] {
		c := newCache()

		c.Subscribe(name1, h)

		h.WaitForDelete(t, name1)

		checkEntryExists(t, c)

		return c
	}

	// In this test, the value is nil. Unsubscribing from it should delete it
	t.Run("delete on last unsub", func(t *testing.T) {
		c := setup(t)
		wildcard := testutils.NewSubscriptionHandler[*Timestamp](
			func(name string, r *ads.Resource[*Timestamp], _ ads.SubscriptionMetadata) {
				t.Fatalf("wildcard handler not expected to ever be called (name=%q, r=%v)", name, r)
			},
		)
		c.Subscribe(ads.WildcardSubscription, wildcard)
		c.Unsubscribe(name1, h)
		// At this point, even though there is a wildcard subscription to the entry, it can still be safely deleted
		// because the wildcard entry will be added back automatically. The behavior of the wildcard subscriptions is
		// tested elsewhere, so it's not necessary to retest it here.
		checkEntryDoesNotExist(t, c)
	})

	// In this test, the value is set, it should not get automatically deleted until cleared
	t.Run("clear deletes entry", func(t *testing.T) {
		c := setup(t)
		// Explicitly set the entry
		c.Set(name1, "0", Now(), noTime)
		// Remove the subscription from it
		c.Unsubscribe(name1, h)
		checkEntryExists(t, c)
		c.Clear(name1, noTime)
		checkEntryDoesNotExist(t, c)
	})
}

func TestCacheCollections(t *testing.T) {
	c := diderot.NewCache[*Timestamp]()

	const prefix = "xdstp:///google.protobuf.Timestamp/"

	h := make(testutils.ChanSubscriptionHandler[*Timestamp], 1)

	c.Subscribe(prefix+"a/*", h)
	h.WaitForDelete(t, prefix+"a/*")

	c.Subscribe(prefix+"a/foo", h)
	h.WaitForDelete(t, prefix+"a/foo")

	var updates []testutils.ExpectedNotification[*Timestamp]
	var deletes []testutils.ExpectedNotification[*Timestamp]
	for i := 0; i < 5; i++ {
		name, v := prefix+"a/"+strconv.Itoa(i), strconv.Itoa(i)
		updates = append(updates, testutils.ExpectUpdate(c.Set(name, v, Now(), noTime)))
		deletes = append(deletes, testutils.ExpectDelete[*Timestamp](name))
	}

	h.WaitForNotifications(t, updates...)

	for _, d := range deletes {
		c.Clear(d.Name, noTime)
	}

	h.WaitForNotifications(t, deletes...)

	h.WaitForDelete(t, prefix+"a/*")
}

// TestCache raw validates that the various *Raw methods on the cache work as expected. Namely, raw
// subscribers are wrapped in a wrappedHandler, which is then used as the key in the subscriber map.
// It's important to check that subscribing then unsubscribing works as expected.
func TestCacheRaw(t *testing.T) {
	c := diderot.RawCache(newCache())
	r := newResource(name1, "42")

	ch := make(chan *ads.RawResource, 1)
	h := testutils.NewRawSubscriptionHandler(
		t,
		func(name string, raw *ads.RawResource, metadata ads.SubscriptionMetadata) {
			if raw != nil {
				testutils.ProtoEquals(t, testutils.MustMarshal(t, r), raw)
			}
			ch <- raw
		},
	)

	diderot.Subscribe(c, name1, h)
	<-ch
	require.NoError(t, c.SetRaw(testutils.MustMarshal(t, r), noTime))
	raw, err := c.GetRaw(r.Name)
	require.NoError(t, err)
	require.Same(t, testutils.MustMarshal(t, r), raw)
	<-ch
	c.Clear(name1, noTime)
	<-ch
	diderot.Unsubscribe(c, name1, h)
	select {
	case raw := <-ch:
		require.Fail(t, "Received unexpected update after unsubscription", raw)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestSetThenSubscribe(t *testing.T) {
	var called sync.WaitGroup
	h := testutils.NewSubscriptionHandler(func(string, *ads.Resource[*wrapperspb.BoolValue], ads.SubscriptionMetadata) {
		called.Done()
	})

	c := diderot.NewCache[*wrapperspb.BoolValue]()
	called.Add(1)
	c.Set("foo", "0", wrapperspb.Bool(true), noTime)
	c.Subscribe("foo", h)
	time.Sleep(10 * time.Millisecond)
	called.Wait()
}

func TestExplicitAndImplicitSubscription(t *testing.T) {
	c := diderot.NewCache[*wrapperspb.Int64Value]()

	names := utils.NewSet[string]()
	for i := 0; i < 100; i++ {
		names.Add(strconv.Itoa(i))
	}

	var lock sync.Mutex
	initialSubscriptionNotificationsRemaining := maps.Clone(names)
	allInitialNotificationsReceived := make(chan struct{})
	resourceCreationNotificationsRemaining := maps.Clone(names)
	allCreationNotificationsReceived := make(chan struct{})

	h := testutils.NewSubscriptionHandler(
		func(name string, r *ads.Resource[*wrapperspb.Int64Value], metadata ads.SubscriptionMetadata) {
			lock.Lock()
			defer lock.Unlock()

			if r == nil {
				initialSubscriptionNotificationsRemaining.Remove(name)
				if len(initialSubscriptionNotificationsRemaining) == 0 {
					close(allInitialNotificationsReceived)
				}
			} else {
				if !resourceCreationNotificationsRemaining.Remove(name) {
					t.Fatalf("Received double notifications for %q", name)
				}
				if len(resourceCreationNotificationsRemaining) == 0 {
					close(allCreationNotificationsReceived)
				}
			}
		},
	)

	// implicit subscription
	c.Subscribe(ads.WildcardSubscription, h)

	for name := range names {
		// Explicit subscription
		c.Subscribe(name, h)
	}

	<-allInitialNotificationsReceived

	for name := range names {
		c.Set(name, "0", wrapperspb.Int64(0), noTime)
	}

	<-allCreationNotificationsReceived
}

func TestSubscribeToGlobCollection(t *testing.T) {
	c := diderot.NewCache[*wrapperspb.Int64Value]()
	h := make(testutils.ChanSubscriptionHandler[*wrapperspb.Int64Value], 2)
	const (
		fullGCNamePrefix = "xdstp:///google.protobuf.Int64Value/"
		foo              = "foo/"
		collectionName   = fullGCNamePrefix + foo + ads.WildcardSubscription
	)
	c.Subscribe(collectionName, h)
	c.IsSubscribedTo(collectionName, h)
	h.WaitForDelete(t, collectionName)

	name := func(i int64) string { return fullGCNamePrefix + foo + fmt.Sprint(i) }
	for i := int64(0); i < 10; i++ {
		resource := c.Set(name(i), "0", wrapperspb.Int64(i), noTime)
		h.WaitForUpdate(t, resource)
	}

	for i := int64(0); i < 10; i++ {
		c.Clear(name(i), noTime)
		h.WaitForDelete(t, name(i))
	}

	h.WaitForDelete(t, collectionName)

	c.Unsubscribe(collectionName, h)
	c.Set(name(10), "0", wrapperspb.Int64(10), noTime)
	select {
	case r := <-h:
		t.Fatalf("Received update after unsubscription: %+v", r)
	case <-time.After(100 * time.Millisecond):
	}
}

var _ ads.SubscriptionHandler[*wrapperspb.Int64Value] = (*fakeHandler)(nil)

type fakeHandler struct {
	lock           sync.Mutex
	resources      map[string]*ads.Resource[*wrapperspb.Int64Value]
	complete       *sync.WaitGroup
	allInitialized *sync.WaitGroup
}

const (
	initializationVersion = "-1"
	endResourceVersion    = "0"
)

func (f *fakeHandler) Notify(name string, r *ads.Resource[*wrapperspb.Int64Value], _ ads.SubscriptionMetadata) {
	// Locking and writing to a map is a fairly representative Handler action and should provide acurate benchmarking
	// results
	f.lock.Lock()
	defer f.lock.Unlock()

	if r != nil && r.Version == initializationVersion {
		f.allInitialized.Done()
		return
	}

	f.resources[name] = r
	if r != nil && r.Version == endResourceVersion {
		f.complete.Done()
	}
}

type TB[T testing.TB] interface {
	testing.TB
	Run(name string, tb func(T)) bool
}

// this method helps benchmark the cache implementation by doing the following:
//   - It will spin up as many SubscriptionHandler instances as specified by the subscribers parameter to simulate that
//     many clients being connected to the server.
//   - It will then create as many entries as specified by the entries parameter, and subscribe each handler to each
//     entry, simulating worst case scenario load.
//   - Once all the handlers have subscribed to each entry, it spins up a goroutine for each entry and updates each
//     entry as many times as specified in the updates parameter. This simulates heavy concurrent updates to the cache.
//   - The test will end once each handler has counted down a sync.WaitGroup initialized with the expected number of
//     updates.
func benchmarkCacheThroughput[T TB[T]](tb T, subscribers, entries int) {
	cache := diderot.NewCache[*wrapperspb.Int64Value]()

	resources := make([]*ads.Resource[*wrapperspb.Int64Value], entries)
	for i := range resources {
		name := strconv.Itoa(i)
		cache.Set(name, initializationVersion, new(wrapperspb.Int64Value), noTime)
		resources[i] = ads.NewResource(name, "1", new(wrapperspb.Int64Value))
	}

	complete := new(sync.WaitGroup)

	allInitialized := new(sync.WaitGroup)
	allInitialized.Add(subscribers * entries)
	for i := 0; i < subscribers; i++ {
		h := &fakeHandler{
			resources:      make(map[string]*ads.Resource[*wrapperspb.Int64Value], entries),
			complete:       complete,
			allInitialized: allInitialized,
		}
		// Use explicit subscriptions, otherwise the backing cache entry may get deleted, causing significant additional
		// allocations.
		for _, r := range resources {
			cache.Subscribe(r.Name, h)
		}
	}
	allInitialized.Wait()

	tb.Run(fmt.Sprintf("%5d subscribers/%5d entries", subscribers, entries), func(tb T) {
		complete.Add(subscribers * entries)

		var n int
		if b, ok := any(tb).(*testing.B); ok {
			n = b.N
		} else {
			n = 100
		}

		for _, r := range resources {
			go func(r *ads.Resource[*wrapperspb.Int64Value]) {
				for i := 0; i < n/2; i++ {
					cache.SetResource(r, noTime)
					cache.Clear(r.Name, noTime)
				}
				// This final Set means a different *ads.RawResource is used as the final resource for each b.Run
				// invocation. This is critical because the cache will ignore back-to-back updates with the same
				// *ads.RawResource, meaning it may not notify the subscribers of the final version if it ignored
				// intermediate updates.
				cache.Set(r.Name, endResourceVersion, new(wrapperspb.Int64Value), noTime)
			}(r)
		}

		complete.Wait()
	})
}

// Benchmark results as of 2023-04-7:
//
//	make -B bin/BenchmarkCacheThroughput.profile BENCH_PKG=ads/cache
//	Not opening profiles since OPEN_PROFILES is not set
//	make[1]: Entering directory `/home/pchesnai/code/linkedin/indis-registry-observer'
//	go test \
//		-o bin/BenchmarkCacheThroughput.profile \
//		-count 1 \
//		-benchmem \
//		-bench="^BenchmarkCacheThroughput$" \
//		-cpuprofile profiles/BenchmarkCacheThroughput.cpu \
//		-memprofile profiles/BenchmarkCacheThroughput.mem \
//		-blockprofile profiles/BenchmarkCacheThroughput.block \
//		-benchtime 10s \
//		-timeout 20m \
//		-run "^$" \
//		./indis-registry-observer/src/ads/cache
//	goos: linux
//	goarch: amd64
//	cpu: Intel(R) Xeon(R) CPU E5-2673 v4 @ 2.30GHz
//	BenchmarkCacheThroughput/____1_subscribers/____1_entries-8         	195447372	        61.30 ns/op	       0 B/op	       0 allocs/op
//	BenchmarkCacheThroughput/____1_subscribers/___10_entries-8         	63061165	       182.8 ns/op	       0 B/op	       0 allocs/op
//	BenchmarkCacheThroughput/____1_subscribers/__100_entries-8         	 6176012	      1893 ns/op	       0 B/op	       0 allocs/op
//	BenchmarkCacheThroughput/____1_subscribers/_1000_entries-8         	  629365	     18486 ns/op	       2 B/op	       0 allocs/op
//	BenchmarkCacheThroughput/___10_subscribers/____1_entries-8         	190472704	        61.84 ns/op	       0 B/op	       0 allocs/op
//	BenchmarkCacheThroughput/___10_subscribers/___10_entries-8         	61527584	       191.1 ns/op	       0 B/op	       0 allocs/op
//	BenchmarkCacheThroughput/___10_subscribers/__100_entries-8         	 6546378	      1864 ns/op	       0 B/op	       0 allocs/op
//	BenchmarkCacheThroughput/___10_subscribers/_1000_entries-8         	  620842	     18315 ns/op	       4 B/op	       0 allocs/op
//	BenchmarkCacheThroughput/__100_subscribers/____1_entries-8         	194074869	        62.25 ns/op	       0 B/op	       0 allocs/op
//	BenchmarkCacheThroughput/__100_subscribers/___10_entries-8         	62036193	       181.7 ns/op	       0 B/op	       0 allocs/op
//	BenchmarkCacheThroughput/__100_subscribers/__100_entries-8         	 6514137	      1955 ns/op	       0 B/op	       0 allocs/op
//	BenchmarkCacheThroughput/__100_subscribers/_1000_entries-8         	  567054	     18684 ns/op	      34 B/op	       0 allocs/op
//	BenchmarkCacheThroughput/__100_subscribers/10000_entries-8         	   56433	    202390 ns/op	    3333 B/op	       5 allocs/op
//	BenchmarkCacheThroughput/_1000_subscribers/____1_entries-8         	195721614	        61.06 ns/op	       0 B/op	       0 allocs/op
//	BenchmarkCacheThroughput/_1000_subscribers/___10_entries-8         	61467828	       193.6 ns/op	       0 B/op	       0 allocs/op
//	BenchmarkCacheThroughput/_1000_subscribers/__100_entries-8         	 6236072	      1844 ns/op	       2 B/op	       0 allocs/op
//	BenchmarkCacheThroughput/_1000_subscribers/_1000_entries-8         	  551036	     20126 ns/op	     375 B/op	       0 allocs/op
//	BenchmarkCacheThroughput/10000_subscribers/__100_entries-8         	 5894846	      1994 ns/op	      23 B/op	       0 allocs/op
//	PASS
//	ok  	_/home/pchesnai/code/linkedin/indis-registry-observer/indis-registry-observer/src/ads/cache	297.029s
//	make[1]: Leaving directory `/home/pchesnai/code/linkedin/indis-registry-observer'
//		Command being timed: "make profile_cache"
//		User time (seconds): 1663.37
//		System time (seconds): 5.77
//		Percent of CPU this job got: 560%
//		Elapsed (wall clock) time (h:mm:ss or m:ss): 4:57.57
//		Average shared text size (kbytes): 0
//		Average unshared data size (kbytes): 0
//		Average stack size (kbytes): 0
//		Average total size (kbytes): 0
//		Maximum resident set size (kbytes): 278716
//		Average resident set size (kbytes): 0
//		Major (requiring I/O) page faults: 0
//		Minor (reclaiming a frame) page faults: 379417
//		Voluntary context switches: 239299
//		Involuntary context switches: 197562
//		Swaps: 0
//		File system inputs: 72
//		File system outputs: 312
//		Socket messages sent: 0
//		Socket messages received: 0
//		Signals delivered: 0
//		Page size (bytes): 4096
//		Exit status: 0
//
// The final row likely represents the most common use case, where 10k clients are connected to a single machine and
// have subscribed to 100 resources each. In this setup, this cache can distribute updates to each listener in 2.4ms.
func BenchmarkCacheThroughput(b *testing.B) {
	DisableTime(b)
	increments := []int{1, 10, 100, 1000}
	for _, subscribers := range increments {
		for _, entries := range increments {
			benchmarkCacheThroughput(b, subscribers, entries)
			if subscribers == 100 && entries == 1000 {
				benchmarkCacheThroughput(b, 100, 10_000)
			}
		}
	}
	benchmarkCacheThroughput(b, 10_000, 100)
}

func TestCacheThroughput(t *testing.T) {
	benchmarkCacheThroughput(t, 10, 10)
}

func DisableTime(tb testing.TB) {
	internal.SetTimeProvider(func() (t time.Time) { return t })
	tb.Cleanup(func() {
		internal.SetTimeProvider(time.Now)
	})
}
