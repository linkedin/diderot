package diderot

import (
	"context"
	"iter"
	"log/slog"
	"maps"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"github.com/linkedin/diderot/ads"
	"github.com/linkedin/diderot/testutils"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Timestamp = timestamppb.Timestamp

var Now = timestamppb.Now

func TestADSClient(t *testing.T) {
	slog.SetLogLoggerLevel(slog.LevelDebug)

	ts := testutils.NewTestGRPCServer(t)

	timestampCache := NewCache[*Timestamp]()

	s := NewADSServer(mockResourceLocator(func(typeURL, resourceName string, h ads.RawSubscriptionHandler) func() {
		var c RawCache
		switch typeURL {
		case timestampCache.Type().URL():
			c = timestampCache
		default:
			h.Notify(resourceName, nil, ads.SubscriptionMetadata{})
			return func() {}
		}
		Subscribe(c, resourceName, h)
		return func() {
			Unsubscribe(c, resourceName, h)
		}
	}))
	discovery.RegisterAggregatedDiscoveryServiceServer(ts.Server, s)
	ts.Start()

	acceptStream, providerOpt := newContextProvider(t)

	client := NewADSClient(ts.Dial(), &ads.Node{Id: "test"}, providerOpt)
	fooH := make(testutils.ChanSubscriptionHandler[*Timestamp], 1)
	foo := ads.NewResource[*Timestamp]("foo", "0", Now())
	Watch[*Timestamp](client, foo.Name, ChanWatcher[*Timestamp](fooH))

	// The stream has not yet been established, no updates should be received.
	checkNoUpdate(t, fooH)

	// Accept a new stream
	closeStream := acceptStream()

	// The resource does not exist in the cache, the first update should be a deletion.
	fooH.WaitForDelete(t, foo.Name)

	// Set foo in the cache, and wait for the creation update
	timestampCache.SetResource(foo, time.Time{})
	fooH.WaitForUpdate(t, foo)

	closeStream()
	closeStream = acceptStream()
	// Closing and reopening the stream makes the client reconnect, but since foo hasn't changed, nothing
	// should happen.
	checkNoUpdate(t, fooH)

	// Disconnect the client, update foo and expect a notification on reconnect.
	closeStream()
	foo = timestampCache.Set(foo.Name, "1", Now(), time.Time{})
	closeStream = acceptStream()
	fooH.WaitForUpdate(t, foo)

	// Set bar, nothing is currently subscribed to bar, least of all fooH so no updates should be received.
	bar := ads.NewResource[*Timestamp]("bar", "0", Now())
	timestampCache.SetResource(bar, time.Time{})
	checkNoUpdate(t, fooH)

	wildcardH := make(testutils.ChanSubscriptionHandler[*Timestamp], 2)
	var wildcardExpectedCount atomic.Int32
	Watch[*Timestamp](client, ads.WildcardSubscription, &FuncWatcher[*Timestamp]{
		notify: func(resources iter.Seq2[string, *ads.Resource[*Timestamp]]) error {
			require.Len(t, maps.Collect(resources), int(wildcardExpectedCount.Load()))
			for name, resource := range resources {
				wildcardH <- testutils.Notification[*Timestamp]{
					Name:     name,
					Resource: resource,
				}
			}
			return nil
		},
	})

	// Expect a notification for foo and bar for wildcardH, but since fooH has already seen that version
	// of foo, it should not receive an update.
	wildcardExpectedCount.Store(2)
	wildcardH.WaitForNotifications(t,
		testutils.ExpectUpdate(foo),
		testutils.ExpectUpdate(bar),
	)
	checkNoUpdate(t, fooH)

	// Clear foo, expect a deletion on fooH and the wildcard subscriber.
	wildcardExpectedCount.Store(1)
	timestampCache.Clear(foo.Name, time.Time{})
	fooH.WaitForDelete(t, foo.Name)
	wildcardH.WaitForDelete(t, foo.Name)

	// Create new glob collection entries, which the wildcard subscriber should receive.
	wildcardExpectedCount.Store(1)
	gcURL := ads.NewGlobCollectionURL[*Timestamp]("", "collection", nil)
	fooGlob := timestampCache.Set(gcURL.MemberURN("foo"), "0", Now(), time.Time{})
	wildcardH.WaitForNotifications(t,
		testutils.ExpectUpdate(fooGlob),
	)
	barGlob := timestampCache.Set(gcURL.MemberURN("bar"), "0", Now(), time.Time{})
	wildcardH.WaitForNotifications(t,
		testutils.ExpectUpdate(barGlob),
	)

	// Subscribe to the glob collection. expecting an update for fooGlob and barGlob.
	globH := make(testutils.ChanSubscriptionHandler[*Timestamp], 2)
	var globExpectedCount atomic.Int32
	globExpectedCount.Store(2)
	Watch[*Timestamp](client, gcURL.String(), &FuncWatcher[*Timestamp]{
		notify: func(resources iter.Seq2[string, *ads.Resource[*Timestamp]]) error {
			require.Len(t, maps.Collect(resources), int(globExpectedCount.Load()))
			for name, resource := range resources {
				globH <- testutils.Notification[*Timestamp]{
					Name:     name,
					Resource: resource,
				}
			}
			return nil
		},
	})
	globH.WaitForNotifications(t,
		testutils.ExpectUpdate(fooGlob),
		testutils.ExpectUpdate(barGlob),
	)

	// Clear fooGlob, expect deletions for it.
	wildcardExpectedCount.Store(1)
	globExpectedCount.Store(1)
	timestampCache.Clear(fooGlob.Name, time.Time{})
	wildcardH.WaitForDelete(t, fooGlob.Name)
	globH.WaitForDelete(t, fooGlob.Name)

	// Disconnect the client and clear the collection during the disconnect. When the client reconnects,
	// because it explicitly subscribes to the glob collection it will receive a deletion notification
	// for the entire collection, but not for barGlob explicitly, as the server has forgotten that it
	// exists. The client must figure out that barGlob has disappeared while it was disconnected. The
	// same is true for the wildcard subscription: the client will not receive an explicit notification
	// that barGlob has disappeared.
	closeStream()
	timestampCache.Clear(barGlob.Name, time.Time{})
	closeStream = acceptStream()
	globH.WaitForDelete(t, barGlob.Name)
	wildcardH.WaitForDelete(t, barGlob.Name)

	// This is an edge case, but bar is known because of the wildcard subscription. Therefore, even while
	// the client is offline, subscribing to bar should deliver the notification.
	closeStream()
	barH := make(testutils.ChanSubscriptionHandler[*Timestamp], 1)
	Watch[*Timestamp](client, bar.Name, ChanWatcher[*Timestamp](barH))
	barH.WaitForUpdate(t, bar)

	closeStream = acceptStream()
	// There should be an explicit subscription sent, but because bar is already known, no further
	// updates should be received.
	checkNoUpdate(t, barH)

	timestampCache.Clear(bar.Name, time.Time{})

	barH.WaitForDelete(t, bar.Name)
	wildcardH.WaitForDelete(t, bar.Name)

	// Disconnect again to test what happens when Watch is called while offline for glob and wildcards.
	closeStream()
	allResources := new(map[string]*ads.Resource[*Timestamp])
	Watch[*Timestamp](client, ads.WildcardSubscription, OnceWatcher[*Timestamp](allResources))
	// This should be immediately ready, as data has been received and far as the client knows, there are
	// no resources.
	require.Empty(t, *allResources)

	// Same behavior expected for glob
	allGlobResource := new(map[string]*ads.Resource[*Timestamp])
	Watch[*Timestamp](client, gcURL.String(), OnceWatcher[*Timestamp](allGlobResource))
	require.Empty(t, *allGlobResource)
}

func newContextProvider(t *testing.T) (acceptStream func() context.CancelFunc, provider ADSClientOption) {
	contextCancels := make(chan context.CancelFunc)
	provider = withContextProvider(func() context.Context {
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		contextCancels <- cancel
		return ctx
	})
	return func() context.CancelFunc {
		return <-contextCancels
	}, provider
}

type FuncWatcher[T proto.Message] struct {
	notify func(resources iter.Seq2[string, *ads.Resource[T]]) error
}

func (f FuncWatcher[T]) Notify(resources iter.Seq2[string, *ads.Resource[T]]) error {
	return f.notify(resources)
}

type ChanWatcher[T proto.Message] testutils.ChanSubscriptionHandler[T]

func (c ChanWatcher[T]) Notify(resources iter.Seq2[string, *ads.Resource[T]]) error {
	for name, resource := range resources {
		testutils.ChanSubscriptionHandler[T](c).Notify(name, resource, ads.SubscriptionMetadata{})
	}
	return nil
}

func checkNoUpdate[T proto.Message](t *testing.T, h testutils.ChanSubscriptionHandler[T]) {
	select {
	case n := <-h:
		require.FailNow(t, "handler should not receive any messages", n)
	case <-time.After(500 * time.Millisecond):
	}
}

func OnceWatcher[T proto.Message](m *map[string]*ads.Resource[T]) Watcher[T] {
	var once sync.Once
	return &FuncWatcher[T]{notify: func(resources iter.Seq2[string, *ads.Resource[T]]) error {
		once.Do(func() {
			*m = maps.Collect(resources)
		})
		return nil
	}}
}
