package diderot

import (
	"context"
	"fmt"
	"io"
	"iter"
	"maps"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"github.com/linkedin/diderot/ads"
	"github.com/linkedin/diderot/internal/utils"
	"github.com/linkedin/diderot/testutils"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Timestamp = timestamppb.Timestamp

var Now = timestamppb.Now

func newClient(t *testing.T, chunkingSupport bool) (conn *mockConn, client *ADSClient) {
	conn = &mockConn{
		t:       t,
		streams: make(chan *mockStream),
	}
	client = NewADSClient(
		conn,
		&ads.Node{Id: "test"},
		WithReconnectBackoff(0, 0),
		WithResponseChunkingSupported(chunkingSupport),
	)
	return conn, client
}

func TestADSClient(t *testing.T) {
	tests := []struct {
		chunkingEnabled bool
	}{
		{
			chunkingEnabled: false,
		},
		{
			chunkingEnabled: true,
		},
	}
	for _, test := range tests {
		t.Run(fmt.Sprintf("flow/chunkingEnabled=%v", test.chunkingEnabled), func(t *testing.T) {
			testADSClientFlow(t, test.chunkingEnabled)
		})
	}

	// Check that the client NACKs a response for a type that was never subscribed to.
	t.Run("invalid type", func(t *testing.T) {
		conn, client := newClient(t, defaultResponseChunkingSupported)

		Watch(client, ads.WildcardSubscription, &FuncWatcher[*Timestamp]{
			notify: func(resources iter.Seq2[string, *ads.Resource[*Timestamp]]) error {
				require.FailNow(t, "Should not be called")
				return nil
			},
		})

		ms := conn.accept()

		ms.expectSubscriptions(ads.WildcardSubscription)

		nonce := respond[*durationpb.Duration](ms, []*ads.Resource[*durationpb.Duration]{
			ads.NewResource[*durationpb.Duration]("test", "0", durationpb.New(time.Minute)),
		}, nil, 0)

		expectNACK[*durationpb.Duration](ms, nonce, codes.InvalidArgument, utils.GetTypeURL[*durationpb.Duration]())
	})

	// Check that the client NACKs a response if a watcher returns an error.
	t.Run("NACKs", func(t *testing.T) {
		conn, client := newClient(t, defaultResponseChunkingSupported)

		Watch(client, ads.WildcardSubscription, &FuncWatcher[*Timestamp]{
			notify: func(resources iter.Seq2[string, *ads.Resource[*Timestamp]]) error {
				return io.EOF
			},
		})

		ms := conn.accept()

		ms.expectSubscriptions(ads.WildcardSubscription)

		nonce := ms.respondUpdates(0, ads.NewResource[*Timestamp]("foo", "0", Now()))

		expectNACK[*Timestamp](ms, nonce, codes.InvalidArgument, io.EOF.Error())
	})

	// Check that if the server responds with an unknown resource, it is skipped and reported, but other
	// valid resources in the response are still parsed.
	t.Run("unknown resource", func(t *testing.T) {
		conn, client := newClient(t, defaultResponseChunkingSupported)

		fooH := make(testutils.ChanSubscriptionHandler[*Timestamp], 1)
		foo := ads.NewResource[*Timestamp]("foo", "0", Now())
		Watch(client, foo.Name, ChanWatcher[*Timestamp](fooH))

		ms := conn.accept()

		ms.expectSubscriptions(foo.Name)

		nonce := ms.respondUpdates(0, foo, ads.NewResource("bar", "0", Now()))

		expectNACK[*Timestamp](ms, nonce, codes.InvalidArgument, "bar")
		fooH.WaitForUpdate(t, foo)
	})
}

func testADSClientFlow(t *testing.T, chunkingEnabled bool) {
	conn, client := newClient(t, chunkingEnabled)

	fooH := make(testutils.ChanSubscriptionHandler[*Timestamp], 1)
	foo := ads.NewResource[*Timestamp]("foo", "0", Now())
	Watch(client, foo.Name, ChanWatcher[*Timestamp](fooH))

	// The stream has not yet been established, no updates should be received.
	checkNoUpdate(t, fooH)

	// Accept a new stream
	ms := conn.accept()

	// The resource does not initially exist, the first update should be a deletion.
	ms.expectSubscriptions(foo.Name)
	nonce := ms.respondDeletes(0, foo.Name)
	fooH.WaitForDelete(t, foo.Name)
	ms.expectACK(nonce)

	// Set foo, and wait for the creation update
	nonce = ms.respondUpdates(0, foo)
	fooH.WaitForUpdate(t, foo)
	ms.expectACK(nonce)

	ms.cancel()
	ms = conn.accept()
	// Closing and reopening the stream makes the client reconnect, but since foo hasn't changed, nothing
	// should happen.
	ms.expectSubscriptions(foo.Name)
	nonce = ms.respondUpdates(0, foo)
	checkNoUpdate(t, fooH)
	ms.expectACK(nonce)

	// Disconnect the client, foo is updated during disconnect so expect a notification
	ms.cancel()
	foo = ads.NewResource(foo.Name, "1", Now())
	ms = conn.accept()
	ms.expectSubscriptions(foo.Name)
	nonce = ms.respondUpdates(0, foo)
	fooH.WaitForUpdate(t, foo)
	ms.expectACK(nonce)

	wildcardH := make(testutils.ChanSubscriptionHandler[*Timestamp], 2)
	var wildcardExpectedCount atomic.Int32
	Watch(client, ads.WildcardSubscription, &FuncWatcher[*Timestamp]{
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

	ms.expectSubscriptions(ads.WildcardSubscription)
	bar := ads.NewResource[*Timestamp]("bar", "0", Now())
	if chunkingEnabled {
		// Respond in multiple chunks, to test that those are handled correctly
		chunkNonce1 := ms.respondUpdates(1, foo)
		// No update expected after first chunk
		checkNoUpdate(t, wildcardH)
		// As soon as the second chunk arrives, an update is expected, so update the expected count before
		// sending the response.
		wildcardExpectedCount.Store(2)
		chunkNonce2 := ms.respondUpdates(0, bar)
		ms.expectACK(chunkNonce1)
		ms.expectACK(chunkNonce2)
	} else {
		wildcardExpectedCount.Store(2)
		nonce = ms.respondUpdates(0, foo, bar)
		ms.expectACK(nonce)
	}

	// Expect a notification for foo and bar for wildcardH, but since fooH has already seen that version
	// of foo, it should not receive an update.
	wildcardH.WaitForNotifications(t,
		testutils.ExpectUpdate(foo),
		testutils.ExpectUpdate(bar),
	)
	checkNoUpdate(t, fooH)

	// Clear foo, expect a deletion on fooH and the wildcard subscriber.
	wildcardExpectedCount.Store(1)
	nonce = ms.respondDeletes(0, foo.Name)
	ms.expectACK(nonce)
	fooH.WaitForDelete(t, foo.Name)
	wildcardH.WaitForDelete(t, foo.Name)

	// Create new glob collection entries, which the wildcard subscriber should receive.
	wildcardExpectedCount.Store(1)
	gcURL := ads.NewGlobCollectionURL[*Timestamp]("", "collection", nil)
	fooGlob := ads.NewResource(gcURL.MemberURN("foo"), "0", Now())
	nonce = ms.respondUpdates(0, fooGlob)
	ms.expectACK(nonce)
	wildcardH.WaitForNotifications(t, testutils.ExpectUpdate(fooGlob))

	barGlob := ads.NewResource(gcURL.MemberURN("bar"), "0", Now())
	nonce = ms.respondUpdates(0, barGlob)
	ms.expectACK(nonce)
	wildcardH.WaitForNotifications(t, testutils.ExpectUpdate(barGlob))

	// Subscribe to the glob collection. expecting an update for fooGlob and barGlob.
	globH := make(testutils.ChanSubscriptionHandler[*Timestamp], 2)
	var globExpectedCount atomic.Int32
	// Because the resources are already known thanks to the wildcard, this expects a notification
	// immediately, before the subscription is even sent.
	Watch(client, gcURL.String(), &FuncWatcher[*Timestamp]{
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
	ms.expectSubscriptions(gcURL.String())
	globExpectedCount.Store(2)
	nonce = ms.respondUpdates(0, fooGlob, barGlob)
	ms.expectACK(nonce)
	globH.WaitForNotifications(t,
		testutils.ExpectUpdate(fooGlob),
		testutils.ExpectUpdate(barGlob),
	)
	globExpectedCount.Store(0)

	// Clear fooGlob, expect deletions for it.
	wildcardExpectedCount.Store(1)
	globExpectedCount.Store(1)
	nonce = ms.respondDeletes(0, fooGlob.Name)
	ms.expectACK(nonce)
	wildcardH.WaitForDelete(t, fooGlob.Name)
	globH.WaitForDelete(t, fooGlob.Name)

	// Disconnect the client and clear the collection during the disconnect. When the client reconnects,
	// because it explicitly subscribes to the glob collection it will receive a deletion notification
	// for the entire collection, but not for barGlob explicitly, as the server has forgotten that it
	// exists. The client must figure out that barGlob has disappeared while it was disconnected. The
	// same is true for the wildcard subscription: the client will not receive an explicit notification
	// that barGlob has disappeared.
	ms.cancel()
	ms = conn.accept()
	ms.expectSubscriptions(foo.Name, ads.WildcardSubscription, gcURL.String())

	nonce = respond[*Timestamp](
		ms,
		// The only remaining resource is bar
		[]*ads.Resource[*Timestamp]{bar},
		// These are explicitly subscribed to but do not exist, so explicit removals are expected
		[]string{foo.Name, gcURL.String()},
		0,
	)
	ms.respondUpdates(0, bar)
	ms.expectACK(nonce)
	globH.WaitForDelete(t, barGlob.Name)
	wildcardH.WaitForDelete(t, barGlob.Name)

	// This is an edge case, but bar is known because of the wildcard subscription. Therefore, even while
	// the client is offline, subscribing to bar should deliver the notification.
	ms.cancel()
	barH := make(testutils.ChanSubscriptionHandler[*Timestamp], 1)
	Watch(client, bar.Name, ChanWatcher[*Timestamp](barH))
	barH.WaitForUpdate(t, bar)
	ms = conn.accept()
	// There should be an explicit subscription sent, but because bar is already known, no further
	// updates should be received.
	ms.expectSubscriptions(foo.Name, bar.Name, ads.WildcardSubscription, gcURL.String())
	nonce = ms.respondUpdates(0, bar)
	ms.expectACK(nonce)
	checkNoUpdate(t, barH)

	// Delete bar, the final resource
	nonce = ms.respondDeletes(0, bar.Name)
	ms.expectACK(nonce)

	barH.WaitForDelete(t, bar.Name)
	wildcardH.WaitForDelete(t, bar.Name)

	// Disconnect again to test what happens when Watch is called while offline for glob and wildcards.
	ms.cancel()
	allResources := new(map[string]*ads.Resource[*Timestamp])
	Watch(client, ads.WildcardSubscription, OnceWatcher(allResources))
	// This should be immediately ready, as data has been received and far as the client knows, there are
	// no resources.
	require.NotNil(t, *allResources)
	require.Empty(t, *allResources)

	// Same behavior expected for glob
	allGlobResource := new(map[string]*ads.Resource[*Timestamp])
	Watch(client, gcURL.String(), OnceWatcher(allGlobResource))
	require.NotNil(t, *allGlobResource)
	require.Empty(t, *allGlobResource)
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

type mockConn struct {
	t       *testing.T
	streams chan *mockStream
}

func (mc *mockConn) Invoke(context.Context, string, any, any, ...grpc.CallOption) error {
	mc.t.Fatalf("Not supported")
	return nil
}

func (mc *mockConn) NewStream(
	_ context.Context,
	_ *grpc.StreamDesc,
	method string,
	_ ...grpc.CallOption,
) (grpc.ClientStream, error) {
	require.Equal(mc.t, discovery.AggregatedDiscoveryService_DeltaAggregatedResources_FullMethodName, method)
	return <-mc.streams, nil
}

func (mc *mockConn) accept() *mockStream {
	s := &mockStream{
		conn:      mc,
		requests:  make(chan *ads.DeltaDiscoveryRequest),
		responses: make(chan *ads.DeltaDiscoveryResponse),
	}
	s.ctx, s.cancel = context.WithCancel(context.Background())
	mc.streams <- s
	return s
}

type mockStream struct {
	conn      *mockConn
	ctx       context.Context
	cancel    context.CancelFunc
	requests  chan *ads.DeltaDiscoveryRequest
	responses chan *ads.DeltaDiscoveryResponse
}

func (ms *mockStream) Header() (metadata.MD, error) {
	return nil, nil
}

func (ms *mockStream) Trailer() metadata.MD {
	return nil
}

func (ms *mockStream) CloseSend() error {
	ms.conn.t.Fatalf("Not supported")
	return nil
}

func (ms *mockStream) Context() context.Context {
	return ms.ctx
}

func (ms *mockStream) SendMsg(msg any) error {
	require.IsType(ms.conn.t, (*ads.DeltaDiscoveryRequest)(nil), msg)
	select {
	case ms.requests <- msg.(*ads.DeltaDiscoveryRequest):
		return nil
	case <-ms.ctx.Done():
		return ms.ctx.Err()
	}
}

func (ms *mockStream) RecvMsg(msg any) error {
	require.IsType(ms.conn.t, (*ads.DeltaDiscoveryResponse)(nil), msg)
	select {
	case res := <-ms.responses:
		proto.Merge(msg.(*ads.DeltaDiscoveryResponse), res)
		return nil
	case <-ms.ctx.Done():
		return ms.ctx.Err()
	}
}

func (ms *mockStream) respondUpdates(
	remainingChunks int,
	resources ...*ads.Resource[*Timestamp],
) string {
	return respond[*Timestamp](ms, resources, nil, remainingChunks)
}

func (ms *mockStream) respondDeletes(
	remainingChunks int,
	removedResources ...string,
) string {
	return respond[*Timestamp](ms, nil, removedResources, remainingChunks)
}

func respond[T proto.Message](
	ms *mockStream,
	resources []*ads.Resource[T],
	removedResources []string,
	remainingChunks int,
) string {
	var marshaled []*ads.RawResource
	for _, resource := range resources {
		raw, err := resource.Marshal()
		require.NoError(ms.conn.t, err)
		marshaled = append(marshaled, raw)
	}
	nonce := utils.NewNonce(remainingChunks)
	ms.responses <- &ads.DeltaDiscoveryResponse{
		Resources:        marshaled,
		TypeUrl:          utils.GetTypeURL[T](),
		RemovedResources: removedResources,
		Nonce:            nonce,
	}
	return nonce
}

func (ms *mockStream) expectACK(nonce string) {
	req := <-ms.requests
	require.Equal(ms.conn.t, utils.GetTypeURL[*Timestamp](), req.TypeUrl)
	require.Equal(ms.conn.t, nonce, req.ResponseNonce)
}

func expectNACK[T proto.Message](ms *mockStream, nonce string, code codes.Code, errorContains string) {
	req := <-ms.requests
	require.Equal(ms.conn.t, utils.GetTypeURL[T](), req.TypeUrl)
	require.Equal(ms.conn.t, nonce, req.ResponseNonce)
	st := status.FromProto(req.GetErrorDetail())
	require.Equal(ms.conn.t, code, st.Code())
	require.ErrorContains(ms.conn.t, st.Err(), errorContains)
}

func (ms *mockStream) expectSubscriptions(subscriptions ...string) {
	req := <-ms.requests
	require.Equal(ms.conn.t, utils.GetTypeURL[*Timestamp](), req.TypeUrl)
	require.Empty(ms.conn.t, req.ResponseNonce)
	require.ElementsMatch(ms.conn.t, subscriptions, req.ResourceNamesSubscribe)
}
