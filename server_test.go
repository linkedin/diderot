package diderot

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	core "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	endpoint "github.com/envoyproxy/go-control-plane/envoy/config/endpoint/v3"
	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"github.com/linkedin/diderot/ads"
	internal "github.com/linkedin/diderot/internal/server"
	"github.com/linkedin/diderot/internal/utils"
	serverstats "github.com/linkedin/diderot/stats/server"
	"github.com/linkedin/diderot/testutils"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/xds"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

var (
	badTypeURL   = "foobar"
	badResources = []string{"badResource1", "badResource2"}
	controlPlane = &core.ControlPlane{Identifier: "fooBar"}
)

type serverStatsHandler struct {
	UnknownTypes     atomic.Int64
	UnknownResources atomic.Int64
	NACKsReceived    atomic.Int64
	ACKsReceived     atomic.Int64
}

func (m *serverStatsHandler) HandleServerEvent(ctx context.Context, event serverstats.Event) {
	switch e := event.(type) {
	case *serverstats.RequestReceived:
		if e.IsNACK {
			m.NACKsReceived.Add(1)
		}
		if e.IsACK {
			m.ACKsReceived.Add(1)
		}
		if e.IsRequestedTypeUnknown {
			m.UnknownTypes.Add(1)
		}
	case *serverstats.ResourceQueued:
		if !e.ResourceExists {
			m.UnknownResources.Add(1)
		}
	}
}

func (m *serverStatsHandler) reset() {
	m.UnknownTypes.Store(0)
	m.UnknownResources.Store(0)
	m.NACKsReceived.Store(0)
	m.ACKsReceived.Store(0)
}

type testLocator struct {
	t      *testing.T
	node   *ads.Node
	caches map[string]RawCache
}

func (tl *testLocator) checkContextNode(streamCtx context.Context) {
	if tl.node == nil {
		// skip checking node if nil
		return
	}
	node, ok := NodeFromContext(streamCtx)
	require.True(tl.t, ok)
	testutils.ProtoEquals(tl.t, tl.node, node)
}

func (tl *testLocator) IsTypeSupported(streamCtx context.Context, typeURL string) bool {
	tl.checkContextNode(streamCtx)
	_, ok := tl.caches[typeURL]
	return ok
}

func (tl *testLocator) Subscribe(
	streamCtx context.Context,
	typeURL, resourceName string,
	handler ads.RawSubscriptionHandler,
) (unsubscribe func()) {
	tl.checkContextNode(streamCtx)

	c := tl.caches[typeURL]
	Subscribe(c, resourceName, handler)
	return func() {
		Unsubscribe(c, resourceName, handler)
	}
}

func (tl *testLocator) Resubscribe(
	streamCtx context.Context,
	typeURL, resourceName string,
	handler ads.RawSubscriptionHandler,
) {
	tl.checkContextNode(streamCtx)
	Subscribe(tl.caches[typeURL], resourceName, handler)
}

func newTestLocator(t *testing.T, node *ads.Node, types ...Type) *testLocator {
	tl := &testLocator{
		t:      t,
		node:   node,
		caches: make(map[string]RawCache),
	}
	for _, tpe := range types {
		tl.caches[tpe.URL()] = tpe.NewCache()
	}
	return tl
}

func getCache[T proto.Message](tl *testLocator) Cache[T] {
	return tl.caches[TypeOf[T]().URL()].(Cache[T])
}

func TestEndToEnd(t *testing.T) {
	locator := newTestLocator(
		t,
		&ads.Node{
			Id:                   "diderot-test",
			UserAgentName:        "gRPC Go",
			UserAgentVersionType: &core.Node_UserAgentVersion{UserAgentVersion: grpc.Version},
			ClientFeatures: []string{
				"envoy.lb.does_not_support_overprovisioning",
				"xds.config.resource-in-sotw",
			},
		},
		TypeOf[*ads.Endpoint](),
		TypeOf[*ads.Cluster](),
		TypeOf[*ads.Route](),
		TypeOf[*ads.Listener](),
		TypeOf[*wrapperspb.BytesValue](),
	)

	endpointCache := getCache[*ads.Endpoint](locator)
	listenerCache := getCache[*ads.Listener](locator)
	bytesCache := getCache[*wrapperspb.BytesValue](locator)

	ts := testutils.NewTestGRPCServer(t)

	resources := readResourcesFromJSONFile(t, "test_xds_config.json")
	require.Len(t, resources, 3)

	for _, r := range resources {
		c, ok := locator.caches[r.Resource.TypeUrl]
		require.Truef(t, ok, "Unknown type loaded from test config %q: %+v", r.Resource.TypeUrl, r)
		require.NoError(t, c.SetRaw(r, time.Now()))
	}

	addr := ts.Addr().(*net.TCPAddr)
	endpointCache.Set(
		"testADSServer",
		"0",
		&ads.Endpoint{
			ClusterName: "testADSServer",
			Endpoints: []*endpoint.LocalityLbEndpoints{{
				Locality:            new(core.Locality),
				LoadBalancingWeight: wrapperspb.UInt32(1),
				LbEndpoints: []*endpoint.LbEndpoint{{
					HostIdentifier: &endpoint.LbEndpoint_Endpoint{
						Endpoint: &endpoint.Endpoint{
							Address: &core.Address{
								Address: &core.Address_SocketAddress{
									SocketAddress: &core.SocketAddress{
										Protocol:      core.SocketAddress_TCP,
										Address:       addr.IP.String(),
										PortSpecifier: &core.SocketAddress_PortValue{PortValue: uint32(addr.Port)},
									},
								},
							},
							Hostname: "localhost",
						},
					},
				}},
			}},
		},
		time.Now(),
	)

	statsHandler := new(serverStatsHandler)

	s := NewADSServer(
		locator,
		WithGranularResponseRateLimit(0),
		WithGlobalResponseRateLimit(0),
		WithServerStatsHandler(statsHandler),
		WithControlPlane(controlPlane),
	)
	discovery.RegisterAggregatedDiscoveryServiceServer(ts.Server, s)
	ts.Start()

	xdsResolverBuilder, err := xds.NewXDSResolverWithConfigForTesting([]byte(`{
	 "xds_servers": [
	   {
	     "server_uri": "` + ts.AddrString() + `",
	     "channel_creds": [{"type": "insecure"}],
	     "server_features": ["xds_v3"]
	   }
	 ],
	 "node": { "id": "diderot-test" }
	}`))
	require.NoError(t, err)

	conn, err := grpc.NewClient(
		"xds:///testADSServer",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithResolvers(xdsResolverBuilder),
	)
	require.NoError(t, err)

	client := discovery.NewAggregatedDiscoveryServiceClient(conn)
	t.Run("xDS sanity check", func(t *testing.T) {
		// It's expected that the xDS client will have ACKed the responses it received from the server during
		// the LDS -> RDS -> CDS -> EDS flow. However, since grpc.NewClient does not actually attempt to
		// establish a connection until the very last moment, testing this actually requires opening a
		// stream. This test opens a stream, checks the ACK counter then closes it.

		stream, err := client.DeltaAggregatedResources(testutils.ContextWithTimeout(t, 5*time.Second))
		require.NoError(t, err)

		require.Equal(t, int64(4), statsHandler.ACKsReceived.Load())

		require.NoError(t, stream.CloseSend())
	})

	testData := &wrapperspb.BytesValue{Value: make([]byte, 20)}
	_, err = rand.Read(testData.Value)
	require.NoError(t, err)
	testResource := ads.NewResource("testData", "0", testData)
	clearEntry := func() {
		bytesCache.Clear(testResource.Name, time.Now())
	}
	setEntry := func(t *testing.T) {
		bytesCache.SetResource(testResource, time.Now())
		t.Cleanup(clearEntry)
	}

	t.Run("delta", func(t *testing.T) {
		statsHandler.reset()

		stream, err := client.DeltaAggregatedResources(testutils.ContextWithTimeout(t, 5*time.Second))
		require.NoError(t, err)

		req := &ads.DeltaDiscoveryRequest{
			Node:                   locator.node,
			TypeUrl:                testResource.TypeURL(),
			ResourceNamesSubscribe: []string{testResource.Name},
		}

		require.NoError(t, stream.Send(req))

		res := new(ads.DeltaDiscoveryResponse)
		waitForResponse(t, res, stream, 10*time.Millisecond)

		require.Equal(t, res.RemovedResources, []string{testResource.Name})
		require.Equal(t, int64(1), statsHandler.UnknownResources.Load())

		setEntry(t)

		waitForResponse(t, res, stream, 10*time.Millisecond)

		require.Len(t, res.Resources, 1)
		testutils.ProtoEquals(t, testutils.MustMarshal(t, testResource), res.Resources[0])

		// check that re-subscribing to a resource causes the server to resend it.
		require.NoError(t, stream.Send(req))

		waitForResponse(t, res, stream, 10*time.Millisecond)

		require.Len(t, res.Resources, 1)
		testutils.ProtoEquals(t, testutils.MustMarshal(t, testResource), res.Resources[0])

		req.ResourceNamesSubscribe = nil
		req.ResponseNonce = res.Nonce
		require.NoError(t, stream.Send(req))

		// It's hard to test for the _absence_ of a response to the ACK, however the followup check for the
		// removed resource will fail if the server responds to the ACK with anything unexpected. The test
		// can still be forced to fail early by checking the value of the ACK metric.
		require.Eventually(t, func() bool {
			return statsHandler.ACKsReceived.Load() == 1
		}, 2*time.Second, 100*time.Millisecond)

		clearEntry()

		waitForResponse(t, res, stream, 10*time.Millisecond)

		require.Len(t, res.Resources, 0)
		require.Equal(t, res.RemovedResources, []string{testResource.Name})

		// However, the server should respect subscription changes in an ACK. By subscribing to a resource
		// that does not exist, it can be forced to respond with a deletion.
		req.ResourceNamesSubscribe = []string{"noSuchResource"}
		req.ResponseNonce = res.Nonce
		require.NoError(t, stream.Send(req))

		waitForResponse(t, res, stream, 10*time.Millisecond)

		require.Len(t, res.Resources, 0)
		require.Equal(t, res.RemovedResources, req.ResourceNamesSubscribe)
		require.Equal(t, int64(2), statsHandler.ACKsReceived.Load())

		// Finally, the NACK metric can be tested by NACKing the previous response. No response is expected
		// from the server for this NACK, so the NACK metric needs to be checked.
		req.ResourceNamesSubscribe = nil
		req.ResponseNonce = res.Nonce
		req.ErrorDetail = &status.Status{
			Code:    420,
			Message: "Testing NACK",
		}
		require.NoError(t, stream.Send(req))

		require.Eventually(t, func() bool {
			return statsHandler.NACKsReceived.Load() == 1
		}, 2*time.Second, 100*time.Millisecond)

		require.NoError(t, stream.Send(&ads.DeltaDiscoveryRequest{
			Node:                   new(core.Node),
			TypeUrl:                badTypeURL,
			ResourceNamesSubscribe: badResources,
		}))

		waitForResponse(t, res, stream, 10*time.Millisecond)

		require.Equal(t, badTypeURL, res.GetTypeUrl())
		require.Equal(t, badResources, res.GetRemovedResources())
		require.Equal(t, int64(1), statsHandler.UnknownTypes.Load())
	})

	t.Run("SotW", func(t *testing.T) {
		statsHandler.reset()

		testListener1 := ads.NewResource("testListener1", "0", &ads.Listener{Name: "testListener1"})
		testListener2 := ads.NewResource("testListener2", "1", &ads.Listener{Name: "testListener2"})
		// This test relies on Listener not being a pseudo delta resource type, so fail the test early otherwise
		require.False(t, utils.IsPseudoDeltaSotW(testListener1.TypeURL()))

		stream, err := client.StreamAggregatedResources(testutils.Context(t))
		require.NoError(t, err)

		req := &ads.SotWDiscoveryRequest{
			Node:          locator.node,
			TypeUrl:       testListener1.TypeURL(),
			ResourceNames: []string{testListener1.Name, testListener2.Name},
		}
		require.NoError(t, stream.Send(req))

		res := new(ads.SotWDiscoveryResponse)
		waitForResponse(t, res, stream, 10*time.Millisecond)
		require.Empty(t, res.Resources)
		require.Equal(t, int64(2), statsHandler.UnknownResources.Load())

		listenerCache.SetResource(testListener1, time.Now())
		t.Cleanup(func() {
			listenerCache.Clear(testListener1.Name, time.Now())
		})
		waitForResponse(t, res, stream, 10*time.Millisecond)
		if len(res.Resources) == 0 {
			// This can happen because the responses from the cache notifying the client that testListener1 and
			// testListener2 arrive asynchronously, so the server may have sent the response for testListener1
			// not being present before receiving the notification for testListener2. This is simply a property
			// of SotW, and it's hard to work around.
			waitForResponse(t, res, stream, 10*time.Millisecond)
		}
		require.Len(t, res.Resources, 1)
		testutils.ProtoEquals(t, testutils.MustMarshal(t, testListener1).Resource, res.Resources[0])

		listenerCache.SetResource(testListener2, time.Now())
		waitForResponse(t, res, stream, 10*time.Millisecond)
		require.Len(t, res.Resources, 2)
		// Order is not guaranteed, so it must be checked explicitly
		if proto.Equal(testutils.MustMarshal(t, testListener1).Resource, res.Resources[0]) {
			testutils.ProtoEquals(t, testutils.MustMarshal(t, testListener2).Resource, res.Resources[1])
		} else {
			testutils.ProtoEquals(t, testutils.MustMarshal(t, testListener1).Resource, res.Resources[1])
			testutils.ProtoEquals(t, testutils.MustMarshal(t, testListener2).Resource, res.Resources[0])
		}

		req.VersionInfo = res.VersionInfo
		req.ResponseNonce = res.Nonce
		require.NoError(t, stream.Send(req))

		// It's hard to test for the _absence_ of a response to the ACK, however the followup check for the
		// removed resource will fail if the server responds to the ACK with anything unexpected. The test
		// can still be forced to fail early by checking the value of the ACK metric.
		require.Eventually(t, func() bool {
			return statsHandler.ACKsReceived.Load() == 1
		}, 2*time.Second, 100*time.Millisecond)

		listenerCache.Clear(testListener2.Name, time.Now())
		waitForResponse(t, res, stream, 10*time.Millisecond)
		require.Len(t, res.Resources, 1)
		testutils.ProtoEquals(t, testutils.MustMarshal(t, testListener1).Resource, res.Resources[0])

		// However, the server is supposed to respect any changes to subscriptions when ACKing, so ACKing the
		// most recent response with a different subscription list, namely by adding a resource that does not
		// exist (testListener3 in this case), the server should send back a response indicating that it does
		// not exist (which in SotW means sending only testListener1).
		req.VersionInfo = res.VersionInfo
		req.ResponseNonce = res.Nonce
		req.ResourceNames = append(req.ResourceNames, "testListener3")
		require.NoError(t, stream.Send(req))

		waitForResponse(t, res, stream, 10*time.Millisecond)
		require.Len(t, res.Resources, 1)
		testutils.ProtoEquals(t, testutils.MustMarshal(t, testListener1).Resource, res.Resources[0])
		require.Equal(t, int64(2), statsHandler.ACKsReceived.Load())

		// Finally, check the NACK logic. Note that even though the subscription list hasn't changed, no
		// response is expected.
		req.VersionInfo = res.VersionInfo
		req.ResponseNonce = res.Nonce
		req.ErrorDetail = &status.Status{
			Code:    420,
			Message: "Testing NACK",
		}
		require.NoError(t, stream.Send(req))

		require.Eventually(t, func() bool {
			return statsHandler.NACKsReceived.Load() == 1
		}, 2*time.Second, 100*time.Millisecond)

		require.NoError(t, stream.Send(&ads.SotWDiscoveryRequest{
			Node:          new(core.Node),
			TypeUrl:       badTypeURL,
			ResourceNames: badResources,
		}))

		waitForResponse(t, res, stream, 10*time.Millisecond)

		require.Equal(t, badTypeURL, res.GetTypeUrl())
		require.Empty(t, res.GetResources())
		require.Equal(t, int64(1), statsHandler.UnknownTypes.Load())
	})

	// Author's note: there are no semantic differences in the way subscriptions and ACKs are managed for
	// pseudo delta SotW, so this test avoids retesting what was already tested in the previous test for
	// brevity.
	t.Run("PseudoDeltaSotW", func(t *testing.T) {
		statsHandler.reset()

		// This test relies on Bytes being a pseudo delta resource type, so fail the test early otherwise
		require.True(t, utils.IsPseudoDeltaSotW(testResource.TypeURL()))

		stream, err := client.StreamAggregatedResources(testutils.Context(t))
		require.NoError(t, err)

		req := &ads.SotWDiscoveryRequest{
			Node:          locator.node,
			TypeUrl:       testResource.TypeURL(),
			ResourceNames: []string{testResource.Name},
		}
		require.NoError(t, stream.Send(req))

		// PseudoDeltaSotW does not provide a mechanism for the server to communicate that a resource does not exist.
		// Functionally, PseudoDeltaSotW clients are expected to treat requested resources that don't arrive within a
		// given timeout to be deleted. Here we check that the server does not respond with the requested resource
		// before it's set by using a wait then checking the time at which the resource was received after it was set.
		const wait = time.Second
		time.AfterFunc(wait, func() {
			// The metric should have been updated at this point, if not, fail the test
			require.Equal(t, int64(1), statsHandler.UnknownResources.Load())
			setEntry(t)
		})
		const delta = 50 * time.Millisecond

		startWait := time.Now()
		res := new(ads.SotWDiscoveryResponse)
		waitForResponse(t, res, stream, wait+delta)
		require.WithinDuration(t, time.Now(), startWait.Add(wait), delta)
		require.Len(t, res.Resources, 1)
		testutils.ProtoEquals(t, testutils.MustMarshal(t, testResource).Resource, res.Resources[0])

		// Note that the following is technically a protocol violation. As noted above, PseudoDeltaSotW
		// cannot signal to the client that a resource does not exist, it simply never responds. However, in
		// the event that the server receives a request for a type it does not know (and never will since
		// they are not dynamic and determined at startup), since there is no way to signal that this request
		// will never be satisfied, the server will respond with an empty response.
		require.NoError(t, stream.Send(&ads.SotWDiscoveryRequest{
			Node:          new(core.Node),
			TypeUrl:       badTypeURL,
			ResourceNames: badResources,
		}))
		waitForResponse(t, res, stream, wait+delta)
		require.Equal(t, badTypeURL, res.GetTypeUrl(), prototext.Format(res))
		require.Empty(t, res.GetResources())
		require.Equal(t, int64(1), statsHandler.UnknownTypes.Load())
	})

}

type xDSResponse interface {
	proto.Message
	GetControlPlane() *core.ControlPlane
}

// waitForResponse waits for a response on the given stream, failing the test if the response does
// not arrive within the timeout or if an error is returned.
func waitForResponse(
	t *testing.T,
	res xDSResponse,
	stream interface{ RecvMsg(any) error },
	timeout time.Duration,
) {
	t.Helper()

	ch := make(chan error)
	go func() {
		ch <- stream.RecvMsg(res)
	}()

	select {
	case err := <-ch:
		require.NoError(t, err)
	case <-time.After(timeout):
		t.Fatalf("Did not receive response in %s", timeout)
	}
	testutils.ProtoEquals(t, controlPlane, res.GetControlPlane())
}

func readResourcesFromJSONFile(t *testing.T, f string) (resources []*ads.RawResource) {
	data, err := os.ReadFile(f)
	require.NoError(t, err)

	var rawResources []json.RawMessage
	require.NoError(t, json.Unmarshal(data, &rawResources))
	for _, raw := range rawResources {
		r := new(ads.RawResource)
		require.NoError(t, protojson.Unmarshal(raw, r), string(raw))
		resources = append(resources, r)
	}
	return resources
}

type simpleBatchHandler struct {
	t      *testing.T
	notify func(name string, r *ads.RawResource, metadata ads.SubscriptionMetadata)
	ch     atomic.Pointer[chan struct{}]
}

func (h *simpleBatchHandler) StartNotificationBatch() {
	ch := make(chan struct{}, 1)
	require.True(h.t, h.ch.CompareAndSwap(nil, &ch))
}

func (h *simpleBatchHandler) Notify(name string, r *ads.RawResource, metadata ads.SubscriptionMetadata) {
	h.notify(name, r, metadata)
}

func (h *simpleBatchHandler) ResourceMarshalError(name string, resource proto.Message, err error) {
	h.t.Fatalf("Unexpected resource marshal error for %q: %v\n%v", name, err, resource)
}

func (h *simpleBatchHandler) EndNotificationBatch() {
	close(*h.ch.Load())
}

func (h *simpleBatchHandler) check() {
	<-*h.ch.Swap(nil)
}

func newSotWReq(subscribe ...string) *ads.SotWDiscoveryRequest {
	return &ads.SotWDiscoveryRequest{
		ResourceNames: subscribe,
	}
}

func newDeltaReq(subscribe, unsubscribe []string) *ads.DeltaDiscoveryRequest {
	return &ads.DeltaDiscoveryRequest{
		ResourceNamesSubscribe:   subscribe,
		ResourceNamesUnsubscribe: unsubscribe,
	}
}

func TestSubscriptionManagerSubscriptions(t *testing.T) {
	const (
		r1 = "r1"
		r2 = "r2"
	)
	checkSubs := func(t *testing.T, c RawCache, h ads.RawSubscriptionHandler, wildcard, r1Sub, r2Sub bool) {
		t.Helper()
		require.Equal(t, wildcard, IsSubscribedTo(c, ads.WildcardSubscription, h), "wildcard")
		require.Equal(t, r1Sub, IsSubscribedTo(c, r1, h), r1)
		require.Equal(t, r2Sub, IsSubscribedTo(c, r2, h), r2)
	}

	newCacheAndHandler := func(t *testing.T) (Cache[*wrapperspb.BoolValue], ResourceLocator, *simpleBatchHandler) {
		tl := newTestLocator(t, nil, TypeOf[*wrapperspb.BoolValue]())
		c := getCache[*wrapperspb.BoolValue](tl)
		expected := ads.NewResource(r1, "0", wrapperspb.Bool(true))
		c.SetResource(expected, time.Time{})

		h := &simpleBatchHandler{
			t: t,
			notify: func(name string, r *ads.RawResource, metadata ads.SubscriptionMetadata) {
				if name == r1 {
					require.Same(t, testutils.MustMarshal(t, expected), r)
					b, err := r.Resource.UnmarshalNew()
					require.NoError(t, err)
					testutils.ProtoEquals(t, wrapperspb.Bool(true), b)
				} else {
					require.Nil(t, r)
				}
			},
		}

		return c, tl, h
	}

	for _, streamType := range []ads.StreamType{ads.DeltaStreamType, ads.SotWStreamType} {
		t.Run(streamType.String(), func(t *testing.T) {
			t.Run("wildcard", func(t *testing.T) {
				c, l, h := newCacheAndHandler(t)

				var sotw internal.SubscriptionManager[*ads.SotWDiscoveryRequest]
				var delta internal.SubscriptionManager[*ads.DeltaDiscoveryRequest]
				if streamType == ads.DeltaStreamType {
					delta = internal.NewDeltaSubscriptionManager(testutils.Context(t), l, c.Type().URL(), h)
				} else {
					sotw = internal.NewSotWSubscriptionManager(testutils.Context(t), l, c.Type().URL(), h)
				}

				checkSubs(t, c, h, false, false, false)

				// subscribe to the wildcard
				if streamType == ads.DeltaStreamType {
					delta.ProcessSubscriptions(newDeltaReq([]string{ads.WildcardSubscription}, nil))
				} else {
					sotw.ProcessSubscriptions(newSotWReq(ads.WildcardSubscription))
				}
				h.check()
				checkSubs(t, c, h,
					true,
					// implicit subscription to r1 via wildcard
					true,
					// implicit subscription to r2 via wildcard
					true,
				)

				// subscribe to r2, unsubscribe from wildcard
				if streamType == ads.DeltaStreamType {
					delta.ProcessSubscriptions(newDeltaReq([]string{r2}, []string{ads.WildcardSubscription}))
				} else {
					sotw.ProcessSubscriptions(newSotWReq(r2))
				}
				h.check()
				checkSubs(t, c, h,
					false,
					// because r1 was not explicitly subscribed to, its implicit subscription should also be removed
					false,
					// explicit subscription
					true,
				)
			})

			t.Run("normal", func(t *testing.T) {
				c, l, h := newCacheAndHandler(t)

				var sotw internal.SubscriptionManager[*ads.SotWDiscoveryRequest]
				var delta internal.SubscriptionManager[*ads.DeltaDiscoveryRequest]
				if streamType == ads.DeltaStreamType {
					delta = internal.NewDeltaSubscriptionManager(testutils.Context(t), l, c.Type().URL(), h)
				} else {
					sotw = internal.NewSotWSubscriptionManager(testutils.Context(t), l, c.Type().URL(), h)
				}

				// subscribe to r1 and r2
				if streamType == ads.DeltaStreamType {
					delta.ProcessSubscriptions(newDeltaReq([]string{r1, r2}, nil))
				} else {
					sotw.ProcessSubscriptions(newSotWReq(r1, r2))
				}
				h.check()
				checkSubs(t, c, h,
					false,
					true,
					true,
				)

				// unsubscribe from r2, keep r1
				if streamType == ads.DeltaStreamType {
					delta.ProcessSubscriptions(newDeltaReq(nil, []string{r2}))
				} else {
					sotw.ProcessSubscriptions(newSotWReq(r1))
				}
				h.check()
				checkSubs(t, c, h,
					false,
					true,
					// unsubscribed
					false,
				)
			})
		})
	}
}

type mockResourceLocator struct {
	isTypeSupported func(typeURL string) bool
	subscribe       func(typeURL, resourceName string) func()
	resubscribe     func(typeURL, resourceName string)
}

func (m *mockResourceLocator) IsTypeSupported(_ context.Context, typeURL string) bool {
	return m.isTypeSupported(typeURL)
}

func (m *mockResourceLocator) Subscribe(_ context.Context, typeURL, resourceName string, _ ads.RawSubscriptionHandler) func() {
	return m.subscribe(typeURL, resourceName)
}

func (m *mockResourceLocator) Resubscribe(_ context.Context, typeURL, resourceName string, _ ads.RawSubscriptionHandler) {
	m.resubscribe(typeURL, resourceName)
}

func TestImplicitWildcardSubscription(t *testing.T) {
	const foo = "foo"
	h := NewNoopBatchSubscriptionHandler(t)
	typeURL := TypeOf[*ads.Secret]().URL()

	newMockLocator := func(t *testing.T) (l *mockResourceLocator, wildcardSub, fooSub chan struct{}) {
		wildcardSub = make(chan struct{}, 1)
		fooSub = make(chan struct{}, 1)
		l = &mockResourceLocator{
			isTypeSupported: func(actualTypeURL string) bool {
				require.Equal(t, typeURL, actualTypeURL)
				return true
			},
			subscribe: func(actualTypeURL, resourceName string) func() {
				require.Equal(t, typeURL, actualTypeURL)
				switch resourceName {
				case ads.WildcardSubscription:
					wildcardSub <- struct{}{}
					return func() {
						close(wildcardSub)
					}
				case foo:
					fooSub <- struct{}{}
					return func() {
						close(fooSub)
					}
				default:
					t.Fatalf("Unexpected resource name %q", resourceName)
					return nil
				}
			},
			resubscribe: func(actualTypeURL, resourceName string) {
				switch resourceName {
				case ads.WildcardSubscription:
					wildcardSub <- struct{}{}
				case foo:
					fooSub <- struct{}{}
				default:
					t.Fatalf("Unexpected resource name %q", resourceName)
				}
			},
		}
		return l, wildcardSub, fooSub
	}
	requireSelect := func(t *testing.T, ch <-chan struct{}, shouldBeClosed bool) {
		t.Helper()
		select {
		case _, ok := <-ch:
			if ok && shouldBeClosed {
				t.Fatalf("Channel not closed")
			}
			if !ok && !shouldBeClosed {
				t.Fatalf("Channel unexpectedly closed")
			}
		default:
			t.Fatalf("empty channel!")
		}
	}

	t.Run("SotW", func(t *testing.T) {
		t.Run("empty first call", func(t *testing.T) {
			l, wildcardSub, fooSub := newMockLocator(t)
			m := internal.NewSotWSubscriptionManager(testutils.Context(t), l, typeURL, h)

			// The first call, if empty should always implicit create a wildcard subscription.
			m.ProcessSubscriptions(newSotWReq())
			requireSelect(t, wildcardSub, false)

			// Subsequent requests can ACK the previous wildcard request but not change the subscriptions and not
			// provide an explicit resource to subscribe to, in which case the wildcard should persist.
			m.ProcessSubscriptions(newSotWReq())
			require.Empty(t, wildcardSub)

			// However once a resource name is explicitly provided, the implicit wildcard should disappear.
			m.ProcessSubscriptions(newSotWReq(foo))
			requireSelect(t, wildcardSub, true)
			requireSelect(t, fooSub, false)
		})
		t.Run("non-empty first call", func(t *testing.T) {
			l, wildcardSub, fooSub := newMockLocator(t)
			m := internal.NewSotWSubscriptionManager(testutils.Context(t), l, typeURL, h)

			// If the first call isn't empty, the implicit wildcard subscription should not be present.
			m.ProcessSubscriptions(newSotWReq(foo))
			requireSelect(t, fooSub, false)
			require.Empty(t, wildcardSub)
		})
		t.Run("explicit wildcard", func(t *testing.T) {
			l, wildcardSub, fooSub := newMockLocator(t)
			m := internal.NewSotWSubscriptionManager(testutils.Context(t), l, typeURL, h)

			m.ProcessSubscriptions(newSotWReq(ads.WildcardSubscription))
			requireSelect(t, wildcardSub, false)
			require.Empty(t, fooSub)

			m.ProcessSubscriptions(newSotWReq())
			requireSelect(t, wildcardSub, true)
		})
	})
	t.Run("Delta", func(t *testing.T) {
		t.Run("empty first call", func(t *testing.T) {
			l, wildcardSub, fooSub := newMockLocator(t)
			m := internal.NewDeltaSubscriptionManager(testutils.Context(t), l, typeURL, h)

			// The first call, if empty should always implicit create a wildcard subscription.
			m.ProcessSubscriptions(newDeltaReq(nil, nil))
			requireSelect(t, wildcardSub, false)

			// Subsequent requests can ACK the previous wildcard request but not change the subscriptions and not
			// provide an explicit resource to subscribe to, in which case the wildcard should persist.
			m.ProcessSubscriptions(newDeltaReq(nil, nil))
			// However, unlike SotW, it should not resubscribe because it was not explicit.
			require.Empty(t, wildcardSub)

			// In Delta, the implicit wildcard subscription created by the first message must be explicitly
			// removed.
			m.ProcessSubscriptions(newDeltaReq([]string{foo}, nil))
			// Since there was no explicit change to the wildcard subscription, no notification is expected
			require.Empty(t, wildcardSub)
			requireSelect(t, fooSub, false)

			m.ProcessSubscriptions(newDeltaReq(nil, []string{ads.WildcardSubscription}))
			require.Empty(t, fooSub)
			requireSelect(t, wildcardSub, true)
		})
		t.Run("non-empty first call", func(t *testing.T) {
			l, wildcardSub, fooSub := newMockLocator(t)
			m := internal.NewDeltaSubscriptionManager(testutils.Context(t), l, typeURL, h)

			// If the first call isn't empty, the implicit wildcard subscription should not be present.
			m.ProcessSubscriptions(newDeltaReq([]string{foo}, nil))
			require.Empty(t, wildcardSub)
			requireSelect(t, fooSub, false)
		})
		t.Run("explicit wildcard", func(t *testing.T) {
			l, wildcardSub, fooSub := newMockLocator(t)
			m := internal.NewDeltaSubscriptionManager(testutils.Context(t), l, typeURL, h)

			m.ProcessSubscriptions(newDeltaReq([]string{ads.WildcardSubscription}, nil))
			requireSelect(t, wildcardSub, false)
			require.Empty(t, fooSub)

			m.ProcessSubscriptions(newDeltaReq(nil, []string{ads.WildcardSubscription}))
			requireSelect(t, wildcardSub, true)
			require.Empty(t, fooSub)
		})
	})
}

// batchFuncHandler the equivalent of funcHandler but for the BatchSubscriptionHandler interface.
type batchFuncHandler struct {
	t      *testing.T
	start  func()
	notify func(name string, r *ads.RawResource, metadata ads.SubscriptionMetadata)
	end    func()
}

func (b *batchFuncHandler) StartNotificationBatch() {
	b.start()
}

func (b *batchFuncHandler) Notify(name string, r *ads.RawResource, metadata ads.SubscriptionMetadata) {
	b.notify(name, r, metadata)
}

func (b *batchFuncHandler) ResourceMarshalError(name string, resource proto.Message, err error) {
	b.t.Fatalf("Unexpected resource marshal error for %q: %v\n%v", name, err, resource)
}

func (b *batchFuncHandler) EndNotificationBatch() {
	b.end()
}

func NewBatchSubscriptionHandler(
	t *testing.T,
	start func(),
	notify func(name string, r *ads.RawResource, metadata ads.SubscriptionMetadata),
	end func(),
) internal.BatchSubscriptionHandler {
	return &batchFuncHandler{
		t:      t,
		start:  start,
		notify: notify,
		end:    end,
	}
}

func NewNoopBatchSubscriptionHandler(t *testing.T) internal.BatchSubscriptionHandler {
	return NewBatchSubscriptionHandler(
		t,
		func() {}, func(string, *ads.RawResource, ads.SubscriptionMetadata) {}, func() {},
	)
}

func TestSubscriptionManagerUnsubscribeAll(t *testing.T) {
	typeURL := TypeOf[*ads.Secret]().URL()
	h := NewNoopBatchSubscriptionHandler(t)

	t.Run("explicit", func(t *testing.T) {
		const foo = "foo"

		var wg sync.WaitGroup

		l := &mockResourceLocator{
			isTypeSupported: func(string) bool { return true },
			subscribe: func(_, resourceName string) func() {
				wg.Done()
				return func() {
					wg.Done()
				}
			},
		}

		m := internal.NewDeltaSubscriptionManager(context.Background(), l, typeURL, h)

		wg.Add(2)
		m.ProcessSubscriptions(&ads.DeltaDiscoveryRequest{
			ResourceNamesSubscribe: []string{ads.WildcardSubscription, foo},
		})
		wg.Wait()

		wg.Add(2)
		m.UnsubscribeAll()
		wg.Wait()
	})

	t.Run("on context expiry", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		var wg sync.WaitGroup
		l := &mockResourceLocator{
			isTypeSupported: func(string) bool { return true },
			subscribe: func(_, _ string) func() {
				wg.Done()
				return func() {
					wg.Done()
				}
			},
		}
		m := internal.NewDeltaSubscriptionManager(ctx, l, typeURL, h)

		wg.Add(1)
		m.ProcessSubscriptions(&ads.DeltaDiscoveryRequest{
			ResourceNamesSubscribe: []string{ads.WildcardSubscription},
		})
		wg.Wait()

		wg.Add(1)
		cancel()
		wg.Wait()
	})

}
