package testutils

import (
	"context"
	"maps"
	"net"
	"slices"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/linkedin/diderot/ads"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"
)

func WithTimeout(t *testing.T, name string, timeout time.Duration, f func(t *testing.T)) {
	t.Run(name, func(t *testing.T) {
		t.Helper()
		done := make(chan struct{})
		go func() {
			f(t)
			close(done)
		}()
		timer := time.NewTimer(timeout)
		defer timer.Stop()
		select {
		case <-timer.C:
			t.Fatalf("%q failed to complete in %s", t.Name(), timeout)
		case <-done:
			return
		}
	})
}

func Context(tb testing.TB) context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	tb.Cleanup(cancel)
	return ctx
}

func ContextWithTimeout(tb testing.TB, timeout time.Duration) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	tb.Cleanup(cancel)
	return ctx
}

type Notification[T proto.Message] struct {
	Name     string
	Resource *ads.Resource[T]
	Metadata ads.SubscriptionMetadata
}

type ChanSubscriptionHandler[T proto.Message] chan Notification[T]

func (c ChanSubscriptionHandler[T]) Notify(name string, r *ads.Resource[T], metadata ads.SubscriptionMetadata) {
	c <- Notification[T]{
		Name:     name,
		Resource: r,
		Metadata: metadata,
	}
}

// This is the bare minimum required by the testify framework. *testing.T implements it, but this
// interface is used for testing the test framework.
type testingT interface {
	Logf(format string, args ...any)
	Errorf(format string, args ...any)
	FailNow()
	Helper()
	Fatalf(string, ...any)
}

var _ testingT = (*testing.T)(nil)
var _ testingT = (*testing.B)(nil)

type ExpectedNotification[T proto.Message] struct {
	Name     string
	Resource *ads.Resource[T]
}

func ExpectDelete[T proto.Message](name string) ExpectedNotification[T] {
	return ExpectedNotification[T]{Name: name}
}

func ExpectUpdate[T proto.Message](r *ads.Resource[T]) ExpectedNotification[T] {
	return ExpectedNotification[T]{Name: r.Name, Resource: r}
}

func (c ChanSubscriptionHandler[T]) WaitForDelete(
	t testingT,
	expectedName string,
) Notification[T] {
	t.Helper()
	return c.WaitForNotifications(t, ExpectDelete[T](expectedName))[0]
}

func (c ChanSubscriptionHandler[T]) WaitForUpdate(t testingT, r *ads.Resource[T]) Notification[T] {
	t.Helper()
	return c.WaitForNotifications(t, ExpectUpdate(r))[0]
}

func (c ChanSubscriptionHandler[T]) WaitForNotifications(t testingT, notifications ...ExpectedNotification[T]) (out []Notification[T]) {
	t.Helper()

	expectedNotifications := make(map[string]int)
	for i, n := range notifications {
		expectedNotifications[n.Name] = i
	}

	out = make([]Notification[T], len(notifications))

	for range notifications {
		var n Notification[T]
		select {
		case n = <-c:
		case <-time.After(5 * time.Second):
			t.Fatalf("Did not receive expected notification for one of: %v",
				slices.Collect(maps.Keys(expectedNotifications)))
		}

		idx, ok := expectedNotifications[n.Name]
		if !ok {
			require.Fail(t, "Received unexpected notification", n.Name)
		}
		expected := notifications[idx]
		out[idx] = n
		delete(expectedNotifications, n.Name)

		if expected.Resource != nil {
			require.NotNilf(t, n.Resource, "Expected update for %q, got deletion instead", expected.Name)
			ResourceEquals(t, expected.Resource, n.Resource)
		} else {
			require.Nilf(t, n.Resource, "Expected delete for %q, got update instead", expected.Name)
		}
	}

	require.Empty(t, expectedNotifications)

	return out
}

func ResourceEquals[T proto.Message](t testingT, expected, actual *ads.Resource[T]) {
	t.Helper()
	require.Equal(t, expected.Name, actual.Name)
	require.Equal(t, expected.Version, actual.Version)
	ProtoEquals(t, expected.Resource, actual.Resource)
	ProtoEquals(t, expected.Ttl, actual.Ttl)
	ProtoEquals(t, expected.CacheControl, actual.CacheControl)
	ProtoEquals(t, expected.Metadata, actual.Metadata)
}

func ProtoEquals(t testingT, expected, actual proto.Message) {
	t.Helper()
	if !proto.Equal(expected, actual) {
		t.Fatalf(
			"Messages not equal:\nexpected:%s\nactual  :%s\n%s",
			expected, actual,
			cmp.Diff(prototext.Format(expected), prototext.Format(actual)),
		)
	}
}

// FuncHandler is a SubscriptionHandler implementation that simply invokes a function. Note that the usual pattern of
// having a literal func type implement the interface (e.g. http.HandlerFunc) does not work in this case because funcs
// are not hashable and therefore cannot be used as map keys, which is often how SubscriptionHandlers are used.
type FuncHandler[T proto.Message] struct {
	notify func(name string, r *ads.Resource[T], metadata ads.SubscriptionMetadata)
}

func (f *FuncHandler[T]) Notify(name string, r *ads.Resource[T], metadata ads.SubscriptionMetadata) {
	f.notify(name, r, metadata)
}

// NewSubscriptionHandler returns a SubscriptionHandler that invokes the given function when
// SubscriptionHandler.Notify is invoked.
func NewSubscriptionHandler[T proto.Message](
	notify func(name string, r *ads.Resource[T], metadata ads.SubscriptionMetadata),
) *FuncHandler[T] {
	return &FuncHandler[T]{
		notify: notify,
	}
}

type RawFuncHandler struct {
	t      testingT
	notify func(name string, r *ads.RawResource, metadata ads.SubscriptionMetadata)
}

func (r *RawFuncHandler) Notify(name string, raw *ads.RawResource, metadata ads.SubscriptionMetadata) {
	r.notify(name, raw, metadata)
}

func (r *RawFuncHandler) ResourceMarshalError(name string, resource proto.Message, err error) {
	r.t.Fatalf("Unexpected resource marshal error for %q: %v\n%v", name, err, resource)
}

// NewRawSubscriptionHandler returns a RawSubscriptionHandler that invokes the given function when
// SubscriptionHandler.Notify is invoked.
func NewRawSubscriptionHandler(
	t testingT,
	notify func(name string, r *ads.RawResource, metadata ads.SubscriptionMetadata),
) *RawFuncHandler {
	return &RawFuncHandler{t: t, notify: notify}
}

// TestServer is instantiated with NewTestGRPCServer and serves to facilitate local testing against
// gRPC service implementations.
type TestServer struct {
	t *testing.T
	*grpc.Server
	net.Listener
}

// Start starts the backing gRPC server in a goroutine. Must be invoked _after_ registering the services.
func (ts *TestServer) Start() {
	go func() {
		require.NoError(ts.t, ts.Server.Serve(ts.Listener))
	}()
}

// Dial invokes DialContext with the given options and a context generated using Context.
func (ts *TestServer) Dial(opts ...grpc.DialOption) *grpc.ClientConn {
	opts = append([]grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}, opts...)
	conn, err := grpc.NewClient(ts.AddrString(), opts...)
	require.NoError(ts.t, err)
	return conn
}

func (ts *TestServer) AddrString() string {
	return ts.Addr().String()
}

// NewTestGRPCServer is a utility function that spins up a TCP listener on a random local port along
// with a grpc.Server. It cleans up any associated state using the Cleanup methods. Sample usage is
// as follows:
//
//	ts := NewTestGRPCServer(t)
//	discovery.RegisterAggregatedDiscoveryServiceServer(ts.Server, s)
//	ts.Start()
//	conn := ts.Dial()
func NewTestGRPCServer(t *testing.T, opts ...grpc.ServerOption) *TestServer {
	ts := &TestServer{
		t:      t,
		Server: grpc.NewServer(opts...),
	}

	var err error
	ts.Listener, err = net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	t.Cleanup(func() {
		ts.Server.Stop()
	})

	return ts
}

func MustMarshal[T proto.Message](t testingT, r *ads.Resource[T]) *ads.RawResource {
	marshaled, err := r.Marshal()
	require.NoError(t, err)
	return marshaled
}
