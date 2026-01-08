package internal

import (
	"context"
	"testing"

	"github.com/envoyproxy/go-control-plane/pkg/resource/v3"
	"github.com/linkedin/diderot/ads"
	"github.com/linkedin/diderot/testutils"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// mockResourceLocator simulates the ResourceLocator interface for testing.
type mockResourceLocator func(typeURL, resourceName string, handler ads.RawSubscriptionHandler) func()

func (m mockResourceLocator) Subscribe(
	_ context.Context,
	typeURL, resourceName string,
	handler ads.RawSubscriptionHandler,
) (unsubscribe func()) {
	return m(typeURL, resourceName, handler)
}

// noopBatchSubscriptionHandler is a simple implementation of BatchSubscriptionHandler for testing.
type noopBatchSubscriptionHandler struct{}

func (n *noopBatchSubscriptionHandler) StartNotificationBatch(_ map[string]string, _ int) {}
func (n *noopBatchSubscriptionHandler) EndNotificationBatch()                             {}
func (n *noopBatchSubscriptionHandler) Notify(_ string, _ *ads.RawResource, _ ads.SubscriptionMetadata) {
}
func (n *noopBatchSubscriptionHandler) ResourceMarshalError(_ string, _ proto.Message, _ error) {}

// TestDeltaSubscriptionManager_VHDSSkipsWildcard tests that VHDS type URLs skip the implicit
// wildcard subscription on empty first request, while other types do not.
func TestDeltaSubscriptionManager_VHDSSkipsWildcard(t *testing.T) {
	const foo = "foo"
	ctx := testutils.Context(t)
	handler := &noopBatchSubscriptionHandler{}

	testCases := []struct {
		name                          string
		typeURL                       string
		shouldCreateWildcardOnEmpty   bool
		shouldCreateWildcardWithNames bool
	}{
		{
			name:                          "VHDS type skips wildcard on empty first request",
			typeURL:                       resource.VirtualHostType,
			shouldCreateWildcardOnEmpty:   false,
			shouldCreateWildcardWithNames: false,
		},
		{
			name:                          "Cluster type creates wildcard on empty first request",
			typeURL:                       resource.ClusterType,
			shouldCreateWildcardOnEmpty:   true,
			shouldCreateWildcardWithNames: false,
		},
		{
			name:                          "Endpoint type creates wildcard on empty first request",
			typeURL:                       resource.EndpointType,
			shouldCreateWildcardOnEmpty:   true,
			shouldCreateWildcardWithNames: false,
		},
		{
			name:                          "Listener type creates wildcard on empty first request",
			typeURL:                       resource.ListenerType,
			shouldCreateWildcardOnEmpty:   true,
			shouldCreateWildcardWithNames: false,
		},
		{
			name:                          "Route type creates wildcard on empty first request",
			typeURL:                       resource.RouteType,
			shouldCreateWildcardOnEmpty:   true,
			shouldCreateWildcardWithNames: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Run("empty first request", func(t *testing.T) {
				subscribedResources := make(map[string]bool)
				locator := mockResourceLocator(func(actualTypeURL, resourceName string, _ ads.RawSubscriptionHandler) func() {
					require.Equal(t, tc.typeURL, actualTypeURL, "typeURL mismatch")
					subscribedResources[resourceName] = true
					return func() {
						delete(subscribedResources, resourceName)
					}
				})

				manager := NewDeltaSubscriptionManager(ctx, locator, tc.typeURL, handler, nil)

				// First request with no resource names
				req := &ads.DeltaDiscoveryRequest{
					ResourceNamesSubscribe:   nil,
					ResourceNamesUnsubscribe: nil,
					InitialResourceVersions:  nil,
				}
				manager.ProcessSubscriptions(req)

				if tc.shouldCreateWildcardOnEmpty {
					require.True(t, subscribedResources[ads.WildcardSubscription],
						"Expected wildcard subscription for %s", tc.typeURL)
				} else {
					require.False(t, subscribedResources[ads.WildcardSubscription],
						"Did not expect wildcard subscription for %s", tc.typeURL)
				}
			})

			t.Run("non-empty first request", func(t *testing.T) {
				subscribedResources := make(map[string]bool)
				locator := mockResourceLocator(func(actualTypeURL, resourceName string, _ ads.RawSubscriptionHandler) func() {
					require.Equal(t, tc.typeURL, actualTypeURL, "typeURL mismatch")
					subscribedResources[resourceName] = true
					return func() {
						delete(subscribedResources, resourceName)
					}
				})

				manager := NewDeltaSubscriptionManager(ctx, locator, tc.typeURL, handler, nil)

				// First request with explicit resource names
				req := &ads.DeltaDiscoveryRequest{
					ResourceNamesSubscribe:   []string{foo},
					ResourceNamesUnsubscribe: nil,
					InitialResourceVersions:  nil,
				}
				manager.ProcessSubscriptions(req)

				if tc.shouldCreateWildcardWithNames {
					require.True(t, subscribedResources[ads.WildcardSubscription],
						"Expected wildcard subscription for %s with explicit names", tc.typeURL)
				} else {
					require.False(t, subscribedResources[ads.WildcardSubscription],
						"Did not expect wildcard subscription for %s with explicit names", tc.typeURL)
				}
				require.True(t, subscribedResources[foo],
					"Expected subscription to %s for %s", foo, tc.typeURL)
			})

			t.Run("subsequent empty requests", func(t *testing.T) {
				subscribedResources := make(map[string]bool)
				subscriptionCount := make(map[string]int)
				locator := mockResourceLocator(func(actualTypeURL, resourceName string, _ ads.RawSubscriptionHandler) func() {
					require.Equal(t, tc.typeURL, actualTypeURL, "typeURL mismatch")
					subscribedResources[resourceName] = true
					subscriptionCount[resourceName]++
					return func() {
						delete(subscribedResources, resourceName)
					}
				})

				manager := NewDeltaSubscriptionManager(ctx, locator, tc.typeURL, handler, nil)

				// First empty request
				req := &ads.DeltaDiscoveryRequest{
					ResourceNamesSubscribe:   nil,
					ResourceNamesUnsubscribe: nil,
					InitialResourceVersions:  nil,
				}
				manager.ProcessSubscriptions(req)

				initialWildcardCount := subscriptionCount[ads.WildcardSubscription]

				// Second empty request - should not change subscription state
				manager.ProcessSubscriptions(req)

				require.Equal(t, initialWildcardCount, subscriptionCount[ads.WildcardSubscription],
					"Wildcard subscription count should not change on subsequent empty requests")
			})
		})
	}
}

// TestDeltaSubscriptionManager_VHDSExplicitWildcard tests that VHDS can still explicitly
// subscribe to wildcard if the client requests it.
func TestDeltaSubscriptionManager_VHDSExplicitWildcard(t *testing.T) {
	ctx := testutils.Context(t)
	handler := &noopBatchSubscriptionHandler{}

	subscribedResources := make(map[string]bool)
	locator := mockResourceLocator(func(actualTypeURL, resourceName string, _ ads.RawSubscriptionHandler) func() {
		require.Equal(t, resource.VirtualHostType, actualTypeURL, "typeURL mismatch")
		subscribedResources[resourceName] = true
		return func() {
			delete(subscribedResources, resourceName)
		}
	})

	manager := NewDeltaSubscriptionManager(ctx, locator, resource.VirtualHostType, handler, nil)

	// Explicitly subscribe to wildcard
	req := &ads.DeltaDiscoveryRequest{
		ResourceNamesSubscribe:   []string{ads.WildcardSubscription},
		ResourceNamesUnsubscribe: nil,
		InitialResourceVersions:  nil,
	}
	manager.ProcessSubscriptions(req)

	require.True(t, subscribedResources[ads.WildcardSubscription],
		"Expected explicit wildcard subscription for VHDS")
}

// TestIsVHDSType tests the helper function that identifies VHDS type URLs.
func TestIsVHDSType(t *testing.T) {
	testCases := []struct {
		name     string
		typeURL  string
		expected bool
	}{
		{
			name:     "VirtualHost type is VHDS",
			typeURL:  resource.VirtualHostType,
			expected: true,
		},
		{
			name:     "Cluster type is not VHDS",
			typeURL:  resource.ClusterType,
			expected: false,
		},
		{
			name:     "Endpoint type is not VHDS",
			typeURL:  resource.EndpointType,
			expected: false,
		},
		{
			name:     "Listener type is not VHDS",
			typeURL:  resource.ListenerType,
			expected: false,
		},
		{
			name:     "Route type is not VHDS",
			typeURL:  resource.RouteType,
			expected: false,
		},
		{
			name:     "Secret type is not VHDS",
			typeURL:  resource.SecretType,
			expected: false,
		},
		{
			name:     "Runtime type is not VHDS",
			typeURL:  resource.RuntimeType,
			expected: false,
		},
		{
			name:     "Empty string is not VHDS",
			typeURL:  "",
			expected: false,
		},
		{
			name:     "Random string is not VHDS",
			typeURL:  "type.googleapis.com/random.Type",
			expected: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := isVHDSType(tc.typeURL)
			require.Equal(t, tc.expected, result,
				"isVHDSType(%q) = %v, expected %v", tc.typeURL, result, tc.expected)
		})
	}
}

// TestDeltaSubscriptionManager_FirstCallReceivedFlag tests that the firstCallReceived flag
// is properly set and only the first call triggers the implicit wildcard logic.
func TestDeltaSubscriptionManager_FirstCallReceivedFlag(t *testing.T) {
	ctx := testutils.Context(t)
	handler := &noopBatchSubscriptionHandler{}
	const foo = "foo"
	const bar = "bar"

	subscribedResources := make(map[string]bool)
	subscriptionCount := make(map[string]int)
	locator := mockResourceLocator(func(typeURL, resourceName string, _ ads.RawSubscriptionHandler) func() {
		subscribedResources[resourceName] = true
		subscriptionCount[resourceName]++
		return func() {
			delete(subscribedResources, resourceName)
		}
	})

	manager := NewDeltaSubscriptionManager(ctx, locator, resource.ClusterType, handler, nil)

	// First call: empty, should create implicit wildcard
	req1 := &ads.DeltaDiscoveryRequest{
		ResourceNamesSubscribe:   nil,
		ResourceNamesUnsubscribe: nil,
		InitialResourceVersions:  nil,
	}
	manager.ProcessSubscriptions(req1)
	require.True(t, subscribedResources[ads.WildcardSubscription],
		"First empty call should create wildcard")
	require.Equal(t, 1, subscriptionCount[ads.WildcardSubscription],
		"Wildcard should be subscribed once")

	// Second call: empty, should NOT recreate wildcard
	req2 := &ads.DeltaDiscoveryRequest{
		ResourceNamesSubscribe:   nil,
		ResourceNamesUnsubscribe: nil,
		InitialResourceVersions:  nil,
	}
	manager.ProcessSubscriptions(req2)
	require.True(t, subscribedResources[ads.WildcardSubscription],
		"Wildcard should still be subscribed")
	require.Equal(t, 1, subscriptionCount[ads.WildcardSubscription],
		"Wildcard subscription count should not increase")

	// Third call: subscribe to foo, wildcard should remain (not implicit anymore)
	req3 := &ads.DeltaDiscoveryRequest{
		ResourceNamesSubscribe:   []string{foo},
		ResourceNamesUnsubscribe: nil,
		InitialResourceVersions:  nil,
	}
	manager.ProcessSubscriptions(req3)
	require.True(t, subscribedResources[ads.WildcardSubscription],
		"Wildcard should remain after subscribing to foo")
	require.True(t, subscribedResources[foo],
		"foo should be subscribed")
	require.Equal(t, 1, subscriptionCount[ads.WildcardSubscription],
		"Wildcard subscription count should remain 1")

	// Fourth call: explicit unsubscribe from wildcard
	req4 := &ads.DeltaDiscoveryRequest{
		ResourceNamesSubscribe:   []string{bar},
		ResourceNamesUnsubscribe: []string{ads.WildcardSubscription},
		InitialResourceVersions:  nil,
	}
	manager.ProcessSubscriptions(req4)
	require.False(t, subscribedResources[ads.WildcardSubscription],
		"Wildcard should be unsubscribed")
	require.True(t, subscribedResources[foo],
		"foo should still be subscribed")
	require.True(t, subscribedResources[bar],
		"bar should be subscribed")
}
