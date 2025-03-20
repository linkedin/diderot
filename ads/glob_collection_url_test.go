package ads

import (
	"testing"

	cluster "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	endpoint "github.com/envoyproxy/go-control-plane/envoy/config/endpoint/v3"
	listener "github.com/envoyproxy/go-control-plane/envoy/config/listener/v3"
	route "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"
	"github.com/envoyproxy/go-control-plane/pkg/resource/v3"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

const (
	resourceType = "google.protobuf.Int64Value"
)

func testBadURIs(t *testing.T, parser func(string) (GlobCollectionURL, error)) {
	badURIs := []struct {
		name         string
		resourceName string
	}{
		{
			name:         "empty name",
			resourceName: "",
		},
		{
			name:         "invalid prefix",
			resourceName: "https://foo/bar",
		},
		{
			name:         "wrong type",
			resourceName: "xdstp://auth/some.other.type/foo",
		},
		{
			name:         "empty id",
			resourceName: "xdstp://auth/google.protobuf.Int64Value",
		},
		{
			name:         "empty id trailing slash",
			resourceName: "xdstp://auth/google.protobuf.Int64Value/",
		},
		{
			name:         "invalid query",
			resourceName: "xdstp://auth/google.protobuf.Int64Value/foo?asd",
		},
	}

	for _, test := range badURIs {
		t.Run(test.name, func(t *testing.T) {
			_, err := parser(test.resourceName)
			require.Error(t, err)
		})
	}
}

func testGoodURIs(t *testing.T, id string, parser func(string) (GlobCollectionURL, error)) {
	tests := []struct {
		name         string
		resourceName string
		expected     GlobCollectionURL
		expectErr    bool
	}{
		{
			name:         "standard",
			resourceName: "xdstp://auth/google.protobuf.Int64Value/foo/" + id,
			expected: GlobCollectionURL{
				Authority:         "auth",
				ResourceType:      resourceType,
				Path:              "foo",
				ContextParameters: "",
			},
		},
		{
			name:         "empty authority",
			resourceName: "xdstp:///google.protobuf.Int64Value/foo/" + id,
			expected: GlobCollectionURL{
				Authority:         "",
				ResourceType:      resourceType,
				Path:              "foo",
				ContextParameters: "",
			},
		},
		{
			name:         "nested",
			resourceName: "xdstp://auth/google.protobuf.Int64Value/foo/bar/baz/" + id,
			expected: GlobCollectionURL{
				Authority:         "auth",
				ResourceType:      resourceType,
				Path:              "foo/bar/baz",
				ContextParameters: "",
			},
		},
		{
			name:         "with query",
			resourceName: "xdstp://auth/google.protobuf.Int64Value/foo/" + id + "?asd=123",
			expected: GlobCollectionURL{
				Authority:         "auth",
				ResourceType:      resourceType,
				Path:              "foo",
				ContextParameters: "?asd=123",
			},
		},
		{
			name:         "with unsorted query",
			resourceName: "xdstp://auth/google.protobuf.Int64Value/foo/" + id + "?b=2&a=1",
			expected: GlobCollectionURL{
				Authority:         "auth",
				ResourceType:      resourceType,
				Path:              "foo",
				ContextParameters: "?a=1&b=2",
			},
		},
		{
			name:         "empty query",
			resourceName: "xdstp://auth/google.protobuf.Int64Value/foo/" + id + "?",
			expected: GlobCollectionURL{
				Authority:         "auth",
				ResourceType:      resourceType,
				Path:              "foo",
				ContextParameters: "",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual, err := parser(test.resourceName)
			if test.expectErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.expected, actual)
			}
		})
	}
}

func TestParseGlobCollectionURL(t *testing.T) {
	genericParser := ParseGlobCollectionURL[*wrapperspb.Int64Value]
	typeURL := getTrimmedTypeURL[*wrapperspb.Int64Value]()
	rawParser := func(s string) (GlobCollectionURL, error) {
		return RawParseGlobCollectionURL(typeURL, s)
	}

	t.Run("bad URIs", func(t *testing.T) {
		testBadURIs(t, genericParser)
		testBadURIs(t, rawParser)
	})
	t.Run("good URIs", func(t *testing.T) {
		testGoodURIs(t, WildcardSubscription, genericParser)
		testGoodURIs(t, WildcardSubscription, rawParser)
	})
	t.Run("rejects URNs", func(t *testing.T) {
		_, err := ParseGlobCollectionURL[*wrapperspb.Int64Value]("xdstp:///" + resourceType + "/foo/bar")
		require.Error(t, err)
		_, err = RawParseGlobCollectionURL(typeURL, "xdstp:///"+resourceType+"/foo/bar")
		require.Error(t, err)
	})
}

func TestParseGlobCollectionURN(t *testing.T) {
	genericParser := func(s string) (GlobCollectionURL, error) {
		gcURL, _, err := ParseGlobCollectionURN[*wrapperspb.Int64Value](s)
		return gcURL, err
	}
	typeURL := getTrimmedTypeURL[*wrapperspb.Int64Value]()
	rawParser := func(s string) (GlobCollectionURL, error) {
		gcURL, _, err := RawParseGlobCollectionURN(typeURL, s)
		return gcURL, err
	}

	t.Run("bad URIs", func(t *testing.T) {
		testBadURIs(t, genericParser)
		testBadURIs(t, rawParser)
	})
	t.Run("good URIs", func(t *testing.T) {
		testGoodURIs(t, "foo", genericParser)
		testGoodURIs(t, "foo", rawParser)
	})
	t.Run("handles glob collection URLs", func(t *testing.T) {
		gcURL, r, err := ParseGlobCollectionURN[*wrapperspb.Int64Value]("xdstp:///" + resourceType + "/foo/*")
		require.NoError(t, err)
		require.Equal(t, NewGlobCollectionURL[*wrapperspb.Int64Value]("", "foo", nil), gcURL)
		require.Equal(t, WildcardSubscription, r)

		gcURL, r, err = RawParseGlobCollectionURN(typeURL, "xdstp:///"+resourceType+"/foo/*")
		require.NoError(t, err)
		require.Equal(t, RawNewGlobCollectionURL("", typeURL, "foo", nil), gcURL)
		require.Equal(t, WildcardSubscription, r)
	})
}

func TestGetTrimmedTypeURL(t *testing.T) {
	check := func(expected, actualTrimmed string) {
		require.Equal(t, expected, resource.APITypePrefix+actualTrimmed)
	}
	check(resource.ListenerType, getTrimmedTypeURL[*listener.Listener]())
	check(resource.EndpointType, getTrimmedTypeURL[*endpoint.ClusterLoadAssignment]())
	check(resource.ClusterType, getTrimmedTypeURL[*cluster.Cluster]())
	check(resource.RouteType, getTrimmedTypeURL[*route.RouteConfiguration]())
}

func BenchmarkGetTrimmedTypeURL(b *testing.B) {
	benchmarkGetTrimmedTypeURL[*wrapperspb.Int64Value](b)
	benchmarkGetTrimmedTypeURL[*cluster.Cluster](b)
}

func benchmarkGetTrimmedTypeURL[T proto.Message](b *testing.B) {
	b.Run(getTrimmedTypeURL[T](), func(b *testing.B) {
		var url string
		for range b.N {
			url = getTrimmedTypeURL[T]()
		}
		require.NotEmpty(b, url)
	})
}

// This benchmark validates that using reflection on the protobuf type does not incur any unexpected
// costs. The most expensive operation is currently [url.Parse], which can't easily be removed. It is
// critical that this operation remain as inexpensive as possible because it is called repeatedly in
// the cache (for each insertion or deletion). Current results:
//
//	goos: darwin
//	goarch: amd64
//	pkg: github.com/linkedin/diderot/ads
//	cpu: VirtualApple @ 2.50GHz
//	                                                                                           │   results   │
//	                                                                                           │   sec/op    │
//	ParseGlobCollectionURN/xdstp://foo/google.protobuf.Int64Value/bar/*/generic-8                624.2n ± 3%
//	ParseGlobCollectionURN/xdstp://foo/google.protobuf.Int64Value/bar/*/raw-8                    610.4n ± 0%
//	ParseGlobCollectionURN/xdstp://foo/google.protobuf.Int64Value/bar/*/raw_with_prefix-8        609.6n ± 0%
//	ParseGlobCollectionURN/xdstp://foo/envoy.config.cluster.v3.Cluster/bar/*/generic-8           697.1n ± 5%
//	ParseGlobCollectionURN/xdstp://foo/envoy.config.cluster.v3.Cluster/bar/*/raw-8               692.4n ± 1%
//	ParseGlobCollectionURN/xdstp://foo/envoy.config.cluster.v3.Cluster/bar/*/raw_with_prefix-8   691.5n ± 0%
//	geomean                                                                                      653.0n
//
//	                                                                                           │  results   │
//	                                                                                           │    B/op    │
//	ParseGlobCollectionURN/xdstp://foo/google.protobuf.Int64Value/bar/*/generic-8                192.0 ± 0%
//	ParseGlobCollectionURN/xdstp://foo/google.protobuf.Int64Value/bar/*/raw-8                    192.0 ± 0%
//	ParseGlobCollectionURN/xdstp://foo/google.protobuf.Int64Value/bar/*/raw_with_prefix-8        192.0 ± 0%
//	ParseGlobCollectionURN/xdstp://foo/envoy.config.cluster.v3.Cluster/bar/*/generic-8           240.0 ± 0%
//	ParseGlobCollectionURN/xdstp://foo/envoy.config.cluster.v3.Cluster/bar/*/raw-8               240.0 ± 0%
//	ParseGlobCollectionURN/xdstp://foo/envoy.config.cluster.v3.Cluster/bar/*/raw_with_prefix-8   240.0 ± 0%
//	geomean                                                                                      214.7
//
//	                                                                                           │  results   │
//	                                                                                           │ allocs/op  │
//	ParseGlobCollectionURN/xdstp://foo/google.protobuf.Int64Value/bar/*/generic-8                2.000 ± 0%
//	ParseGlobCollectionURN/xdstp://foo/google.protobuf.Int64Value/bar/*/raw-8                    2.000 ± 0%
//	ParseGlobCollectionURN/xdstp://foo/google.protobuf.Int64Value/bar/*/raw_with_prefix-8        2.000 ± 0%
//	ParseGlobCollectionURN/xdstp://foo/envoy.config.cluster.v3.Cluster/bar/*/generic-8           3.000 ± 0%
//	ParseGlobCollectionURN/xdstp://foo/envoy.config.cluster.v3.Cluster/bar/*/raw-8               3.000 ± 0%
//	ParseGlobCollectionURN/xdstp://foo/envoy.config.cluster.v3.Cluster/bar/*/raw_with_prefix-8   3.000 ± 0%
//	geomean                                                                                      2.449
func BenchmarkParseGlobCollectionURN(b *testing.B) {
	benchmarkParseGlobCollectionURN[*wrapperspb.Int64Value](b)
	benchmarkParseGlobCollectionURN[*cluster.Cluster](b)
}

func benchmarkParseGlobCollectionURN[T proto.Message](b *testing.B) {
	expectedURL := NewGlobCollectionURL[T]("foo", "bar", nil)
	url := expectedURL.String()

	run := func(b *testing.B, f func() (GlobCollectionURL, string, error)) {
		var actualURL GlobCollectionURL
		var err error
		for range b.N {
			actualURL, _, err = f()
			if err != nil {
				b.Fatal(err)
			}
		}
		require.Equal(b, expectedURL, actualURL)
	}

	b.Run(url, func(b *testing.B) {
		b.Run("generic", func(b *testing.B) {
			run(b, func() (GlobCollectionURL, string, error) {
				return ParseGlobCollectionURN[T](url)
			})
		})

		typeURL := getTrimmedTypeURL[T]()
		b.Run("raw", func(b *testing.B) {
			run(b, func() (GlobCollectionURL, string, error) {
				return RawParseGlobCollectionURN(typeURL, url)
			})
		})

		prefixedTypeURL := getTrimmedTypeURL[T]()
		b.Run("raw with prefix", func(b *testing.B) {
			run(b, func() (GlobCollectionURL, string, error) {
				return RawParseGlobCollectionURN(prefixedTypeURL, url)
			})
		})
	})
}
