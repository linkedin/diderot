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
	t.Run("bad URIs", func(t *testing.T) {
		testBadURIs(t, ParseGlobCollectionURL[*wrapperspb.Int64Value])
	})
	t.Run("good URIs", func(t *testing.T) {
		testGoodURIs(t, WildcardSubscription, ParseGlobCollectionURL[*wrapperspb.Int64Value])
	})
	t.Run("rejects URNs", func(t *testing.T) {
		_, err := ParseGlobCollectionURL[*wrapperspb.Int64Value]("xdstp:///" + resourceType + "/foo/bar")
		require.Error(t, err)
	})
}

func TestParseGlobCollectionURN(t *testing.T) {
	parser := func(s string) (GlobCollectionURL, error) {
		gcURL, _, err := ParseGlobCollectionURN[*wrapperspb.Int64Value](s)
		return gcURL, err
	}

	t.Run("bad URIs", func(t *testing.T) {
		testBadURIs(t, parser)
	})
	t.Run("good URIs", func(t *testing.T) {
		testGoodURIs(t, "foo", parser)
	})
	t.Run("handles glob collection URLs", func(t *testing.T) {
		gcURL, r, err := ParseGlobCollectionURN[*wrapperspb.Int64Value]("xdstp:///" + resourceType + "/foo/*")
		require.NoError(t, err)
		require.Equal(t, NewGlobCollectionURL[*wrapperspb.Int64Value]("", "foo", nil), gcURL)
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

func BenchmarkParseGlobCollectionURN(b *testing.B) {
	benchmarkParseGlobCollectionURN[*wrapperspb.Int64Value](b)
	benchmarkParseGlobCollectionURN[*cluster.Cluster](b)
}

func benchmarkParseGlobCollectionURN[T proto.Message](b *testing.B) {
	expectedURL := NewGlobCollectionURL[T]("foo", "bar", nil)
	url := expectedURL.String()

	var err error
	b.Run(url, func(b *testing.B) {
		var actualURL GlobCollectionURL
		for range b.N {
			actualURL, _, err = ParseGlobCollectionURN[T](url)
			if err != nil {
				b.Fatal(err)
			}
		}
		require.Equal(b, expectedURL, actualURL)
	})
}
