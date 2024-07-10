package ads

import (
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	resourceType = "google.protobuf.Int64Value"
)

func testBadURIs(t *testing.T, parser func(string, string) (GlobCollectionURL, error)) {
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
			_, err := parser(test.resourceName, resourceType)
			require.Error(t, err)
		})
	}
}

func testGoodURIs(t *testing.T, id string, parser func(string, string) (GlobCollectionURL, error)) {
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
			actual, err := parser(test.resourceName, resourceType)
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
		testBadURIs(t, ParseGlobCollectionURL)
	})
	t.Run("good URIs", func(t *testing.T) {
		testGoodURIs(t, WildcardSubscription, ParseGlobCollectionURL)
	})
	t.Run("rejects URNs", func(t *testing.T) {
		_, err := ParseGlobCollectionURL("xdstp:///"+resourceType+"/foo/bar", resourceType)
		require.Error(t, err)
	})
}

func TestExtractGlobCollectionURLFromResourceURN(t *testing.T) {
	t.Run("bad URIs", func(t *testing.T) {
		testBadURIs(t, ExtractGlobCollectionURLFromResourceURN)
	})
	t.Run("good URIs", func(t *testing.T) {
		testGoodURIs(t, "foo", ExtractGlobCollectionURLFromResourceURN)
	})
	t.Run("rejects glob collection URLs", func(t *testing.T) {
		_, err := ExtractGlobCollectionURLFromResourceURN("xdstp:///"+resourceType+"/foo/*", resourceType)
		require.Error(t, err)
	})
}
