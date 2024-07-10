package utils

import (
	"testing"

	"github.com/envoyproxy/go-control-plane/pkg/resource/v3"
	"github.com/linkedin/diderot/ads"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
)

func TestGetTypeURL(t *testing.T) {
	require.Equal(t, resource.ListenerType, GetTypeURL[*ads.Listener]())
	require.Equal(t, resource.EndpointType, GetTypeURL[*ads.Endpoint]())
	require.Equal(t, resource.ClusterType, GetTypeURL[*ads.Cluster]())
	require.Equal(t, resource.RouteType, GetTypeURL[*ads.Route]())
}

func TestProtoMap(t *testing.T) {
	t.Run("good", func(t *testing.T) {
		m := map[string]string{
			"foo":   "bar",
			"baz":   "qux",
			"empty": "",
			"":      "empty",
		}
		s := MapToProto(m)
		m2, err := ProtoToMap(s)
		require.NoError(t, err)
		require.Equal(t, m, m2)

		// Check that on a different invocation, the output remains the same
		require.Equal(t, s, MapToProto(m))

		m2, err = ProtoToMap("")
		require.NoError(t, err)
		require.Empty(t, m2)
	})
	t.Run("bad", func(t *testing.T) {
		_, err := ProtoToMap("1")
		require.Error(t, err)

		b := protowire.AppendString(nil, "foo")
		_, err = ProtoToMap(string(b))
		require.Error(t, err)
	})
}

func TestNonceLength(t *testing.T) {
	require.Len(t, NewNonce(), NonceLength)
}
