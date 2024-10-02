package utils

import (
	"fmt"
	"testing"
	"time"

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

func TestNewNonce(t *testing.T) {
	now := time.Now()
	t.Run("remainingChunks", func(t *testing.T) {
		for _, expected := range []int{0, 42} {
			nonce := newNonce(now, expected)
			require.Equal(t, fmt.Sprintf("%x%08x", now.UnixNano(), expected), nonce)
			actualRemainingChunks, err := ads.ParseRemainingChunksFromNonce(nonce)
			require.NoError(t, err)
			require.Equal(t, expected, actualRemainingChunks)
		}
	})
	t.Run("badNonce", func(t *testing.T) {
		remaining, err := ads.ParseRemainingChunksFromNonce("foo")
		require.Error(t, err)
		require.Zero(t, remaining)
	})
	t.Run("oldNonce", func(t *testing.T) {
		remaining, err := ads.ParseRemainingChunksFromNonce(fmt.Sprintf("%x", now.UnixNano()))
		require.Error(t, err)
		require.Zero(t, remaining)
	})
}
