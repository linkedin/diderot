package ads

import (
	"testing"

	types "github.com/envoyproxy/go-control-plane/pkg/resource/v3"
	"github.com/linkedin/diderot/internal/utils"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestTypeURL(t *testing.T) {
	require.Panics(t, func() {
		utils.GetTypeURL[proto.Message]()
	})

	r := NewResource[proto.Message]("foo", "0", new(Endpoint))
	require.Equal(t, types.EndpointType, r.TypeURL())
}
