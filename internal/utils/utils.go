package utils

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"slices"
	"strings"
	"time"

	types "github.com/envoyproxy/go-control-plane/pkg/resource/v3"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

const (
	// NonceLength is the length of the string returned by NewNonce. NewNonce encodes the current UNIX
	// time in nanos and the remaining chunks, encoded as 64-bit and 32-bit integers respectively, then
	// hex encoded. This means a nonce will always be 8 + 4 bytes, multiplied by 2 by the hex encoding.
	NonceLength = (8 + 4) * 2
)

// NewNonce creates a new unique nonce based on the current UNIX time in nanos, always returning a
// string of [NonceLength].
func NewNonce(remainingChunks int) string {
	return newNonce(time.Now(), remainingChunks)
}

func newNonce(now time.Time, remainingChunks int) string {
	// preallocating these buffers with constants (instead of doing `out = make([]byte, len(buf) * 2)`)
	// means the compiler will allocate them on the stack, instead of heap. This significantly reduces
	// the amount of garbage created by this function, as the only heap allocation will be the final
	// string(out), rather than all of these buffers.
	buf := make([]byte, NonceLength/2)
	out := make([]byte, NonceLength)

	binary.BigEndian.PutUint64(buf[:8], uint64(now.UnixNano()))
	binary.BigEndian.PutUint32(buf[8:], uint32(remainingChunks))

	hex.Encode(out, buf)

	return string(out)
}

func GetTypeURL[T proto.Message]() string {
	var t T
	return getTypeURL(t)
}

func getTypeURL(t proto.Message) string {
	return types.APITypePrefix + string(t.ProtoReflect().Descriptor().FullName())
}

// MapToProto serializes the given map using protobuf. It sorts the entries based on the key such that the same map
// always produces the same output. It then encodes the entries by appending the key then value, and b64 encodes the
// entire output. Note that the final b64 encoding step is critical as this function is intended to be used with
// [ads.SotWDiscoveryResponse.Version], which is a string field. In protobuf, string fields must contain valid UTF-8
// characters, and b64 encoding ensures that.
func MapToProto(m map[string]string) string {
	if len(m) == 0 {
		return ""
	}

	var b []byte
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)

	for _, k := range keys {
		b = protowire.AppendString(b, k)
		b = protowire.AppendString(b, m[k])
	}
	return base64.StdEncoding.EncodeToString(b)
}

// ProtoToMap is the inverse of MapToProto. It returns an error on any decoding or deserialization issues.
func ProtoToMap(s string) (map[string]string, error) {
	if s == "" {
		return nil, nil
	}

	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return nil, err
	}
	m := make(map[string]string)

	parse := func() (string, error) {
		s, n := protowire.ConsumeString(b)
		if n < 0 {
			return "", protowire.ParseError(n)
		}
		b = b[n:]
		return s, nil
	}

	for len(b) > 0 {
		k, err := parse()
		if err != nil {
			return nil, err
		}
		v, err := parse()
		if err != nil {
			return nil, err
		}
		m[k] = v
	}

	return m, nil
}

// IsPseudoDeltaSotW checks whether the given resource type url is intended to behave as a "pseudo
// delta" resource. Instead of sending the entire state of the world for every resource change, the
// server is expected to only send the changed resource. From [the spec]:
//
//	In the SotW protocol variants, all resource types except for Listener and Cluster are grouped into
//	responses in the same way as in the incremental protocol variants. However, Listener and Cluster
//	resource types are handled differently: the server must include the complete state of the world,
//	meaning that all resources of the relevant type that are needed by the client must be included,
//	even if they did not change since the last response.
//
// In other words, for everything except Listener and Cluster, the server should only send the
// changed resources, rather than every resource every time.
//
// [the spec]: https://www.envoyproxy.io/docs/envoy/latest/api-docs/xds_protocol#grouping-resources-into-responses
func IsPseudoDeltaSotW(typeURL string) bool {
	return !(typeURL == types.ListenerType || typeURL == types.ClusterType)
}

// TrimTypeURL removes the leading "types.googleapis.com/" prefix from the given string.
func TrimTypeURL(typeURL string) string {
	return strings.TrimPrefix(typeURL, types.APITypePrefix)
}
