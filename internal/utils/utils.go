package utils

import (
	"encoding/base64"
	"slices"
	"strconv"
	"strings"
	"time"

	types "github.com/envoyproxy/go-control-plane/pkg/resource/v3"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

// NonceLength is the length of the string returned by NewNonce. NewNonce encodes the current UNIX
// time in nanos in hex encoding, so the nonce will be 16 characters if the current UNIX nano time is
// greater than 2^60-1. This is because it takes 16 hex characters to encode 64 bits, but only 15 to
// encode 60 bits (the output of strconv.FormatInt is not padded by 0s). 2^60-1 nanos from epoch time
// (January 1st 1970) is 2006-07-14 23:58:24.606, which as of this writing is over 17 years ago. This
// is why it's guaranteed that NonceLength will be 16 characters (before that date, encoding the
// nanos only required 15 characters). For the curious, the UNIX nano timestamp will overflow int64
// some time in 2262, making this constant valid for the next few centuries.
const NonceLength = 16

// NewNonce creates a new unique nonce based on the current UNIX time in nanos. It always returns a
// string of length NonceLength.
func NewNonce() string {
	// The second parameter to FormatInt is the base, e.g. 2 will return binary, 8 will return octal
	// encoding, etc. 16 means FormatInt returns the integer in hex encoding, e.g. 30 => "1e" or
	// 1704239351400 => "18ccc94c668".
	const hexBase = 16
	return strconv.FormatInt(time.Now().UnixNano(), hexBase)
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
