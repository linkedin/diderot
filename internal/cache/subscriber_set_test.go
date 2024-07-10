package internal

import (
	"testing"
	"time"

	"github.com/linkedin/diderot/ads"
	"github.com/stretchr/testify/require"
	. "google.golang.org/protobuf/types/known/timestamppb"
)

type noopHandler byte

func (*noopHandler) Notify(string, *ads.Resource[*Timestamp], ads.SubscriptionMetadata) {}

type iterateArgs struct {
	handler      ads.SubscriptionHandler[*Timestamp]
	subscribedAt time.Time
}

func checkIterate(t *testing.T, m *SubscriberSet[*Timestamp], expectedV SubscriberSetVersion, expectedArgs ...iterateArgs) {
	require.Equal(t, m.Size(), len(expectedArgs))
	seq, v := m.Iterator()
	require.Equal(t, expectedV, v)

	var actualArgs []iterateArgs

	for handler, subscribedAt := range seq {
		actualArgs = append(actualArgs, iterateArgs{
			handler:      handler,
			subscribedAt: subscribedAt,
		})
	}
	require.ElementsMatch(t, expectedArgs, actualArgs)
}

func TestSubscriberMap(t *testing.T) {
	s := new(SubscriberSet[*Timestamp])
	checkIterate(t, s, 0)

	h1 := new(noopHandler)
	sAt1, v := s.Subscribe(h1)
	require.Equal(t, SubscriberSetVersion(1), v)
	require.True(t, s.IsSubscribed(h1))

	checkIterate(t, s, 1,
		iterateArgs{
			handler:      h1,
			subscribedAt: sAt1,
		},
	)

	h2 := new(noopHandler)
	sAt2, v := s.Subscribe(h2)
	require.NotEqual(t, sAt1, sAt2)
	require.Equal(t, SubscriberSetVersion(2), v)
	require.True(t, s.IsSubscribed(h2))

	checkIterate(t, s, 2,
		iterateArgs{
			handler:      h1,
			subscribedAt: sAt1,
		},
		iterateArgs{
			handler:      h2,
			subscribedAt: sAt2,
		},
	)

	sAt3, v := s.Subscribe(h1)
	require.NotEqual(t, sAt1, sAt3)
	require.Equal(t, SubscriberSetVersion(3), v)
	require.True(t, s.IsSubscribed(h1))

	checkIterate(t, s, 3,
		iterateArgs{
			handler:      h2,
			subscribedAt: sAt2,
		},
		iterateArgs{
			handler:      h1,
			subscribedAt: sAt3,
		},
	)

	s.Unsubscribe(h1)
	require.False(t, s.IsSubscribed(h1))
	checkIterate(t, s, 3,
		iterateArgs{
			handler:      h2,
			subscribedAt: sAt2,
		})

	s.Unsubscribe(h2)
	require.False(t, s.IsSubscribed(h2))
	checkIterate(t, s, 3)
}
