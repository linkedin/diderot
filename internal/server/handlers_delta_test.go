package internal

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/linkedin/diderot/ads"
	"github.com/linkedin/diderot/internal/utils"
	serverstats "github.com/linkedin/diderot/stats/server"
	"github.com/linkedin/diderot/testutils"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestDeltaHandler(t *testing.T) {
	l := NewTestHandlerLimiter()

	typeURL := utils.GetTypeURL[*wrapperspb.BoolValue]()
	var lastRes *ads.DeltaDiscoveryResponse
	h := newDeltaHandler(
		testutils.Context(t),
		NoopLimiter{},
		l,
		new(customStatsHandler),
		0,
		typeURL,
		func(res *ads.DeltaDiscoveryResponse) error {
			defer l.Done()
			lastRes = res
			return nil
		},
	)

	const foo, bar = "foo", "bar"
	h.Notify(foo, nil, ignoredMetadata)
	r := new(ads.RawResource)
	h.Notify(bar, r, ignoredMetadata)

	l.Release()

	require.Equal(t, typeURL, lastRes.TypeUrl)
	require.Len(t, lastRes.Resources, 1)
	require.Equal(t, r, lastRes.Resources[0])
	require.Equal(t, []string{foo}, lastRes.RemovedResources)
}

func TestEncodedUpdateSize(t *testing.T) {
	foo := testutils.MustMarshal(t, ads.NewResource("foo", "42", new(wrapperspb.Int64Value)))
	notFoo := testutils.MustMarshal(t, ads.NewResource("notFoo", "27", new(wrapperspb.Int64Value)))

	checkSize := func(t *testing.T, msg proto.Message, size int) {
		require.Equal(t, size, proto.Size(msg))
		data, err := proto.Marshal(msg)
		require.NoError(t, err)
		require.Len(t, data, size)
	}

	ds := &deltaSender{typeURL: utils.GetTypeURL[*wrapperspb.StringValue]()}

	t.Run("add", func(t *testing.T) {
		res := ds.newChunk()
		responseSize := proto.Size(res)

		res.Resources = append(res.Resources, foo)
		responseSize += encodedUpdateSize(foo.Name, foo)
		checkSize(t, res, responseSize)

		res.Resources = append(res.Resources, notFoo)
		responseSize += encodedUpdateSize(notFoo.Name, notFoo)
		checkSize(t, res, responseSize)
	})
	t.Run("remove", func(t *testing.T) {
		res := ds.newChunk()
		responseSize := proto.Size(res)

		res.RemovedResources = append(res.RemovedResources, foo.Name)
		responseSize += encodedUpdateSize(foo.Name, nil)
		checkSize(t, res, responseSize)

		res.RemovedResources = append(res.RemovedResources, notFoo.Name)
		responseSize += encodedUpdateSize(notFoo.Name, nil)
		checkSize(t, res, responseSize)
	})
	t.Run("add and remove", func(t *testing.T) {
		res := ds.newChunk()
		responseSize := proto.Size(res)

		res.Resources = append(res.Resources, foo)
		responseSize += encodedUpdateSize(foo.Name, foo)
		checkSize(t, res, responseSize)

		res.RemovedResources = append(res.RemovedResources, notFoo.Name)
		responseSize += encodedUpdateSize(notFoo.Name, nil)
		checkSize(t, res, responseSize)
	})
}

func TestInitialChunkSize(t *testing.T) {
	typeURL := utils.GetTypeURL[*wrapperspb.StringValue]()
	require.Equal(t, proto.Size(&ads.DeltaDiscoveryResponse{
		TypeUrl: typeURL,
		Nonce:   utils.NewNonce(),
	}), initialChunkSize(typeURL))
}

func TestDeltaHandlerChunking(t *testing.T) {
	foo := testutils.MustMarshal(t, ads.NewResource("foo", "0", wrapperspb.String("foo")))
	bar := testutils.MustMarshal(t, ads.NewResource("bar", "0", wrapperspb.String("bar")))
	require.Equal(t, proto.Size(foo), proto.Size(bar))
	resourceSize := proto.Size(foo)

	typeURL := utils.GetTypeURL[*wrapperspb.StringValue]()
	statsHandler := new(customStatsHandler)
	ds := &deltaSender{
		typeURL:      typeURL,
		statsHandler: statsHandler,
		maxChunkSize: initialChunkSize(typeURL) + protobufSliceOverhead + resourceSize,
		minChunkSize: initialChunkSize(typeURL),
	}

	sentResponses := ds.chunk(map[string]entry{
		foo.Name: {Resource: foo},
		bar.Name: {Resource: bar},
	})

	require.Equal(t, len(sentResponses[0].Resources), 1)
	require.Equal(t, len(sentResponses[1].Resources), 1)
	response0 := sentResponses[0].Resources[0]
	response1 := sentResponses[1].Resources[0]

	if response0.Name == foo.Name {
		testutils.ProtoEquals(t, foo, response0)
		testutils.ProtoEquals(t, bar, response1)
	} else {
		testutils.ProtoEquals(t, bar, response0)
		testutils.ProtoEquals(t, foo, response1)
	}

	// Delete resources whose names are the same size as the resources to trip the chunker with the same conditions
	name1 := strings.Repeat("1", resourceSize)
	name2 := strings.Repeat("2", resourceSize)
	sentResponses = ds.chunk(map[string]entry{
		name1: {Resource: nil},
		name2: {Resource: nil},
	})
	require.Equal(t, len(sentResponses[0].RemovedResources), 1)
	require.Equal(t, len(sentResponses[1].RemovedResources), 1)
	require.ElementsMatch(t,
		[]string{name1, name2},
		[]string{sentResponses[0].RemovedResources[0], sentResponses[1].RemovedResources[0]},
	)

	small1, small2, small3 := "a", "b", "c"
	wayTooBig := strings.Repeat("3", 10*resourceSize)

	sentResponses = ds.chunk(map[string]entry{
		small1:    {Resource: nil},
		small2:    {Resource: nil},
		small3:    {Resource: nil},
		wayTooBig: {Resource: nil},
	})
	require.Equal(t, len(sentResponses[0].RemovedResources), 3)
	require.ElementsMatch(t, []string{small1, small2, small3}, sentResponses[0].RemovedResources)
	require.Equal(t, int64(1), statsHandler.DeltaResourcesOverMaxSize.Load())
}

type customStatsHandler struct {
	DeltaResourcesOverMaxSize atomic.Int64 `metric:",counter"`
}

func (h *customStatsHandler) HandleServerEvent(ctx context.Context, event serverstats.Event) {
	if _, ok := event.(*serverstats.ResourceOverMaxSize); ok {
		h.DeltaResourcesOverMaxSize.Add(1)
	}
}
