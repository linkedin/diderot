package testutils

import (
	"testing"
	"time"

	"github.com/linkedin/diderot/ads"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

var failNowInvoked = new(byte)

type testingTMock testing.T

func (t *testingTMock) Errorf(format string, args ...any) {
	(*testing.T)(t).Logf(format, args...)
}

func (t *testingTMock) Fatalf(format string, args ...any) {
	(*testing.T)(t).Logf(format, args...)
	t.FailNow()
}

func (t *testingTMock) FailNow() {
	panic(failNowInvoked)
}

func (t *testingTMock) Helper() {
}

func TestChanSubscriptionHandler_WaitForNotification(t *testing.T) {
	const foo = "foo"

	expected := wrapperspb.Int64(42)
	version := "0"
	resource := &ads.Resource[*wrapperspb.Int64Value]{
		Name:     foo,
		Version:  version,
		Resource: expected,
	}

	tests := []struct {
		name       string
		shouldFail bool
		test       func(mock *testingTMock, h ChanSubscriptionHandler[*wrapperspb.Int64Value])
	}{
		{
			name:       "receive different name",
			shouldFail: true,
			test: func(mock *testingTMock, h ChanSubscriptionHandler[*wrapperspb.Int64Value]) {
				metadata := ads.SubscriptionMetadata{SubscribedAt: time.Now()}
				h.Notify("bar", nil, metadata)
				h.WaitForDelete(mock, foo)
			},
		},
		{
			name:       "expect delete",
			shouldFail: false,
			test: func(mock *testingTMock, h ChanSubscriptionHandler[*wrapperspb.Int64Value]) {
				metadata := ads.SubscriptionMetadata{SubscribedAt: time.Now()}
				h.Notify(foo, nil, metadata)
				h.WaitForDelete(mock, foo)
			},
		},
		{
			name:       "expect delete, get update",
			shouldFail: true,
			test: func(mock *testingTMock, h ChanSubscriptionHandler[*wrapperspb.Int64Value]) {
				metadata := ads.SubscriptionMetadata{SubscribedAt: time.Now()}
				h.Notify(foo, resource, metadata)
				h.WaitForDelete(mock, foo)
			},
		},
		{
			name:       "expect update",
			shouldFail: false,
			test: func(mock *testingTMock, h ChanSubscriptionHandler[*wrapperspb.Int64Value]) {
				metadata := ads.SubscriptionMetadata{SubscribedAt: time.Now()}
				h.Notify(foo, resource, metadata)
				h.WaitForUpdate(mock, resource)
			},
		},
		{
			name:       "expect update, get delete",
			shouldFail: true,
			test: func(mock *testingTMock, h ChanSubscriptionHandler[*wrapperspb.Int64Value]) {
				metadata := ads.SubscriptionMetadata{SubscribedAt: time.Now()}
				h.Notify(foo, nil, metadata)
				h.WaitForUpdate(mock, resource)
			},
		},
		{
			name:       "received different value",
			shouldFail: true,
			test: func(mock *testingTMock, h ChanSubscriptionHandler[*wrapperspb.Int64Value]) {
				metadata := ads.SubscriptionMetadata{SubscribedAt: time.Now()}
				h.Notify(foo, resource, metadata)
				h.WaitForUpdate(mock, ads.NewResource[*wrapperspb.Int64Value](foo, version, wrapperspb.Int64(27)))
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			h := make(ChanSubscriptionHandler[*wrapperspb.Int64Value], 1)
			mock := (*testingTMock)(t)
			if test.shouldFail {
				require.PanicsWithValuef(t, failNowInvoked, func() {
					test.test(mock, h)
				}, "did not panic!")
			} else {
				test.test(mock, h)
			}
		})
	}

}
