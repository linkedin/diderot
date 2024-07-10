package diderot

import (
	"testing"
	"time"

	"github.com/linkedin/diderot/ads"
	"github.com/linkedin/diderot/testutils"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestType(t *testing.T) {
	tests := []struct {
		Name         string
		UseRawSetter bool
	}{
		{
			Name:         "typed",
			UseRawSetter: false,
		},
		{
			Name:         "raw",
			UseRawSetter: true,
		},
	}

	for _, test := range tests {
		t.Run(test.Name, func(t *testing.T) {
			c := NewCache[*wrapperspb.BoolValue]()

			const foo = "foo"

			r := &ads.Resource[*wrapperspb.BoolValue]{
				Name:     foo,
				Version:  "0",
				Resource: wrapperspb.Bool(true),
			}
			if test.UseRawSetter {
				require.NoError(t, c.SetRaw(testutils.MustMarshal(t, r), time.Time{}))
			} else {
				c.SetResource(r, time.Time{})
			}

			testutils.ResourceEquals(t, r, c.Get(foo))

			c.Clear(foo, time.Time{})
			require.Nil(t, c.Get(foo))
		})
	}
}
