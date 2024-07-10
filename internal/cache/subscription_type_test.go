package internal

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSubscriptionType(t *testing.T) {
	require.False(t, ExplicitSubscription.isImplicit())
	require.True(t, GlobSubscription.isImplicit())
	require.True(t, WildcardSubscription.isImplicit())
}
