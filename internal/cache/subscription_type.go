package internal

// subscriptionType describes the ways a client can subscribe to a resource.
type subscriptionType byte

// The following subscriptionType constants define the ways a client can subscribe to a resource. See
// RawCache.Subscribe for additional details.
const (
	// An ExplicitSubscription means the client subscribed to a resource by explicit providing its name.
	ExplicitSubscription = subscriptionType(iota)
	// A GlobSubscription means the client subscribed to a resource by specifying its parent glob
	// collection URL, implicitly subscribing it to all the resources that are part of the collection.
	GlobSubscription
	// A WildcardSubscription means the client subscribed to a resource by specifying the wildcard
	// (ads.WildcardSubscription), implicitly subscribing it to all resources in the cache.
	WildcardSubscription

	subscriptionTypes = iota
)

func (t subscriptionType) isImplicit() bool {
	return t != ExplicitSubscription
}
