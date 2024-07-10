/*
Package diderot provides a set of utilities to implement an xDS control plan in go. Namely, it
provides two core elements:
 1. The [ADSServer], the implementation of both the SotW and Delta ADS stream variants.
 2. The [Cache], which is an efficient means to store, retrieve and subscribe to xDS resource definitions.

# ADS Server and Resource Locator

The [ADSServer] is an implementation of the xDS protocol's various features. It implements both the
Delta and state-of-the-world variants, but abstracts this away completely by only exposing a single
entry point: the [ResourceLocator]. When the server receives a request (be it Delta or SotW), it
will first check if the requested type is supported, whether it is an ACK (or a NACK), then invoke,
if necessary, the corresponding subscription methods on the ResourceLocator. The locator is simply
in charge of invoking Notify on the handler whenever the resource changes, and the server will relay
that resource update to the client using the corresponding response type. This makes it very easy to
implement an xDS control plane without needing to worry about the finer details of the xDS protocol.

Most ResourceLocator implementations will likely be a series of [Cache] instances for the
corresponding supported types, which implements the semantics of Subscribe and Resubscribe out of
the box. However, as long as the semantics are respected, implementations may do as they please. For
example, a common pattern is listed in the [xDS spec]:

	For Listener and Cluster resource types, there is also a “wildcard” subscription, which is triggered
	when subscribing to the special name *. In this case, the server should use site-specific business
	logic to determine the full set of resources that the client is interested in, typically based on
	the client’s node identification.

Instead of invoking subscribing to a backing [Cache] with the wildcard subscription, the said
"business logic" can be implemented in the [ResourceLocator] and wildcard subscriptions can be
transformed into an explicit set of resources.

# Cache

This type is the core building block provided by this package. It is effectively a map from
resource name to [ads.Resource] definitions. It provides a way to subscribe to them in order to be
notified whenever they change. For example, the [ads.Endpoint] type (aka
"envoy.config.endpoint.v3.ClusterLoadAssignment") contains the set of IPs that back a specific
[ads.Cluster] ("envoy.config.cluster.v3.Cluster") and is the final step in the standard LDS -> RDS
-> CDS -> EDS Envoy flow. The Cache will store the Endpoint instances that back each cluster, and
Envoy will be able to subscribe to the [ads.Endpoint] resource by providing the correct name when
subscribing. See [diderot.Cache.Subscribe] for additional details on the subscription model.

It is safe for concurrent use as its concurrency model is per-resource. This means different
goroutines can modify different resources concurrently, and goroutines attempting to modify the
same resource will be synchronized.

# Cache Priority

The cache supports a notion of "priority". Concretely, this feature is intended to be used when a
resource definition can come from multiple sources. For example, if resource definitions are being
migrated from one source to another, it would be sane to always use the new source if it is present,
otherwise fall back to the old source. This would be as opposed to simply picking whichever source
defined the resource most recently, as it would mean the resource definition cannot be relied upon
to be stable. [NewPrioritizedCache] returns a slice of instances of their respective types. The
instances all point to the same underlying cache, but at different priorities, where instances that
appear earlier in the slice have a higher priority than those that appear later. If a resource is
defined at priorities p1 and p2 where p1 is a higher priority than p2, subscribers will see the
version that was defined at p1. If the resource is cleared at p1, the cache will fall back to the
definition at p2. This means that a resource is only ever considered fully deleted if it is cleared
at all priority levels. The reason a slice of instances is returned rather than adding a priority
parameter to each function on [Cache] is to avoid complicated configuration or simple bugs where a
resource is being set at an unintended or invalid priority. Instead, the code path where a source is
populating the cache simply receives a reference to the cache and starts writing to it. If the
priority of a source changes in subsequent versions, it can be handled at initialization/startup
instead of requiring any actual code changes to the source itself.

# xDS TP1 Support

The notion of glob collections defined in the TP1 proposal is supported natively in the [Cache].
This means that if resource names are [xdstp:// URNs], they will be automatically added to the
corresponding glob collection, if applicable. These resources are still available for subscription
by their full URN, but will also be available for subscription by subscribing to the parent glob
collection. More details available at [diderot.Cache.Subscribe], [ads.ParseGlobCollectionURL] and
[ads.ExtractGlobCollectionURLFromResourceURN].

[xDS spec]: https://www.envoyproxy.io/docs/envoy/latest/api-docs/xds_protocol#how-the-client-specifies-what-resources-to-return
[xdstp:// URNs]: https://github.com/cncf/xds/blob/main/proposals/TP1-xds-transport-next.md#uri-based-xds-resource-names
*/
package diderot
