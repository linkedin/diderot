# Diderot
(pronounced dee-duh-row)

---

Diderot is a server implementation of
the [xDS protocol](https://www.envoyproxy.io/docs/envoy/latest/api-docs/xds_protocol) that makes it extremely easy and
efficient to implement a control plane for your Envoy and gRPC services. For the most up-to-date information, please
visit the [documentation](https://pkg.go.dev/github.com/linkedin/diderot).

## Quick Start Guide
The only thing you need to implement to make your resources available via xDS is a
`diderot.ResourceLocator`([link](https://pkg.go.dev/github.com/linkedin/diderot#ResourceLocator)). It is the interface
exposed by the [ADS server implementation](https://pkg.go.dev/github.com/linkedin/diderot#ADSServer) which should
contain the business logic of all your resource definitions and how to find them. To facilitate this implementation,
Diderot provides an efficient, low-resource [cache](https://pkg.go.dev/github.com/linkedin/diderot#Cache) that supports
highly concurrent updates. By leveraging the cache implementation for the heavy lifting, you will be able to focus on
the meaningful part of operating your own xDS control plane: your resource definitions.

Once you have implemented your `ResourceLocator`, you can simply drop in a `diderot.ADSServer` to your gRPC service, and
you're ready to go! Please refer to the [examples/quickstart](examples/quickstart/main.go) package

## Features
Diderot's ADS server implementation is a faithful implementation of the xDS protocol. This means it implements both the
State-of-the-World and Delta/Incremental variants. It supports advanced features such as
[glob collections](https://github.com/cncf/xds/blob/main/proposals/TP1-xds-transport-next.md#glob), unlocking the more
efficient alternative to the `EDS` stage: `LEDS`
([design doc](https://docs.google.com/document/d/1aZ9ddX99BOWxmfiWZevSB5kzLAfH2TS8qQDcCBHcfSE/edit#heading=h.mmb97owcrx3c)).

