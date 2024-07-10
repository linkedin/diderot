package ads

import (
	"errors"
	"net/url"
	"strings"

	types "github.com/envoyproxy/go-control-plane/pkg/resource/v3"
)

// GlobCollectionURL represents the individual elements of a glob collection URL. Please refer to the
// [TP1 Proposal] for additional context on each field. In summary, a glob collection URL has the following format:
//
//	xdstp://{Authority}/{ResourceType}/{Path}{?ContextParameters}
//
// [TP1 Proposal]: https://github.com/cncf/xds/blob/main/proposals/TP1-xds-transport-next.md#uri-based-xds-resource-names
type GlobCollectionURL struct {
	// The URL's authority. Optional when URL of form "xdstp:///{ResourceType}/{Path}".
	Authority string
	// The type of the resources in the collection, without the "type.googleapis.com/" prefix.
	ResourceType string
	// The collection's path, without the trailing /*
	Path string
	// Optionally, the context parameters associated with the collection, always sorted by key name. If
	// present, starts with "?".
	ContextParameters string
}

func (u GlobCollectionURL) String() string {
	var path string
	switch u.Path {
	case "":
		path = WildcardSubscription
	case "/":
		path = "/" + WildcardSubscription
	default:
		path = u.Path + "/" + WildcardSubscription
	}

	return XDSTPScheme +
		u.Authority + "/" +
		u.ResourceType + "/" +
		path +
		u.ContextParameters
}

// ErrInvalidGlobCollectionURI is always returned by the various glob collection URL parsing
// functions.
var ErrInvalidGlobCollectionURI = errors.New("diderot: invalid glob collection URI")

// TODO: the functions in this file return non-specific errors to avoid additional allocations during
//  cache updates, which can build up and get expensive. However this can be improved by having an
//  error for each of the various ways a string can be an invalid glob collection URL.

// ParseGlobCollectionURL attempts to parse the given name as GlobCollectionURL, returning an error
// if the given name does not represent one. See the [TP1 proposal] for additional context on the
// exact definition of a glob collection.
//
// [TP1 proposal]: https://github.com/cncf/xds/blob/main/proposals/TP1-xds-transport-next.md#uri-based-xds-resource-names
func ParseGlobCollectionURL(name, resourceType string) (GlobCollectionURL, error) {
	gcURL, err := parseXDSTPURI(name, resourceType)
	if err != nil {
		return GlobCollectionURL{}, err
	}

	var ok bool
	gcURL.Path, ok = strings.CutSuffix(gcURL.Path, "/"+WildcardSubscription)
	if !ok {
		// URLs must end with /*
		return GlobCollectionURL{}, ErrInvalidGlobCollectionURI
	}

	return gcURL, nil
}

// ExtractGlobCollectionURLFromResourceURN checks if the given name is a resource URN, and returns
// the corresponding GlobCollectionURL. The format of a resource URN is defined in the
// [TP1 proposal], and looks like this:
//
//	xdstp://[{authority}]/{resource type}/{id/*}?{context parameters}
//
// For example:
//
//	xdstp://some-authority/envoy.config.listener.v3.Listener/foo/bar/baz
//
// In the above example, the URN belongs to this collection:
//
//	xdstp://authority/envoy.config.listener.v3.Listener/foo/bar/*
//
// Note that in the above example, the URN does _not_ belong to the following collection:
//
//	xdstp://authority/envoy.config.listener.v3.Listener/foo/*
//
// Glob collections are not recursive, and the {id/?} segment of the URN (after the type) should be
// opaque, and not interpreted any further than the trailing /*. More details on this matter can be
// found [here].
//
// This function returns an error if the given name is not a resource URN.
//
// [TP1 proposal]: https://github.com/cncf/xds/blob/main/proposals/TP1-xds-transport-next.md#uri-based-xds-resource-names
// [here]: https://github.com/cncf/xds/issues/91
func ExtractGlobCollectionURLFromResourceURN(name, resourceType string) (GlobCollectionURL, error) {
	gcURL, err := parseXDSTPURI(name, resourceType)
	if err != nil {
		return GlobCollectionURL{}, err
	}

	lastSlash := strings.LastIndex(gcURL.Path, "/")
	if lastSlash == -1 {
		// Missing path in URL
		return GlobCollectionURL{}, ErrInvalidGlobCollectionURI
	}

	if gcURL.Path[lastSlash:] == "/"+WildcardSubscription {
		// resource URN cannot end in /*
		return GlobCollectionURL{}, ErrInvalidGlobCollectionURI
	}

	if lastSlash == 0 {
		gcURL.Path = "/"
	} else {
		gcURL.Path = gcURL.Path[:lastSlash]
	}

	return gcURL, nil
}

func parseXDSTPURI(resourceName, resourceType string) (GlobCollectionURL, error) {
	// Skip deserializing the resource name if it doesn't start with the correct scheme
	if !strings.HasPrefix(resourceName, XDSTPScheme) {
		// doesn't start with xdstp://
		return GlobCollectionURL{}, ErrInvalidGlobCollectionURI
	}

	parsedURL, err := url.Parse(resourceName)
	if err != nil {
		// invalid URL
		return GlobCollectionURL{}, ErrInvalidGlobCollectionURI
	}

	// Glob collection URLs do not start with the type prefix, so trim it here.
	resourceType = strings.TrimPrefix(resourceType, types.APITypePrefix)

	collectionPath, ok := strings.CutPrefix(parsedURL.EscapedPath(), "/"+resourceType+"/")
	if !ok {
		// should include expected type after authority
		return GlobCollectionURL{}, ErrInvalidGlobCollectionURI
	}

	u := GlobCollectionURL{
		Authority:    parsedURL.Host,
		ResourceType: resourceType,
		Path:         collectionPath,
	}
	if len(parsedURL.RawQuery) > 0 {
		// Using .Query() to parse the query then .Encode() to re-serialize ensures the query parameters are
		// in the right sorted order.
		u.ContextParameters = "?" + parsedURL.Query().Encode()
	}

	return u, nil
}
