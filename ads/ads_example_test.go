package ads_test

import (
	"log"

	"github.com/linkedin/diderot/ads"
)

func ExampleParseRemainingChunksFromNonce() {
	// Acquire a delta ADS client
	var client ads.DeltaClient

	var responses []*ads.DeltaDiscoveryResponse
	for {
		res, err := client.Recv()
		if err != nil {
			log.Panicf("Error receiving delta response: %v", err)
		}
		responses = append(responses, res)

		if remaining, _ := ads.ParseRemainingChunksFromNonce(res.Nonce); remaining == 0 {
			break
		}
	}

	log.Printf("All responses received: %+v", responses)
}
