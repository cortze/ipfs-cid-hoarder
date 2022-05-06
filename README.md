# IPFS-CID-hoarder

An IPFS CID "crawler" that monitors the shared content in the IPFS Network. The tool will serve as the data-gathering part to study and measure [Protocol Labs's RFM 1](https://github.com/protocol/network-measurements/blob/master/RFMs.md#rfm-1--liveness-of-a-document-in-the-ipfs-network) (Liveness of a document in the IPFS Network).

# Development Stages
Since the project is still in a "draft" version, this chapter contains the development roadmap/milestones that I thought would organize and define the project's workload.

## 1. First Approach (v1.0.0)
In this first stage, the CID-Hoarder will get/fetch the content for a given set of CIDs (provided in a txt/json file). These CIDs, extracted from the IPFS Gateways, will serve as a test for the tool and as a base to prepare dashboards and later studies.

The tool will regularly attempt to fetch the content from the DHT, gathering a set of metrics for each of the contents as described in the following table:

```
ProviderPeerIDs     // ID of the peer serving the content 
ProviderRecords     // Multiaddress of the Peers providing the content 
ContentType         // Parsed content type for the retrieved data (Website, Image, Video, Compressed Folder, etc)
ContentExtension    // Extension of the file/files retrieved from the CID
FirstTimeFetched    // First time the tool fetched the CID content 
LastTimeFetched     // Last time the tool could retrieve the content 
NonFetchableDates   // List of dates where we couldn't retrieve the content
```

The tool will fill the `content metadata` DB in the first retrieval of the CID, and it will keep pinging the CID every hour and updating the DB with a positive or negative attempt. 
The first experiment will run for a few days to test the tool's performance. After all the checks are successful, we could already move to stage 2 :) 

## 2. Cli CID Discovery Improvement (v2.0.0)
The second stage of the tool implies adding CID discovery through the Bitswap protocol. As suggested in the [RMF](https://github.com/protocol/network-measurements/blob/master/RFMs.md#rfm-1--liveness-of-a-document-in-the-ipfs-network), the tool would have to unlimit the number of connections to fetch ass many `HAVE` and `WANT` messages as possible.

With the Bitswap implementation, we would be able to extend the measurements to a bigger portion of CIDs, which would allow us to generate a list of popular content by:
- `CID`
- `Content Type`
- `Content Extension`

## 3. Hydra Avoiding flag (v3.0.0)
For the third stage, it would be ideal to set up a `go-ipfs` fork that avoids fetching content from `hydra-boosters`. The `go-ipfs` fork would check if the peer we are dialing has the hydra `user agent`, which will abort the petition if the result is positive. 

It doesn't necessarily need to avoid them, but it would be essential to find the `provider records` from `non-hydra` peers while keeping track of whether the content was reachable through a `hydra` peer.
This third step would provide some preliminary study of how much impact `hydras` have on the IPFS Network. 

NOTE: Ambitious, but cool! :sunglasses:

# Maintainers
@cortze

# Contributing
The project is open for everyone to contribute! 