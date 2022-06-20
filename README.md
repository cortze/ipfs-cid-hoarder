
# IPFS-CID-hoarder

An IPFS CID "crawler" that monitors the shared content in the IPFS Network. The tool will serve as the data-gathering part to study and measure Protocol Labs's [RFM 17](https://github.com/protocol/network-measurements/blob/master/RFMs.md#rfm-17--provider-record-liveness) and [RFM 1](https://github.com/protocol/network-measurements/blob/master/RFMs.md#rfm-1--liveness-of-a-document-in-the-ipfs-network) (Liveness of a document in the IPFS Network).

# Motivation
In content sharing platforms, distributed or non-distributed ones, the content always needs to be stored somewhere. In the IPFS network, although the content may be in more than one location, it normally starts from the IPFS client/server that has the content and publishes the Provider Records (PR) to the rest of the network. This PR contains the link between the CID (content) that they are sharing and the multi-address where the content can be retrieved. 

As explained in the Kademlia DHT paper, the PRs get shared with other [K=20](https://github.com/libp2p/go-libp2p-kbucket) peers of the network, corresponding K to the closest peers to the CID using the XOR distance. The described step corresponds to the [PROVIDE](https://github.com/libp2p/go-libp2p-kad-dht/blob/cd05807c54f3168f01a5a363b37aee5e38fee63d/routing.go#L373) method of the [IPFS-Kad-DHT](https://github.com/libp2p/go-libp2p-kad-dht) implementation, where the client/server finds out which peers are the closest ones to the CID, and then sends them an ADD_PROVIDE message to notify them that they are inside the set of closest peers to that specific content. After this process, any other peer that walks the IPFS DHT to retrieve that CID, will ask for the closest peers to the CID and it will end up asking one of these k=20 peers for the PR (ideal scenario).

The theory looks solid, the K=20 value was initially chosen to increase the resilience of the network to node churn. At the moment, the overall network seems to be working fine, although there are some concerns about the impact of Hydra nodes and the churn rate.

The fact that k=20 peers keep the records, means that as soon as one of them is actively keeping the PR, the content should be retrievable. However, if after 4 hours of publishing the PRs, only one of the 20 peers keeps the records, one could conclude that the network is exposed to a very high node churn rate, and therefore, the K value won’t longer be the appropriate one for the node churn and network size at that specific moment.

There are some concerns about the impact of the Hydra-boosters in the network as they represent a more centralized infrastructure than the one targeted by the IPFS network. Hydra nodes are placed in the network to accelerate the content discovery, and therefore, the performance of IPFS. However, Are they the ones keeping up IPFS Network alive? 

The focus of the study is to tackle these concerns by generating a tool that can follow up with the peers chosen to be PR Holders, bringing up more insights about the Provider Record Liveness.

# Methodology
[IPFS-CID-Hoarder](https://github.com/cortze/ipfs-cid-hoarder) is the tool that will collect the data for the study (currently in development stage).

In this first approach of the CID-Hoarder, the tool will generate and track a set of CIDs over time. The randomness of the CIDs helps to cover homogeneously the entire hash space, which consequently helps to understand which range of the hash space suffers more from node churn or lack of peers.

As explained before, the tool will have a set of inputs to configure the study:

```
CID Hoarder Flags:
- Log Level
- Database Endpoint
- CID content size
- CID number for the entire study
- Batch size to reduce the overhead of the pinging the CIDs
- CID track frequency
- Total track time
- K value for the Kad DHT configuration
```

For the given number of CIDs and the number of workers, the tool will automatically publish the generated CIDs using the number of workers.

For example, if we want to generate 1000 CIDs with a worker number of 200 CIDs, the tool will spawn 200 workers to publish the CIDs concurrently.

*NOTE: The Provider Records WON’T be republished after 12 hours. The goal of the study is to track the theoretical record lifetime for a range of CIDs across the hash space.*

For each generated CID, the tool will keep track of:

```
CidInfo:
- Generation Time
- Number of peers requested to keep the PR
- [] PR Holder Info
- [] PR Ping Results
- Provide Method Duration 
```

After the generation and publication of the CID, the tool waits for the result of each ADD_PROVIDE message sent to the K closest peers. 

Once we know which are the peers holding the PR, the tool keeps track of the given info from the peer, 

```
PR Holder Info:
- Peer ID
- Multiaddresses
- User Agent
- Client Type
- Client Version
```

Already from the first ADD_PROVIDE connection to the K closest peers, the tool fulfills the following table as a result of the PR holder’s pinging process:

```
Fetch Results: (Summary of the K PR Ping Results)
- CID 
- Fetch round
- Fetch Time
- Fetch Round Duration
- PR Holders Ping Duration
- Find-Providers Duration 
- Get K Close-Peers Duration
- [] PR Ping Results
- Is Retrievable 
- [] K Closest peers to the CID

PR Ping Rsults: (Individual ping for a Peer ID per round and CID)
- CID
- Peer ID
- Ping Round
- Fetch Time
- Fetch Duration
- Is Active
- Has Records
- Connection Error
```

Once each CID has been generated and published to the network, The tool proceeds to persist to the DB the recorded information and sends the CID information to the CID Ping-Orchester routine.

The CID Ping-Orchester has a list of CIDs ordered by the next connection time. It uses this next connection time to determine whether the CID needs to be pinged again or not. If the CID’s next connections proves that the track frequency has passed since the last ping, it will add the CID info into a `Ping Queue` for the ping worker.

Each of the CID Ping workers will try to establish a connection with the PR Holders of the CID reader from the `Ping Queue` tracking the dial time, whether they are active or not, the reported error if the connection attempt failed, and whether they still keep the PR or not. Further more, the tool also walks the DHT looking for the K closest peers at the moment of pinging the PR Holders, and tries to retrieve the PR from the DHT itself. 

After attempting to connect all the PR Holders of a CID, the tool persists the Ping Results and their summary (Fetch Results) into the DB.

The tool will keep pinging PR Holders every tick until the CID Track Time is completed.

By changing the configuration parameters of the tool, we will be able to generate a complete analysis of the Provider Record Liveness, including tests with different K values that will help us understand which value of K better fits the current network churn and size.


# Install

## Requirements

To compile the tool you will need:
- `make`
- Go `1.17` (Go `1.18` is still not supported by few imported modules)
- A `postgres` instance, doesn't matter if you are running it on a docker or locally


## Compilation

Download the repo, install the dependencies, and compile it by yourself:

```
git clone https:github.com/cortze/ipfs-cid-hoarder.git
cd ipfs-cid-hoarder
make dependencies
make install
```

You are ready to go!

# Usage

The CID-Hoarder has a set of arguments to configure each of the studies:

```
ipfs-cid-hoarder run [command options] [arguments...]

OPTIONS:
   --log-level value          verbosity of the logs that will be displayed [debug,warn,info,error] (default: info) [$IPFS_CID_HOARDER_LOGLEVEL]
   --priv-key value           Private key to initialize the host (to avoid generating node churn in the network) [$IPFS_CID_HOARDER_PRIV_KEY]
   --database-endpoint value  database enpoint (e.g. /Path/to/sqlite3.db) (default: ./data/ipfs-hoarder-db.db) [$IPFS_CID_HOARDER_DATABASE_ENDPOINT]
   --cid-source value         defines the mode where we want to run the tool [random-content-gen, text-file, bitswap] (default: text)
   --cid-file value           link to the file containing the files to track (txt/json files) (default: cids/test.txt) [$IPFS_CID_HOARDER_CID_FILE]
   --cid-content-size value   size in KB of the random block generated (default: 1MB) [$IPFS_CID_HOARDER_CID_CONTENT_SIZE]
   --cid-number value         number of CIDs that will be generated for the study (default: 1000 CIDs) [$IPFS_CID_HOARDER_CID_NUMBER]
   --workers value            max number of CIDs on each of the generation batch (default: 250 CIDs) [$IPFS_CID_HOARDER_BATCH_SIZE]
   --req-interval value       delay in minutes in between PRHolders pings for each CID (example '30m' - '1h' - '60s') (default: 30m) [$IPFS_CID_HOARDER_REQ_INTERVAL]
   --study-duration value     max time for the study to run (example '24h', '35h', '48h') (default: 48h) [$IPFS_CID_HOARDER_STUDY_DURATION]
   -k value                   number of peers that we want to forward the Provider Records (default: K=20) [$IPFS_CID_HOARDER_K]
   --help, -h                 show help (default: false)
```


# Maintainers
@cortze

# Contributing
The project is open for everyone to contribute! 