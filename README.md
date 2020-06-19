## About 

Our mission is to make ETH nodes operationally manageable. Flume, the
log indexing service, makes getting log data simple and fast. It gives people
access to same core functionality of proprietary services while running your
own nodes.

## MVP

This repo contains an MVP based on our EtherCattle deployment infrastructure. It
contains a basic gatsby website that will be able to connect to a running
instance of flume, which is connected to a websocket endpoint.

To build the original flume database:
- `geth`
- `massive eth gethLogs --fromBlock 0 --toBlock $RECENT_BLOCK_NUMBER http://localhost:8545 | gzip > logs.gz # https://github.com/NoteGio/massive`
- `zcat logs.gz | sqliteload.py mainnet.db`

Once database is built, compile and run the flume server, pointing at the
database, and then compile and open the static javascript flume-interface
example site.

## Architecture

Flume is designed to organize Ethereum log data to make it more accessible than
a conventional Ethereum node. The flume server is separated into three modules:

### The Log Feed

The log feed pulls logs from some source, and enables other components to
subscribe via Go channels. At present, we support three sources for logs:

#### Null

This isn't really a log source, but it matches the interface. You need a log
source to start up a flume server, so if you want to start Flume without a
source of logs, you can use the null log feed.

#### Websockets

If you're running Flume outside of an EtherCattle deployment, you probably want
the websockets log feed. It will resume from the most recent log in the database
and catch up with the network, then it will subscribe to a websocket feed for
updates. Point this to a standard Web3 RPC Websocket provider.

#### Kafka

If you are running an EtherCattle deployment, you have one or more masters that
feeds updates to its replicas via Kafka. The master can also feed event data to
Flume via Kafka, and the Kafka log feed knows how to interpret that. Note that
the logic for interpreting the log feed is fairly complicated to allow for
multiple masters and chain reorgs.

### The Indexer

The indexer pulls from a log feed and commits to a sqlite database. Commits
happen in large transactions whenever there's a lull in the log feed (meaning
the indexer checks the log feed and doesn't find any new logs to process). Once
the indexer is caught up with the chain, this happens every time a block comes
in, but while catching up it might process several blocks worth of logs in a
single transaction.

### The Request Handler

The request handler listens for requests on HTTP, translates RPC calls into SQL
queries, and SQL results back into web3 RPC responses. Right now, with the
limited set of RPC methods we support, the handler is implemented somewhat
naively; if we add a significant number of supported RPC methods we may want to
revisit.

#### Supported RPC Methods

* `eth_blockNumber`: The standard Web3 block number request, just like a normal
  Ethereum node. This helps with monitoring where Flume sits relative to nodes.
* `eth_getLogs`: The standard Web3 getLogs request, just like a normal Ethereum
  node except running from the sqlite database rather than leveldb / ancients
  database.
* `flume_erc20ByAccount`: Flume extended API to get all ERC20 tokens held by an
  account. Full details below.
* `flume_erc20Holders`: Flume extended API to to get all holders of a given
  ERC20 token. Full details below.

### General Design

We considered several potential models for how to manage the index relative to
other data in the EtherCattle system. Early on we considered having this index
live with replica, populating the index and searching it within the replica
process. There were a few problems with this approach:

* It adds around 300 GB of disk that needed to live with replicas. Disk is one
  of the major costs of running a replica, so nearly doubling the required disk
  was undesirable. Since only a small portion of traffic would access the event
  log index, we don't need one copy of the event logs per replica.
* Our snapshotting approach works well for LevelDB, but terribly for SQLite. AWS
  snapshots start up with poor performance and improve over time; with LevelDB
  we use an overlay systems so we can populate leveldb quickly, but SQLite is
  not conducive to comparable overlays. This makes startups much slower.

Ruling out having a SQLite database with each replica, our next thought was to
put the data into a conventional relational database such as PostgreSQL.
Cost-wise, this was better than having a copy per replica (and we could scale
read replicas to meet demand). But the cost of a managed database in AWS seemed
a bit steep, and there were some challenges around managing the database
reliably in a multi-master multi-replica setup.

Ultimately, we decided to mirror the replica setup from EtherCattle and put log
data onto its own servers. By pulling log data from Kafka, we can maintain the
local database very efficiently, and it makes our snapshotting process easy. By
putting Flume on separate servers we've separated our scaling needs - we can
scale Flume servers when they reach a certain threshold independently of scaling
our replica servers, and avoid doubling up on disk costs.

#### Why SQLite?

Some people are surprised when we tell them we're running a large scale
production system on SQLite. Isn't that for embedded systems and testing?

SQLite is actually a rock solid relational database, with performance in the
same class as MySQL and PostgreSQL. Its shortcomings are that it is not a
networked database, and can run into locking issues with multiple readers and
writers. Since all of the data being written to Flume is a function of the
peer-to-peer network, having a networked database is not especially important;
we can keep our copies of the database sufficiently in sync simply by pulling
from the Kafka feed. Concerns about locking with multiple readers and writers
are easily handled; with just one thread for writing to the database we can use
SQLite's Write Ahead Log mode, which allows us to have as many readers as we
need concurrent with a single writer without any locking issues.

## Build

flume-interface:
```
cd flume-interface
yarn install
yarn build
```

flumeserver:
```
cd flumeserver
go get
go build .
```

## Deploy

- Upload your lz4 compressed mainnet.db to an S3 bucket
- Stand up an infrastructure stack from EtherCattle: https://github.com/openrelayxyz/ethercattle-deployment
- Create a cloudformation stack based on the `flume-cluster.yaml`
- Specify your bucket on deployment where you have posted your sqlite db (As of MVP this is a hard coded location. Edit the file) TODO

## Extended APIs

Aside from serving `eth_getLogs`, Flume is able to serve additioanl API calls
based on its index.

For now, we've added two new RPC calls:

* `flume_erc20ByAccount(address[, next])`: Returns a list of the ERC20 tokens
  that the specified address has received. If the result set exceeds a certain
  limit, the result will include a `next` token, which can be passed to a
  subsequent call to get more results.

  Example:

  ```
  curl http://flumehost/ --data '{"id": 1, "method": "flume_erc20ByAccount", "params": ["0x0a65659b64573628ff7f90226b5a8bcbd3abf075"]}'
  {"jsonrpc":"","id":1,"result":{"items":["0x0027449bf0887ca3e431d263ffdefb244d95b555", "0xdde19c145c1ee51b48f7a28e8df125da0cc440be"]}}
  ```

  *Note*: This API will return any ERC20 tokens the address has ever received;
  it is possible that those tokens are not currently present in the wallet.

* `flume_erc20Holders(address[, next])`: Returns a list of the addresses that
have received the specified ERC20 token. If the result set exceeds a certain
limit, the result will include a `next` token, which can be passed to a
subsequent call to get more results.

  Example:

  ```
  curl http://flumehost/ --data '{"id": 1, "method": "flume_erc20Holders", "params": ["0xdde19c145c1ee51b48f7a28e8df125da0cc440be"]}'
  {"jsonrpc":"","id":1,"result":{"items":["0xaa461d363125ad5ce27b3941ed6a2b1cf2c7cdf3","0x08409de58f3ad94c5e2c53dbe60ae01be472a820","0x0a65659b64573628ff7f90226b5a8bcbd3abf075","0x18e4ff99ee82f4a38292f1a5d5b2951a5d2a6f2d",["..."]]}}
  ```
  *Note*: This API will return any accounts that have ever received the token;
  it is possible that those tokens are not currently present in the wallet.

In the future we may add additional APIs to be able to include current balances,
along with the list of tokens received.
