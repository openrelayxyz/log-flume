## About
Our mission is to make ETH nodes operationally manageable. Flume, the log indexing service makes getting log data, simple and fast. It gives people access to same core functionality of proprietary services while running your own nodes.

## MVP

This repo contains an MVP based on our EtherCattle deployment infrastructure. It contains a basic gatsby website that will be able to connect to a running instance of flume, which is connected to a websocket endpoint.

To build the original flume database:
- `geth`
- `massive eth gethLogs --fromBlock 0 --toBlock $RECENT_BLOCK_NUMBER http://localhost:8545 | gzip > logs.gz # https://github.com/NoteGio/massive`
- `zcat logs.gz | sqliteload.py mainnet.db`

Once database is built, compile and run the flume server, pointing at the database, and then compile and open the static javascript flume-interface example site.


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
