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
