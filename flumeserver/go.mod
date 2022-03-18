module github.com/openrelayxyz/flume/flumeserver

go 1.13

require (
	github.com/NYTimes/gziphandler v1.1.1
	github.com/Shopify/sarama v1.29.0
	github.com/StackExchange/wmi v0.0.0-20210224194228-fe8f1750fd46 // indirect
	github.com/ethereum/go-ethereum v1.10.4
	github.com/go-ole/go-ole v1.2.5 // indirect
	github.com/klauspost/compress v1.12.3
	github.com/mattn/go-sqlite3 v2.0.3+incompatible
	github.com/openrelayxyz/cardinal-evm v0.0.0-20211007195410-cf1ee1556687
	github.com/openrelayxyz/cardinal-streams v0.0.29
	github.com/openrelayxyz/cardinal-types v0.0.4
	github.com/rs/cors v1.7.0
)

replace github.com/ethereum/go-ethereum => github.com/notegio/go-ethereum v1.10.8-0

replace github.com/Shopify/sarama => github.com/openrelayxyz/sarama v0.0.0-20200619041629-a7760f73892f
