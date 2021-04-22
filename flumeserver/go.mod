module github.com/openrelayxyz/flume/flumeserver

go 1.13

require (
	github.com/NYTimes/gziphandler v1.1.1
	github.com/Shopify/sarama v1.26.1
	github.com/StackExchange/wmi v0.0.0-20210224194228-fe8f1750fd46 // indirect
	github.com/ethereum/go-ethereum v1.9.25
	github.com/go-ole/go-ole v1.2.5 // indirect
	github.com/klauspost/compress v1.10.7
	github.com/mattn/go-sqlite3 v2.0.3+incompatible
	github.com/pborman/uuid v1.2.1 // indirect
	github.com/rs/cors v1.7.0
)

replace github.com/ethereum/go-ethereum => github.com/notegio/go-ethereum v1.10.1-6

replace github.com/Shopify/sarama => github.com/openrelayxyz/sarama v0.0.0-20200619041629-a7760f73892f
