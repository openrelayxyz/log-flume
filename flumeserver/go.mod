module github.com/openrelayxyz/flume/flumeserver

go 1.13

require (
	github.com/NYTimes/gziphandler v1.1.1
	github.com/Shopify/sarama v1.26.1
	github.com/ethereum/go-ethereum v1.9.11
	github.com/gorilla/websocket v1.4.1
	github.com/mattn/go-sqlite3 v2.0.3+incompatible
	github.com/rs/cors v1.7.0
)

replace github.com/ethereum/go-ethereum => github.com/notegio/go-ethereum v0.0.0-20200930195028-b6b5f086629e
