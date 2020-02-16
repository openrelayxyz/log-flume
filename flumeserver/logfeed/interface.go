package logfeed

import (
  "github.com/ethereum/go-ethereum/core/types"
  "github.com/ethereum/go-ethereum/event"
  "time"
)

type Feed interface {
  SubscribeBlocks(chan map[string]interface{}) event.Subscription
  SubscribeLogs(chan types.Log) event.Subscription
  Ready() chan struct{}
  Healthy(d time.Duration) bool
}
