package logfeed

import (
  "database/sql"
  "github.com/ethereum/go-ethereum/core/types"
  "github.com/ethereum/go-ethereum/event"

  "time"
)

type Feed interface {
  SubscribeLogs(chan types.Log) event.Subscription
  Ready() chan struct{}
  Healthy(d time.Duration) bool
  Commit(uint64, *sql.Tx)
  // Rollback()
}
