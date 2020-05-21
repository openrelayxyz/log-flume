package logfeed

import(
  "database/sql"
  "github.com/ethereum/go-ethereum/core/types"
  "github.com/ethereum/go-ethereum/event"
  "time"
)

type NullSubscription struct {}

func (s *NullSubscription) Err() <-chan error {
  return make(chan error)
}
func (s *NullSubscription) Unsubscribe() {}

type NullFeed struct {}

func (f *NullFeed) SubscribeLogs(chan types.Log) event.Subscription {
  return &NullSubscription{}
}

func (f* NullFeed) Ready() chan struct{} {
  result := make(chan struct{}, 1)
  result <- struct{}{}
  return result
}
func (f* NullFeed) Healthy(d time.Duration) bool {
  return true
}
func (f* NullFeed) Commit(uint64, *sql.Tx) {}
