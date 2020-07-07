package logfeed

import (
  "context"
  "fmt"
  "strings"
  "database/sql"
  "github.com/Shopify/sarama"
  "github.com/ethereum/go-ethereum/common"
  "github.com/ethereum/go-ethereum/core"
  "github.com/ethereum/go-ethereum/core/types"
  "github.com/ethereum/go-ethereum/event"
  "github.com/ethereum/go-ethereum/replica"
  "sync/atomic"
  "time"
  "log"
)

type ethKafkaFeed struct {
  lastBlockTime *atomic.Value
  eventConsumer replica.EventConsumer
  blockFeed event.Feed
  logFeed event.Feed
  chainHeadEventCh chan core.ChainHeadEvent
  offsetCh chan replica.OffsetHash
  db *sql.DB
}

func NewKafkaFeed(urlStr string, db *sql.DB) (Feed, error) {
  var tableName string
  db.QueryRowContext(context.Background(), "SELECT name FROM sqlite_master WHERE type='table' and name='offsets';").Scan(&tableName)
  if tableName != "offsets" {
    if _, err := db.Exec("CREATE TABLE offsets (offset BIGINT, PRIMARY KEY (offset));"); err != nil {
      return nil, fmt.Errorf("Offsets table does not exist and could not create: %v", err.Error())
    }
    if _, err := db.Exec("INSERT INTO offsets(offset) VALUES (?);", sarama.OffsetOldest); err != nil {
      return nil, err
    }
  }
  var resumeOffset int64
  db.QueryRowContext(context.Background(), "SELECT max(offset) FROM offsets;").Scan(&resumeOffset)
  if resumeOffset == 0 {
    resumeOffset = sarama.OffsetOldest
  }
  parts := strings.Split(urlStr, ";")
  log.Printf("Parts: %v", parts)
  log.Printf("Resume offset: %v", resumeOffset)

  consumer, err := replica.NewKafkaEventConsumerFromURLs(strings.TrimPrefix(parts[0], "kafka://"), parts[1], common.Hash{}, resumeOffset)
  if err != nil { return nil, err }
  feed := &ethKafkaFeed{
    lastBlockTime: &atomic.Value{},
    eventConsumer: consumer,
    db: db,
  }
  feed.subscribe()
  return feed, nil
}

func (feeder *ethKafkaFeed) subscribe() {
  feeder.eventConsumer.Start()
  logsEventCh := make(chan []*types.Log)
  logsEventSub := feeder.eventConsumer.SubscribeLogsEvent(logsEventCh)
  removedLogsEventCh := make(chan core.RemovedLogsEvent)
  removedLogsEventSub := feeder.eventConsumer.SubscribeRemovedLogsEvent(removedLogsEventCh)
  feeder.chainHeadEventCh = make(chan core.ChainHeadEvent, 1000)
  chainHeadEventSub := feeder.eventConsumer.SubscribeChainHeadEvent(feeder.chainHeadEventCh)
  feeder.offsetCh = make(chan replica.OffsetHash, 1000)
  offsetSub := feeder.eventConsumer.SubscribeOffsets(feeder.offsetCh)
  chainHeadEventCh := make(chan core.ChainHeadEvent, 1000)
  chainHeadEventSub2 := feeder.eventConsumer.SubscribeChainHeadEvent(chainHeadEventCh)
  go func() {
    defer logsEventSub.Unsubscribe()
    defer removedLogsEventSub.Unsubscribe()
    defer chainHeadEventSub.Unsubscribe()
    defer offsetSub.Unsubscribe()
    defer chainHeadEventSub2.Unsubscribe()
    for {
      select {
      case addLogs := <-logsEventCh:
        for _, log := range addLogs {
          feeder.logFeed.Send(*log)
        }
      case removeLogs := <-removedLogsEventCh:
        for _, log := range removeLogs.Logs {
          log.Removed = true
          feeder.logFeed.Send(*log)
        }
      case chainHead := <-chainHeadEventCh:
        // For metrics gathering, we need to log the block number more often on
        // chains with low log volume. If we have a head event and the log
        // channels are empty, log a 0 log message.
        if len(logsEventCh) == 0 && len(removedLogsEventCh) == 0 {
          log.Printf("Committing 0 logs up to block %v", chainHead.Block.NumberU64())
          feeder.Commit(chainHead.Block.NumberU64(), nil)
        }
      }
    }
  }()
}

func (feeder *ethKafkaFeed) Commit(num uint64, tx *sql.Tx) {
  chainHead := <-feeder.chainHeadEventCh
  count := 1
  chainHeadLoop:
  for chainHead.Block.NumberU64() < num{
    count++
    select {
    case chainHead = <-feeder.chainHeadEventCh:
    default:
      log.Printf("No chainheads matching commit. Using latest.")
      break chainHeadLoop
    }
  }
  offset := <-feeder.offsetCh
  offsetLoop:
  for offset.Hash != chainHead.Block.Hash() {
    select {
    case offset = <-feeder.offsetCh:
    default:
      log.Printf("No offset matching block. Using latest.")
      break offsetLoop
    }
  }
  if tx != nil {
    _, err := tx.Exec("UPDATE offsets SET offset = ? WHERE offset < ?;", offset.Offset, offset.Offset)
    if err != nil { log.Printf("Error updating offset: %v", err.Error())}
  }
}

func (feeder *ethKafkaFeed) SubscribeLogs(ch chan types.Log) event.Subscription {
  return feeder.logFeed.Subscribe(ch)
}
func (feeder *ethKafkaFeed) Ready() chan struct{} {
  return feeder.eventConsumer.Ready()
}
func (feeder *ethKafkaFeed) Healthy(d time.Duration) bool {
  lastBlockTime, ok := feeder.lastBlockTime.Load().(time.Time)
  if !ok {
    log.Printf("ethKafkaFeed unhealthy - lastBlockTime is not a time.Time")
    return false
  } else if time.Since(lastBlockTime) > d {
    log.Printf("ethKafkaFeed unhealthy - No blocks received in timeout")
    return false
  }
  return true
}
