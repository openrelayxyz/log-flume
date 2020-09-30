package datafeed

import (
  "context"
  "database/sql"
  "github.com/ethereum/go-ethereum/replica"
  "github.com/ethereum/go-ethereum/common"
  // "github.com/ethereum/go-ethereum/core/types"
  "github.com/ethereum/go-ethereum/event"
  "time"
  "strings"
  "sync/atomic"
  "log"
  "fmt"
)
// replica.KafkaEventConsumer

// type receiptMeta struct {
//   contractAddress common.Address
//   cumulativeGasUsed uint64
//   gasUsed uint64
//   logsBloom types.Bloom
//   status uint64
// }
//
// type ChainEvent struct {
//   Block *types.Block
//   Commit func(*sql.Tx) error
//   receiptMeta map[common.Hash]*receiptMeta
//   logs map[common.Hash][]*types.Log
// }

func ChainEventFromKafka(kce *replica.ChainEvent) *ChainEvent {
  ce := &ChainEvent{
    Block: kce.Block,
    logs: kce.Logs,
    receiptMeta: make(map[common.Hash]*receiptMeta),
  }
  for k, rm := range kce.ReceiptMeta {
    ce.receiptMeta[k] = &receiptMeta{
      contractAddress: rm.ContractAddress,
      cumulativeGasUsed: rm.CumulativeGasUsed,
      gasUsed: rm.GasUsed,
      logsBloom: rm.LogsBloom,
      status: rm.Status,
    }
  }
  return ce
}

type kafkaDataFeed struct {
  lastBlockTime *atomic.Value
  feed event.Feed
  consumer replica.EventConsumer
  topic string
}

func (kdf *kafkaDataFeed) Close() {
  kdf.consumer.Close()
}
func (kdf *kafkaDataFeed) Subscribe(ch chan *ChainEvent) event.Subscription{
  return kdf.feed.Subscribe(ch)
}
func (kdf *kafkaDataFeed) Ready() <-chan struct{}{
  return kdf.consumer.Ready()
}

func (kdf *kafkaDataFeed) Healthy(d time.Duration) bool {
  lastBlockTime, ok := kdf.lastBlockTime.Load().(time.Time)
  if !ok {
    log.Printf("kafkaDataFeed unhealthy - lastBlockTime is not a time.Time")
    return false
  } else if time.Since(lastBlockTime) > d {
    log.Printf("kafkaDataFeed unhealthy - No blocks received in timeout")
    return false
  }
  return true
}

func NewKafkaDataFeed(urlStr string, db *sql.DB) (DataFeed, error) {
  var tableName string
  db.QueryRowContext(context.Background(), "SELECT name FROM sqlite_master WHERE type='table' and name='event_offsets';").Scan(&tableName)
  if tableName != "event_offsets" {
    if _, err := db.Exec("CREATE TABLE event_offsets (partition INT, offset BIGINT, topic STRING, PRIMARY KEY (topic, partition));"); err != nil {
      return nil, fmt.Errorf("Offsets table does not exist and could not create: %v", err.Error())
    }
  }
  parts := strings.Split(urlStr, ";")
  var resumeOffset int64
  var partition int32
  offsets := make(map[int32]int64)
  rows, err := db.QueryContext(context.Background(), "SELECT partition, offset FROM event_offsets WHERE topic = ?;", parts[1])
  if err != nil { return nil, err }
  for rows.Next() {
    if err := rows.Scan(&partition, &resumeOffset); err != nil { return nil, err }
    offsets[partition] = resumeOffset
  }
  log.Printf("Parts: %v", parts)
  log.Printf("Resume offset: %v", offsets)
  consumer, err := replica.NewKafkaEventConsumerFromURLs(strings.TrimPrefix(parts[0], "kafka://"), parts[1], common.Hash{}, offsets)
  if err != nil { return nil, err }
  feed := &kafkaDataFeed{
    lastBlockTime: &atomic.Value{},
    consumer: consumer,
    topic: parts[1],
  }
  feed.subscribe()
  return feed, nil
}

func (kdf *kafkaDataFeed) subscribe() {
  kdf.consumer.Start()
  eventCh := make(chan *replica.ChainEvents)
  eventSub := kdf.consumer.SubscribeChainEvents(eventCh)
  go func() {
    defer eventSub.Unsubscribe()
    for chainEvents := range eventCh {
      n := make([]string, len(chainEvents.New))
      r := make([]string, len(chainEvents.Reverted))
      for i, ce := range chainEvents.New {
        n[i] = ce.Block.Hash().Hex()
      }
      for i, ce := range chainEvents.Reverted {
        r[i] = ce.Block.Hash().Hex()
      }
      // log.Printf("Event: New(%v) Reverted(%v)", n, r)
      for i, chainEvent := range chainEvents.New {
        ce := ChainEventFromKafka(chainEvent)
        if i < len(chainEvents.New) - 1 {
          // If there are multiple chain events in a group, we only want to
          // commit offsets to the database for the last item in the group, as
          // these same offsets triggered the emit of all of the events.
          ce.Commit = func(*sql.Tx) (error) { return nil }
        } else {
          ce.Commit = func(tx *sql.Tx) (error) {
            if tx == nil { return nil }
            for partition, offset := range chainEvents.Partitions {
              if _, err := tx.Exec("INSERT OR REPLACE INTO event_offsets(offset, partition, topic) VALUES (?, ?, ?)", offset, partition, kdf.topic); err != nil {
              //if _, err := tx.Exec("UPDATE event_offsets SET offset = ? WHERE partition = ? AND topic = ?", offset, partition, kdf.topic); err != nil {
                return err
              }
            }
            return nil
          }
        }
        kdf.feed.Send(ce)
      }
    }
  }()

}
