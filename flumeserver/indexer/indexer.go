package indexer

import (
  "bytes"
  "context"
  "github.com/openrelayxyz/flume/flumeserver/logfeed"
  "database/sql"
  "github.com/ethereum/go-ethereum/core/types"
  "github.com/ethereum/go-ethereum/common"
  "log"
  "time"
)

func trimPrefix(data []byte) ([]byte) {
  if len(data) == 0 {
    return data
  }
  v := bytes.TrimLeft(data, string([]byte{0}))
  if len(v) == 0 {
    return []byte{0}
  }
  return v
}

func getTopicIndex(topics []common.Hash, idx int) []byte {
  if len(topics) > idx {
    return topics[idx].Bytes()
  }
  return []byte{}
}

func ProcessFeed(feed logfeed.Feed, db *sql.DB, quit <-chan struct{}) {
  logCh := make(chan types.Log, 1000)
  logSub := feed.SubscribeLogs(logCh)
  defer logSub.Unsubscribe()
  logtx, err := db.BeginTx(context.Background(), nil)
  var logRecord types.Log
  started := false
  counter := 0
  var highestBlock uint64
  for {
    select {
    case <-quit:
      return
    case logRecord = <- logCh:
    default:
      if started {
        log.Printf("Committing %v logs", counter)
        feed.Commit(highestBlock, logtx)
        if err := logtx.Commit(); err != nil {
          // TODO: Consider implementing a feed.Rollback() to roll the feed back
          // to a previous commit. For now crashing and restarting will have a
          // similar effect for indexing, with the downside of serving errors to
          // queries.
          log.Fatalf("Transaction commit failed: %v", err.Error())
        }
        counter = 0
      }
      select{
      case <-quit:
        return
      case logRecord = <- logCh:
        logtx, err = db.BeginTx(context.Background(), nil)
        if err != nil {
          log.Printf("WARN: Failed to start transaction: %v", err.Error())
        }
      }
    }
    // log.Printf("Processing log")
    counter++
    started = true
    for {
      if !logRecord.Removed {
        _, err := logtx.Exec(
          "INSERT OR IGNORE INTO event_logs(address, topic0, topic1, topic2, topic3, topic4, data, blockNumber, transactionHash, transactionIndex, blockHash, logIndex) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);",
          trimPrefix(logRecord.Address.Bytes()),
          trimPrefix(getTopicIndex(logRecord.Topics, 0)),
          trimPrefix(getTopicIndex(logRecord.Topics, 1)),
          trimPrefix(getTopicIndex(logRecord.Topics, 2)),
          trimPrefix(getTopicIndex(logRecord.Topics, 3)),
          trimPrefix(getTopicIndex(logRecord.Topics, 4)),
          logRecord.Data,
          logRecord.BlockNumber,
          trimPrefix(logRecord.TxHash.Bytes()),
          logRecord.TxIndex,
          trimPrefix(logRecord.BlockHash.Bytes()),
          logRecord.Index,
        )
        if err != nil {
          log.Printf("WARN: (insert) %v", err.Error())
          time.Sleep(time.Millisecond)
          continue
        }
        highestBlock = logRecord.BlockNumber
        break
        } else {
          _, err := logtx.Exec(
            "DELETE FROM event_logs WHERE blockHash = ? AND logIndex = ?",
            trimPrefix(logRecord.BlockHash.Bytes()),
            logRecord.Index,
          )
          if err != nil {
            log.Printf("WARN: (delete) %v", err.Error())
            continue
          }
          break
        }
    }
  }
}
