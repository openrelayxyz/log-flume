package indexer

import (
  "bytes"
  "context"
  "github.com/openrelayxyz/flume/flumeserver/logfeed"
  "database/sql"
  "github.com/ethereum/go-ethereum/core/types"
  "github.com/ethereum/go-ethereum/common"
  "log"
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

func ProcessFeed(feed logfeed.Feed, db *sql.DB) {
  logCh := make(chan types.Log, 1000)
  logSub := feed.SubscribeLogs(logCh)
  defer logSub.Unsubscribe()
  logtx, err := db.BeginTx(context.Background(), nil)
  var logRecord types.Log
  started := false
  counter := 0
  for {
    select {
    case logRecord = <- logCh:
    default:
      if started {
        log.Printf("Committing %v logs", counter)
        if err := logtx.Commit(); err != nil {
          log.Printf("WARN: Transaction commit failed: %v", err.Error())
        }
        counter = 0
        logtx, err = db.BeginTx(context.Background(), nil)
        if err != nil {
          log.Printf("WARN: Failed to start transaction: %v", err.Error())
        }
      }
      logRecord = <- logCh
    }
    // log.Printf("Processing log")
    counter++
    started = true
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
      if err != nil { log.Printf("WARN: (insert) %v", err.Error()) }
    } else {
      _, err := logtx.Exec(
        "DELETE FROM event_logs WHERE blockHash = ? AND logIndex = ?",
        logRecord.BlockHash,
        logRecord.Index,
      )
      if err != nil { log.Printf("WARN: (delete) %v", err.Error()) }
    }
  }
}
