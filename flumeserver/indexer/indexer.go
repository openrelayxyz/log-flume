package indexer

import (
  "bytes"
  "context"
  // "github.com/openrelayxyz/flume/flumeserver/logfeed"
  "github.com/openrelayxyz/flume/flumeserver/datafeed"
  "database/sql"
  "github.com/ethereum/go-ethereum/core/types"
  "github.com/ethereum/go-ethereum/common"
  "log"
  // "time"
  "compress/zlib"
  "io/ioutil"
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

func compress(data []byte) []byte {
  if len(data) == 0 { return data }
  var b bytes.Buffer
  w := zlib.NewWriter(&b)
  w.Write(data)
  w.Close()
  return b.Bytes()
}

func decompress(data []byte) ([]byte, error) {
  if len(data) == 0 { return data, nil }
  r, err := zlib.NewReader(bytes.NewBuffer(data))
  if err != nil { return []byte{}, err }
  return ioutil.ReadAll(r)
}

func getFuncSig(data []byte) ([]byte) {
  if len(data) >= 4 {
    return data[:4]
  }
  return data[:len(data)]
}

func ProcessDataFeed(feed datafeed.DataFeed, db *sql.DB, quit <-chan struct{}, eip155Block, homesteadBlock uint64) {
  log.Printf("Processing data feed")
  ch := make(chan *datafeed.ChainEvent, 10)
  sub := feed.Subscribe(ch)
  defer sub.Unsubscribe()
  for {
    select {
    case <-quit:
      return
    case chainEvent := <- ch:
      BLOCKLOOP:
      for {
        dbtx, _ := db.BeginTx(context.Background(), nil)
        if err := chainEvent.Commit(dbtx); err != nil {
          dbtx.Rollback()
          stats := db.Stats()
          log.Printf("WARN: Failed to commit chainEvent: %v", err.Error())
          log.Printf("SQLite Pool - Open: %v InUse: %v Idle: %v", stats.OpenConnections, stats.InUse, stats.Idle)
          continue BLOCKLOOP
        }
        deleteRes, err := dbtx.Exec("DELETE FROM transactions WHERE blockNumber >= ?;", chainEvent.Block.NumberU64())
        if err != nil {
          dbtx.Rollback()
          stats := db.Stats()
          log.Printf("WARN: Failed to cleanup reorged transactions: %v", err.Error())
          log.Printf("SQLite Pool - Open: %v InUse: %v Idle: %v", stats.OpenConnections, stats.InUse, stats.Idle)
          continue BLOCKLOOP
        }
        if count, _ := deleteRes.RowsAffected(); count > 0 {
          log.Printf("Deleted %v records for blocks >= %v", count, chainEvent.Block.NumberU64())
        }
        var signer types.Signer
        for _, txwr := range chainEvent.TxWithReceipts() {
          switch {
          case chainEvent.Block.NumberU64() > eip155Block:
            signer = types.NewEIP155Signer(txwr.Transaction.ChainId())
          case chainEvent.Block.NumberU64() > homesteadBlock:
            signer = types.HomesteadSigner{}
          default:
            signer = types.FrontierSigner{}
          }
          v, r, s := txwr.Transaction.RawSignatureValues()
          sender, err := types.Sender(signer, txwr.Transaction)
          if err != nil {
            dbtx.Rollback()
            stats := db.Stats()
            log.Printf("WARN: Failed to derive sender: %v", err.Error())
            log.Printf("SQLite Pool - Open: %v InUse: %v Idle: %v", stats.OpenConnections, stats.InUse, stats.Idle)
            continue BLOCKLOOP
          }
          var to []byte
          if txwr.Transaction.To() != nil {
            to = trimPrefix(txwr.Transaction.To().Bytes())
          }
          // log.Printf("Inserting transaction %#x", txwr.Transaction.Hash())
          result, err := dbtx.Exec("INSERT INTO transactions(blockHash, blockNumber, gas, gasPrice, hash, input, nonce, recipient, transactionIndex, value, v, r, s, sender, func, contractAddress, cumulativeGasUsed, gasUsed, logsBloom, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            trimPrefix(chainEvent.Block.Hash().Bytes()),
            chainEvent.Block.NumberU64(),
            txwr.Transaction.Gas(),
            txwr.Transaction.GasPrice().Uint64(),
            trimPrefix(txwr.Transaction.Hash().Bytes()),
            compress(txwr.Transaction.Data()),
            txwr.Transaction.Nonce(),
            to,
            txwr.Receipt.TransactionIndex,
            trimPrefix(txwr.Transaction.Value().Bytes()),
            v.Int64(),
            trimPrefix(r.Bytes()),
            trimPrefix(s.Bytes()),
            trimPrefix(sender.Bytes()),
            getFuncSig(txwr.Transaction.Data()),
            trimPrefix(txwr.Receipt.ContractAddress.Bytes()),
            txwr.Receipt.CumulativeGasUsed,
            txwr.Receipt.GasUsed,
            compress(txwr.Receipt.Bloom.Bytes()),
            txwr.Receipt.Status,
          )
          if err != nil {
            dbtx.Rollback()
            stats := db.Stats()
            log.Printf("WARN: Failed to insert transaction: %v", err.Error())
            log.Printf("SQLite Pool - Open: %v InUse: %v Idle: %v", stats.OpenConnections, stats.InUse, stats.Idle)
            continue BLOCKLOOP
          }
          insertID, err := result.LastInsertId()
          if err != nil {
            dbtx.Rollback()
            stats := db.Stats()
            log.Printf("WARN: Failed to insert transaction: %v", err.Error())
            log.Printf("SQLite Pool - Open: %v InUse: %v Idle: %v", stats.OpenConnections, stats.InUse, stats.Idle)
            continue BLOCKLOOP
          }
          for _, logRecord := range txwr.Receipt.Logs {
            _, err := dbtx.Exec(
              "INSERT OR IGNORE INTO event_logs(address, topic0, topic1, topic2, topic3, topic4, data, tx, logIndex) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);",
              trimPrefix(logRecord.Address.Bytes()),
              trimPrefix(getTopicIndex(logRecord.Topics, 0)),
              trimPrefix(getTopicIndex(logRecord.Topics, 1)),
              trimPrefix(getTopicIndex(logRecord.Topics, 2)),
              trimPrefix(getTopicIndex(logRecord.Topics, 3)),
              trimPrefix(getTopicIndex(logRecord.Topics, 4)),
              compress(logRecord.Data),
              insertID,
              logRecord.Index,
            )
            if err != nil {
              dbtx.Rollback()
              stats := db.Stats()
              log.Printf("WARN: Failed to insert logs: %v", err.Error())
              log.Printf("SQLite Pool - Open: %v InUse: %v Idle: %v", stats.OpenConnections, stats.InUse, stats.Idle)
              continue BLOCKLOOP
            }
          }
        }
        if err := dbtx.Commit(); err != nil {
          stats := db.Stats()
          log.Printf("WARN: Failed to insert logs: %v", err.Error())
          log.Printf("SQLite Pool - Open: %v InUse: %v Idle: %v", stats.OpenConnections, stats.InUse, stats.Idle)
          continue BLOCKLOOP
        }
        log.Printf("Committed Block %v (%#x)", chainEvent.Block.NumberU64(), chainEvent.Block.Hash().Bytes())
        break
      }
    }
  }
}
