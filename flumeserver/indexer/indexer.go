package indexer

import (
  "bytes"
  "context"
  // "github.com/openrelayxyz/flume/flumeserver/logfeed"
  "github.com/openrelayxyz/flume/flumeserver/datafeed"
  "database/sql"
  "github.com/ethereum/go-ethereum/core/types"
  "github.com/ethereum/go-ethereum/common"
  "github.com/ethereum/go-ethereum/rlp"
  "log"
  "time"
  "github.com/klauspost/compress/zlib"
  // "io/ioutil"
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

var compressor *zlib.Writer
var compressionBuffer = bytes.NewBuffer(make([]byte, 0, 5 * 1024 * 1024))

func compress(data []byte) []byte {
  if len(data) == 0 { return data }
  compressionBuffer.Reset()
  if compressor == nil {
    compressor = zlib.NewWriter(compressionBuffer)
  } else {
    compressor.Reset(compressionBuffer)
  }
  compressor.Write(data)
  compressor.Flush()
  return compressionBuffer.Bytes()
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
      start := time.Now()
      BLOCKLOOP:
      for {
        dbtx, err := db.BeginTx(context.Background(), nil)
        if err != nil { log.Fatalf("Error creating a transaction: %v", err.Error())}
        if err := chainEvent.Commit(dbtx); err != nil {
          dbtx.Rollback()
          stats := db.Stats()
          log.Printf("WARN: Failed to commit chainEvent: %v", err.Error())
          log.Printf("SQLite Pool - Open: %v InUse: %v Idle: %v", stats.OpenConnections, stats.InUse, stats.Idle)
          continue BLOCKLOOP
        }
        deleteRes, err := dbtx.Exec("DELETE FROM blocks WHERE number >= ?;", chainEvent.Block.Number.ToInt().Int64())
        if err != nil {
          dbtx.Rollback()
          stats := db.Stats()
          log.Printf("WARN: Failed to cleanup reorged transactions: %v", err.Error())
          log.Printf("SQLite Pool - Open: %v InUse: %v Idle: %v", stats.OpenConnections, stats.InUse, stats.Idle)
          continue BLOCKLOOP
        }
        if count, _ := deleteRes.RowsAffected(); count > 0 {
          log.Printf("Deleted %v records for blocks >= %v", count, chainEvent.Block.Number.ToInt().Int64())
        }
        uncles, _ := rlp.EncodeToBytes(chainEvent.Block.Uncles)
        _, err = dbtx.Exec("INSERT INTO blocks(number, hash, parentHash, uncleHash, coinbase, root, txRoot, receiptRoot, bloom, difficulty, gasLimit, gasUsed, time, extra, mixDigest, nonce, uncles, size, td) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);",
          chainEvent.Block.Number.ToInt().Int64(),
          trimPrefix(chainEvent.Block.Hash.Bytes()),
          trimPrefix(chainEvent.Block.ParentHash.Bytes()),
          trimPrefix(chainEvent.Block.Sha3Uncles.Bytes()),
          trimPrefix(chainEvent.Block.Coinbase.Bytes()),
          trimPrefix(chainEvent.Block.StateRoot.Bytes()),
          trimPrefix(chainEvent.Block.TransactionsRoot.Bytes()),
          trimPrefix(chainEvent.Block.ReceiptRoot.Bytes()),
          compress(chainEvent.Block.LogsBloom),
          chainEvent.Block.Difficulty.ToInt().Int64(),
          uint64(chainEvent.Block.GasLimit),
          uint64(chainEvent.Block.GasUsed),
          uint64(chainEvent.Block.Timestamp),
          chainEvent.Block.ExtraData,
          trimPrefix(chainEvent.Block.MixHash.Bytes()),
          int64(chainEvent.Block.Nonce.Uint64()),
          uncles,//rlp
          uint64(chainEvent.Block.Size),
          chainEvent.Block.TotalDifficulty.ToInt().Int64(),
        )
        if err != nil {
          dbtx.Rollback()
          stats := db.Stats()
          log.Printf("WARN: Failed to insert block: %v", err.Error())
          log.Printf("SQLite Pool - Open: %v InUse: %v Idle: %v", stats.OpenConnections, stats.InUse, stats.Idle)
          continue BLOCKLOOP
        }
        var signer types.Signer
        senderMap := make(map[common.Hash]<-chan common.Address)
        for _, txwr := range chainEvent.TxWithReceipts() {
          ch := make(chan common.Address, 1)
          senderMap[txwr.Transaction.Hash()] = ch
          go func(tx *types.Transaction, ch chan<- common.Address) {
            switch {
            case uint64(chainEvent.Block.Number.ToInt().Int64()) > eip155Block:
              signer = types.NewEIP155Signer(tx.ChainId())
            case uint64(chainEvent.Block.Number.ToInt().Int64()) > homesteadBlock:
              signer = types.HomesteadSigner{}
            default:
              signer = types.FrontierSigner{}
            }
            sender, err := types.Sender(signer, tx)
            if err != nil {
              log.Printf("WARN: Failed to derive sender: %v", err.Error())
            }
            ch <- sender
          }(txwr.Transaction, ch)
        }
        txstmt, err := dbtx.Prepare("INSERT INTO transactions(block, gas, gasPrice, hash, input, nonce, recipient, transactionIndex, value, v, r, s, sender, func, contractAddress, cumulativeGasUsed, gasUsed, logsBloom, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
        if err != nil {
          dbtx.Rollback()
          stats := db.Stats()
          log.Printf("WARN: Failed to create txstmt: %v", err.Error())
          log.Printf("SQLite Pool - Open: %v InUse: %v Idle: %v", stats.OpenConnections, stats.InUse, stats.Idle)
          continue BLOCKLOOP
        }
        logstmt, err := dbtx.Prepare("INSERT OR IGNORE INTO event_logs(address, topic0, topic1, topic2, topic3, topic4, data, tx, block, logIndex) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);")
        if err != nil {
          dbtx.Rollback()
          stats := db.Stats()
          log.Printf("WARN: Failed to create logstmt: %v", err.Error())
          log.Printf("SQLite Pool - Open: %v InUse: %v Idle: %v", stats.OpenConnections, stats.InUse, stats.Idle)
          continue BLOCKLOOP
        }
        for _, txwr := range chainEvent.TxWithReceipts() {
          v, r, s := txwr.Transaction.RawSignatureValues()
          txHash := txwr.Transaction.Hash()
          sender := <-senderMap[txHash]
          if sender == (common.Address{}) {
            dbtx.Rollback()
            stats := db.Stats()
            log.Printf("WARN: Failed to derive sender.")
            log.Printf("SQLite Pool - Open: %v InUse: %v Idle: %v", stats.OpenConnections, stats.InUse, stats.Idle)
            continue BLOCKLOOP
          }
          var to []byte
          if txwr.Transaction.To() != nil {
            to = trimPrefix(txwr.Transaction.To().Bytes())
          }
          // log.Printf("Inserting transaction %#x", txwr.Transaction.Hash())
          result, err := txstmt.Exec(
            chainEvent.Block.Number.ToInt().Int64(),
            txwr.Transaction.Gas(),
            txwr.Transaction.GasPrice().Uint64(),
            trimPrefix(txHash.Bytes()),
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
            _, err := logstmt.Exec(
              trimPrefix(logRecord.Address.Bytes()),
              trimPrefix(getTopicIndex(logRecord.Topics, 0)),
              trimPrefix(getTopicIndex(logRecord.Topics, 1)),
              trimPrefix(getTopicIndex(logRecord.Topics, 2)),
              trimPrefix(getTopicIndex(logRecord.Topics, 3)),
              trimPrefix(getTopicIndex(logRecord.Topics, 4)),
              compress(logRecord.Data),
              insertID,
              chainEvent.Block.Number.ToInt().Int64(),
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
        txstmt.Close()
        logstmt.Close()
        log.Printf("Committed Block %v (%#x) in %v", uint64(chainEvent.Block.Number.ToInt().Int64()), chainEvent.Block.Hash.Bytes(), time.Since(start))
        break
      }
    }
  }
}
