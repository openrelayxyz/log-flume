package indexer

import (
  "bytes"
  "context"
  // "github.com/openrelayxyz/flume/flumeserver/logfeed"
  "github.com/openrelayxyz/flume/flumeserver/datafeed"
  "database/sql"
  "github.com/ethereum/go-ethereum/core/types"
  "github.com/ethereum/go-ethereum/common"
  "github.com/ethereum/go-ethereum/crypto"
  "github.com/ethereum/go-ethereum/rlp"
  "log"
  "time"
  "compress/zlib"
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

func compress(data []byte) []byte {
  if len(data) == 0 { return data }
  b := bytes.NewBuffer(make([]byte, 0, 5 * 1024 * 1024))
  if compressor == nil {
    compressor = zlib.NewWriter(b)
  } else {
    compressor.Reset(b)
  }
  compressor.Write(data)
  compressor.Flush()
  return b.Bytes()
}

func getFuncSig(data []byte) ([]byte) {
  if len(data) >= 4 {
    return data[:4]
  }
  return data[:len(data)]
}

func ProcessDataFeed(feed datafeed.DataFeed, confirmedRetentionLimit int, retentionDuration time.Duration, db, mempool *sql.DB, quit <-chan struct{}, eip155Block, homesteadBlock uint64) {
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
        memtx, err := mempool.BeginTx(context.Background(), nil)
        if err != nil { log.Printf("Error creating a mempool transaction: %v", err.Error())}
        dbtx, err := db.BeginTx(context.Background(), nil)
        if err != nil { log.Fatalf("Error creating a transaction: %v", err.Error())}
        if err := chainEvent.Commit(dbtx); err != nil {
          dbtx.Rollback()
          stats := db.Stats()
          log.Printf("WARN: Failed to commit chainEvent: %v", err.Error())
          log.Printf("SQLite Pool - Open: %v InUse: %v Idle: %v", stats.OpenConnections, stats.InUse, stats.Idle)
          continue BLOCKLOOP
        }
        bn := chainEvent.Block.Number.ToInt().Int64()
        memtx.Exec("UPDATE pending_transactions SET block = NULL, confirmationDuration = NULL WHERE block >= ?;", bn)
        memtx.Exec("DELETE FROM pending_transactions WHERE (block != NULL AND block < ?) OR (block == NULL AND seenTimestamp < ?);", bn - int64(confirmedRetentionLimit), time.Now().Add(-retentionDuration).Unix())
        deleteRes, err := dbtx.Exec("DELETE FROM blocks WHERE number >= ?;", bn)
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
          bn,
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
          memtx.Exec("UPDATE pending_transactions SET block = ?, confirmationDuration = ? - seenTimestamp WHERE hash = ?;", trimPrefix(txwr.Transaction.Hash().Bytes()))
          memtx.Exec("DELETE FROM pending_transactions WHERE sender = ? AND nonce = ? AND hash != ?", trimPrefix(sender.Bytes()), txwr.Transaction.Nonce(), trimPrefix(txwr.Transaction.Hash().Bytes()))
          // log.Printf("Inserting transaction %#x", txwr.Transaction.Hash())
          result, err := dbtx.Exec("INSERT INTO transactions(block, gas, gasPrice, hash, input, nonce, recipient, transactionIndex, value, v, r, s, sender, func, contractAddress, cumulativeGasUsed, gasUsed, logsBloom, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
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
            _, err := dbtx.Exec(
              "INSERT OR IGNORE INTO event_logs(address, topic0, topic1, topic2, topic3, topic4, data, tx, block, logIndex) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);",
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
        log.Printf("Committed Block %v (%#x)", uint64(chainEvent.Block.Number.ToInt().Int64()), chainEvent.Block.Hash.Bytes())
        break
      }
    }
  }
}


func ProcessMempool(feed datafeed.TransactionFeed, mempool *sql.DB, maxMempoolSize int) {
  go func() {
    for range time.NewTicker(time.Minute).C {
      // Delete any pending transactions where
      mempool.Exec("DELETE FROM pending_transactions WHERE block == NULL AND gasPrice < (SELECT MIN(gasPrice) FROM pending_transactions WHERE 1 ORDER BY gasPrice DESC LIMIT ?);", maxMempoolSize);
    }
  }()
  go func() {
    for txWithTs := range feed.Messages() {
      tx := txWithTs.Tx
      ts := txWithTs.Timestamp
      signer := types.NewEIP155Signer(tx.ChainId())
      sender, _ := types.Sender(signer, tx)
      v, r, s := tx.RawSignatureValues()
      var to, contractAddress []byte
      if tx.To() != nil {
        to = trimPrefix(tx.To().Bytes())
      } else {
        contractAddress = trimPrefix(crypto.CreateAddress(sender, tx.Nonce()).Bytes())
      }
      mempool.Exec("INSERT OR IGNORE INTO pending_transactions(gas, gasPrice, hash, input, nonce, recipient, value, v, r, s, sender, contractAddress, seenTimestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);",
        tx.Gas(),
        tx.GasPrice().Uint64(),
        trimPrefix(tx.Hash().Bytes()),
        compress(tx.Data()),
        tx.Nonce(),
        to,
        trimPrefix(tx.Value().Bytes()),
        v.Int64(),
        trimPrefix(r.Bytes()),
        trimPrefix(s.Bytes()),
        trimPrefix(sender.Bytes()),
        contractAddress,
        ts.Unix(),
      )
    }
  }()
}
