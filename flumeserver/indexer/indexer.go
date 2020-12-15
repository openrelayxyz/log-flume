package indexer

import (
  "fmt"
  "strings"
  "bytes"
  "context"
  // "github.com/openrelayxyz/flume/flumeserver/logfeed"
  "github.com/openrelayxyz/flume/flumeserver/datafeed"
  "database/sql"
  "github.com/ethereum/go-ethereum/core/types"
  "github.com/ethereum/go-ethereum/common"
  "github.com/ethereum/go-ethereum/common/hexutil"
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

type bytesable interface {
  Bytes() []byte
}

func applyParameters(query string, params ...interface{}) string {
  preparedParams := make([]interface{}, len(params))
  for i, param := range params {
    switch value := param.(type) {
    case []byte:
      preparedParams[i] = fmt.Sprintf("X'%x'", value)
    case bytesable:
      preparedParams[i] = fmt.Sprintf("X'%x'", trimPrefix(value.Bytes()))
    case hexutil.Bytes:
      preparedParams[i] = fmt.Sprintf("X'%x'", value[:])
    case hexutil.Big:
      preparedParams[i] = fmt.Sprintf("%v", value.ToInt().Int64())
    case hexutil.Uint64:
      preparedParams[i] = fmt.Sprintf("%v", uint64(value))
    case types.BlockNonce:
      preparedParams[i] = fmt.Sprintf("%v", value.Uint64())
    default:
      preparedParams[i] = fmt.Sprintf("%v", value)
    }
  }
  return fmt.Sprintf(query, preparedParams...)
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
        dstart := time.Now()
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
        log.Printf("Spent %v deleting reorged data", time.Since(dstart))
        uncles, _ := rlp.EncodeToBytes(chainEvent.Block.Uncles)
        statements := []string{}
        statements = append(statements, applyParameters(
          "INSERT INTO blocks(number, hash, parentHash, uncleHash, coinbase, root, txRoot, receiptRoot, bloom, difficulty, gasLimit, gasUsed, time, extra, mixDigest, nonce, uncles, size, td) VALUES (%v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v)",
          chainEvent.Block.Number,
          chainEvent.Block.Hash,
          chainEvent.Block.ParentHash,
          chainEvent.Block.Sha3Uncles,
          chainEvent.Block.Coinbase,
          chainEvent.Block.StateRoot,
          chainEvent.Block.TransactionsRoot,
          chainEvent.Block.ReceiptRoot,
          compress(chainEvent.Block.LogsBloom),
          chainEvent.Block.Difficulty,
          chainEvent.Block.GasLimit,
          chainEvent.Block.GasUsed,
          chainEvent.Block.Timestamp,
          chainEvent.Block.ExtraData,
          chainEvent.Block.MixHash,
          chainEvent.Block.Nonce,
          uncles, // rlp
          chainEvent.Block.Size,
          chainEvent.Block.TotalDifficulty,
        ))
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
          // log.Printf("Inserting transaction %#x", txwr.Transaction.Hash())
          statements = append(statements, applyParameters(
            "INSERT INTO transactions(block, gas, gasPrice, hash, input, nonce, recipient, transactionIndex, value, v, r, s, sender, func, contractAddress, cumulativeGasUsed, gasUsed, logsBloom, status) VALUES (%v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v)",
            chainEvent.Block.Number,
            txwr.Transaction.Gas(),
            txwr.Transaction.GasPrice().Uint64(),
            txHash,
            compress(txwr.Transaction.Data()),
            txwr.Transaction.Nonce(),
            to,
            txwr.Receipt.TransactionIndex,
            trimPrefix(txwr.Transaction.Value().Bytes()),
            v.Int64(),
            r,
            s,
            sender,
            getFuncSig(txwr.Transaction.Data()),
            txwr.Receipt.ContractAddress,
            txwr.Receipt.CumulativeGasUsed,
            txwr.Receipt.GasUsed,
            compress(txwr.Receipt.Bloom.Bytes()),
            txwr.Receipt.Status,
          ))
          for _, logRecord := range txwr.Receipt.Logs {
            statements = append(statements, applyParameters(
              "INSERT OR IGNORE INTO event_logs(address, topic0, topic1, topic2, topic3, topic4, data, block, logIndex, tx) VALUES (%v, %v, %v, %v, %v, %v, %v, %v, %v, (SELECT id FROM transactions WHERE hash = %v))",
              logRecord.Address,
              getTopicIndex(logRecord.Topics, 0),
              getTopicIndex(logRecord.Topics, 1),
              getTopicIndex(logRecord.Topics, 2),
              getTopicIndex(logRecord.Topics, 3),
              getTopicIndex(logRecord.Topics, 4),
              compress(logRecord.Data),
              chainEvent.Block.Number,
              logRecord.Index,
              txHash,
            ))
          }
        }
        istart := time.Now()
        if _, err := dbtx.Exec(strings.Join(statements, " ; ")); err != nil {
          dbtx.Rollback()
          stats := db.Stats()
          log.Printf("WARN: Failed to insert logs: %v", err.Error())
          log.Printf("SQLite Pool - Open: %v InUse: %v Idle: %v", stats.OpenConnections, stats.InUse, stats.Idle)
          continue BLOCKLOOP
        }
        log.Printf("Spent %v on %v inserts", time.Since(istart), len(statements))
        cstart := time.Now()
        if err := dbtx.Commit(); err != nil {
          stats := db.Stats()
          log.Printf("WARN: Failed to insert logs: %v", err.Error())
          log.Printf("SQLite Pool - Open: %v InUse: %v Idle: %v", stats.OpenConnections, stats.InUse, stats.Idle)
          continue BLOCKLOOP
        }
        log.Printf("Spent %v on commit", time.Since(cstart))
        log.Printf("Committed Block %v (%#x) in %v", uint64(chainEvent.Block.Number.ToInt().Int64()), chainEvent.Block.Hash.Bytes(), time.Since(start))
        break
      }
    }
  }
}
