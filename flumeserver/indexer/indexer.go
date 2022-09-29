package indexer

import (
  "fmt"
  "strings"
  "bytes"
  "context"
  // "github.com/openrelayxyz/flume/flumeserver/logfeed"
  "github.com/openrelayxyz/flume/flumeserver/datafeed"
  "github.com/openrelayxyz/flume/flumeserver/txfeed"
  "database/sql"
  "github.com/ethereum/go-ethereum/core/types"
  "github.com/ethereum/go-ethereum/common"
  "github.com/ethereum/go-ethereum/common/hexutil"
  "github.com/ethereum/go-ethereum/rlp"
  "github.com/ethereum/go-ethereum/event"
  "log"
  "time"
  "github.com/klauspost/compress/zlib"
  // "io/ioutil"
  "sync"
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
    return trimPrefix(topics[idx].Bytes())
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
  compressor.Close()
  return compressionBuffer.Bytes()
}

func getCopy(in []byte) []byte {
  out := make([]byte, len(in))
  copy(out, in)
  return out
}

func getFuncSig(data []byte) ([]byte) {
  if len(data) >= 4 {
    return data[:4]
  }
  return data[:len(data)]
}

func nullZeroAddress(addr common.Address) []byte {
  if addr == (common.Address{}) {
    return []byte{}
  }
  return addr.Bytes()
}

type bytesable interface {
  Bytes() []byte
}

// applyParameters applies a set of parameters into a SQL statement in a manner
// that will be safe for execution. Note that this should only be used in the
// context of blocks, transactions, and logs - beyond the datatypes used in
// those datatypes, safety is not guaranteed.
func applyParameters(query string, params ...interface{}) string {
  preparedParams := make([]interface{}, len(params))
  for i, param := range params {
    switch value := param.(type) {
    case []byte:
      if len(value) == 0 {
        preparedParams[i] = "NULL"
      } else {
        preparedParams[i] = fmt.Sprintf("X'%x'", value)
      }
    case bytesable:
      b := trimPrefix(value.Bytes())
      if len(b) == 0 {
        preparedParams[i] = "NULL"
      } else {
        preparedParams[i] = fmt.Sprintf("X'%x'", b)
      }
    case hexutil.Bytes:
      if len(value) == 0 {
        preparedParams[i] = "NULL"
      } else {
        preparedParams[i] = fmt.Sprintf("X'%x'", []byte(value[:]))
      }
    case *hexutil.Big:
      if value == nil {
        preparedParams[i] = "NULL"
      } else {
        preparedParams[i] = fmt.Sprintf("X'%x'", trimPrefix(value.ToInt().Bytes()))
      }
    case hexutil.Uint64:
      preparedParams[i] = fmt.Sprintf("%v", uint64(value))
    case types.BlockNonce:
      preparedParams[i] = fmt.Sprintf("%v", int64(value.Uint64()))
    default:
      // log.Printf("WARNING: Unknown type passed to applyParameters: %v", value)
      preparedParams[i] = fmt.Sprintf("%v", value)
    }
  }
  return fmt.Sprintf(query, preparedParams...)
}
// eip155block and homesteadBlock and contingent logic are probably unnecessary and can be eliminated
func ProcessDataFeed(feed datafeed.DataFeed, completionFeed event.Feed, txFeed *txfeed.TxFeed, db *sql.DB, quit <-chan struct{}, eip155Block, homesteadBlock uint64, mut *sync.RWMutex, mempoolSlots int) {
  log.Printf("Processing data feed")
  ch := make(chan *datafeed.ChainEvent, 10)
  sub := feed.Subscribe(ch)
  processed := false
  defer sub.Unsubscribe()
  for {
    select {
    case <-quit:
      if !processed {
        panic("Shutting down without processing any blocks")
      }
      log.Printf("Shutting down index process")
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
        // dstart := time.Now()

        // Post refactor, we will need to do event_logs, transactions, and any other tables
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
        // log.Printf("Spent %v deleting reorged data", time.Since(dstart))
        uncles, _ := rlp.EncodeToBytes(chainEvent.Block.Uncles)
        statements := []string{}
        statements = append(statements, applyParameters(
          "INSERT INTO blocks(number, hash, parentHash, uncleHash, coinbase, root, txRoot, receiptRoot, bloom, difficulty, gasLimit, gasUsed, `time`, extra, mixDigest, nonce, uncles, size, td, baseFee) VALUES (%v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v)",
          chainEvent.Block.Number.ToInt().Int64(),
          chainEvent.Block.Hash,
          chainEvent.Block.ParentHash,
          chainEvent.Block.Sha3Uncles,
          chainEvent.Block.Coinbase,
          chainEvent.Block.StateRoot,
          chainEvent.Block.TransactionsRoot,
          chainEvent.Block.ReceiptRoot,
          compress(chainEvent.Block.LogsBloom),
          chainEvent.Block.Difficulty.ToInt().Int64(),
          chainEvent.Block.GasLimit,
          chainEvent.Block.GasUsed,
          chainEvent.Block.Timestamp,
          chainEvent.Block.ExtraData,
          chainEvent.Block.MixHash,
          chainEvent.Block.Nonce,
          uncles, // rlp
          chainEvent.Block.Size,
          chainEvent.Block.TotalDifficulty.ToInt().Bytes(),
          chainEvent.Block.BaseFee,
        ))
        for _, txwr := range chainEvent.TxWithReceipts() {
          txHash := txwr.Transaction.Hash()
          for _, logRecord := range txwr.Receipt.Logs {
            statements = append(statements, applyParameters(
              "INSERT OR IGNORE INTO event_logs(address, topic0, topic1, topic2, topic3, data, block, logIndex, transactionHash, transactionIndex, blockhash) VALUES (%v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v)",
              logRecord.Address,
              getTopicIndex(logRecord.Topics, 0),
              getTopicIndex(logRecord.Topics, 1),
              getTopicIndex(logRecord.Topics, 2),
              getTopicIndex(logRecord.Topics, 3),
              compress(logRecord.Data),
              chainEvent.Block.Number.ToInt().Int64(),
              logRecord.Index,
              txHash,
              txwr.Receipt.TransactionIndex,
              chainEvent.Block.Hash,
            ))
          }
        }
        // istart := time.Now()
        mut.Lock()
        if _, err := dbtx.Exec(strings.Join(statements, " ; ")); err != nil {
          dbtx.Rollback()
          stats := db.Stats()
          log.Printf("WARN: Failed to insert logs: %v", err.Error())
          log.Printf("SQLite Pool - Open: %v InUse: %v Idle: %v", stats.OpenConnections, stats.InUse, stats.Idle)
          mut.Unlock()
          continue BLOCKLOOP
        }
        // log.Printf("Spent %v on %v inserts", time.Since(istart), len(statements))
        // cstart := time.Now()
        if err := dbtx.Commit(); err != nil {
          stats := db.Stats()
          log.Printf("WARN: Failed to insert logs: %v", err.Error())
          log.Printf("SQLite Pool - Open: %v InUse: %v Idle: %v", stats.OpenConnections, stats.InUse, stats.Idle)
          mut.Unlock()
          continue BLOCKLOOP
        }
        mut.Unlock()
        processed = true
        completionFeed.Send(chainEvent.Block.Hash)
        // log.Printf("Spent %v on commit", time.Since(cstart))
        log.Printf("Committed Block %v (%#x) in %v (age %v)", uint64(chainEvent.Block.Number.ToInt().Int64()), chainEvent.Block.Hash.Bytes(), time.Since(start), time.Since(time.Unix(int64(chainEvent.Block.Timestamp), 0)))
        break
      }
    }
  }
}
