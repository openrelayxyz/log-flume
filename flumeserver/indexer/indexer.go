package indexer

import (
  "fmt"
  "strings"
  "bytes"
  "context"
  "math/big"
  // "github.com/openrelayxyz/flume/flumeserver/logfeed"
  "github.com/openrelayxyz/flume/flumeserver/datafeed"
  "github.com/openrelayxyz/flume/flumeserver/txfeed"
  "database/sql"
  "github.com/ethereum/go-ethereum/core/types"
  "github.com/ethereum/go-ethereum/common"
  "github.com/ethereum/go-ethereum/common/hexutil"
  "github.com/ethereum/go-ethereum/common/math"
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
      preparedParams[i] = fmt.Sprintf("%v", value)
    }
  }
  return fmt.Sprintf(query, preparedParams...)
}

func ProcessDataFeed(feed datafeed.DataFeed, completionFeed event.Feed, txFeed *txfeed.TxFeed, db *sql.DB, quit <-chan struct{}, eip155Block, homesteadBlock uint64, wg *sync.WaitGroup, mempoolSlots int) {
  log.Printf("Processing data feed")
  txCh := make(chan *types.Transaction, 200)
  txSub := txFeed.Subscribe(txCh)
  ch := make(chan *datafeed.ChainEvent, 10)
  sub := feed.Subscribe(ch)
  processed := false
  pruneTicker := time.NewTicker(5 * time.Second)
  txCount := 0
  txDedup := make(map[common.Hash]struct{})
  defer sub.Unsubscribe()
  defer txSub.Unsubscribe()
  db.Exec("DELETE FROM mempool.transactions WHERE (sender, nonce) IN (SELECT sender, nonce FROM transactions)")
  for {
    select {
    case <-quit:
      if !processed {
        panic("Shutting down without processing any blocks")
      }
      log.Printf("Shutting down index process")
      return
    case <- pruneTicker.C:
      txDedup = make(map[common.Hash]struct{})
      db.QueryRow("SELECT count(*) FROM mempool.transactions;").Scan(&txCount)
      if txCount > mempoolSlots {
        pstart := time.Now()
        db.Exec("DELETE FROM mempool.transactions WHERE gasPrice < (SELECT gasPrice FROM mempool.transactions ORDER BY gasPrice LIMIT 1 OFFSET ?)", mempoolSlots)
        log.Printf("Pruned %v transactions from mempool in %v", (txCount - mempoolSlots), time.Since(pstart))
      }
    case tx := <-txCh:
      txHash := tx.Hash()
      if _, ok := txDedup[txHash]; ok {
        continue
      }
      var signer types.Signer
      var accessListRLP []byte
      gasPrice := tx.GasPrice().Uint64()
      switch {
      case tx.Type() == types.AccessListTxType:
        accessListRLP, _ = rlp.EncodeToBytes(tx.AccessList())
        signer = types.NewEIP2930Signer(tx.ChainId())
      case tx.Type() == types.DynamicFeeTxType:
        signer = types.NewLondonSigner(tx.ChainId())
        accessListRLP, _ = rlp.EncodeToBytes(tx.AccessList())
        gasPrice = tx.GasFeeCap().Uint64()
      default:
        signer = types.NewEIP155Signer(tx.ChainId())
      }
      sender, _ := types.Sender(signer, tx)
      var to []byte
      if tx.To() != nil {
        to = trimPrefix(tx.To().Bytes())
      }
      v, r, s := tx.RawSignatureValues()
      statements := []string{}
      // If this is a replacement transaction, delete any it might be replacing
      statements = append(statements, applyParameters(
        "DELETE FROM mempool.transactions WHERE sender = %v AND nonce = %v",
        sender,
        tx.Nonce(),
      ))
      // Insert the transaction
      statements = append(statements, applyParameters(
        "INSERT INTO mempool.transactions(gas, gasPrice, hash, input, nonce, recipient, `value`, v, r, s, sender, `type`, access_list, gasFeeCap, gasTipCap) VALUES (%v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v)",
        tx.Gas(),
        gasPrice,
        txHash,
        getCopy(compress(tx.Data())),
        tx.Nonce(),
        to,
        trimPrefix(tx.Value().Bytes()),
        v.Int64(),
        r,
        s,
        sender,
        tx.Type(),
        compress(accessListRLP),
        trimPrefix(tx.GasFeeCap().Bytes()),
        trimPrefix(tx.GasTipCap().Bytes()),
      ))
      // Delete the transaction we just inserted if the confirmed transactions
      // pool has a conflicting entry
      statements = append(statements, applyParameters(
        "DELETE FROM mempool.transactions WHERE sender = %v AND nonce = %v AND (sender, nonce) IN (SELECT sender, nonce FROM transactions)",
        sender,
        tx.Nonce(),
      ))
      db.Exec(strings.Join(statements, " ; "))
      txCount++
      if txCount > (11 * mempoolSlots / 10) {
        // More than 10% above mempool limit, prune some.
        pstart := time.Now()
        db.QueryRow("DELETE FROM mempool.transactions WHERE gasPrice < (SELECT gasPrice FROM mempool.transactions ORDER BY gasPrice LIMIT 1 OFFSET %v)", mempoolSlots)
        log.Printf("Pruned %v transactions from Mempool in %v", (txCount - mempoolSlots), time.Since(pstart))
      }
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
        var signer types.Signer
        senderMap := make(map[common.Hash]<-chan common.Address)
        for _, txwr := range chainEvent.TxWithReceipts() {
          ch := make(chan common.Address, 1)
          senderMap[txwr.Transaction.Hash()] = ch
          go func(tx *types.Transaction, ch chan<- common.Address) {
            switch {
            case tx.Type() == types.AccessListTxType:
              signer = types.NewEIP2930Signer(tx.ChainId())
            case tx.Type() == types.DynamicFeeTxType:
              signer = types.NewLondonSigner(tx.ChainId())
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
          var accessListRLP []byte
          gasPrice := txwr.Transaction.GasPrice().Uint64()
          switch txwr.Transaction.Type() {
          case types.AccessListTxType:
            accessListRLP, _ = rlp.EncodeToBytes(txwr.Transaction.AccessList())
          case types.DynamicFeeTxType:
            accessListRLP, _ = rlp.EncodeToBytes(txwr.Transaction.AccessList())
            gasPrice = math.BigMin(new(big.Int).Add(txwr.Transaction.GasTipCap(), chainEvent.Block.BaseFee.ToInt()), txwr.Transaction.GasFeeCap()).Uint64()
          }
          // log.Printf("Inserting transaction %#x", txwr.Transaction.Hash())
          statements = append(statements, applyParameters(
            "DELETE FROM mempool.transactions WHERE sender = %v AND nonce = %v", sender, txwr.Transaction.Nonce(),
          ))
          statements = append(statements, applyParameters(
            "INSERT INTO transactions(block, gas, gasPrice, hash, input, nonce, recipient, transactionIndex, `value`, v, r, s, sender, func, contractAddress, cumulativeGasUsed, gasUsed, logsBloom, `status`, `type`, access_list, gasFeeCap, gasTipCap) VALUES (%v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v)",
            chainEvent.Block.Number.ToInt().Int64(),
            txwr.Transaction.Gas(),
            gasPrice,
            txHash,
            getCopy(compress(txwr.Transaction.Data())),
            txwr.Transaction.Nonce(),
            to,
            txwr.Receipt.TransactionIndex,
            trimPrefix(txwr.Transaction.Value().Bytes()),
            v.Int64(),
            r,
            s,
            sender,
            getFuncSig(txwr.Transaction.Data()),
            nullZeroAddress(txwr.Receipt.ContractAddress),
            txwr.Receipt.CumulativeGasUsed,
            txwr.Receipt.GasUsed,
            getCopy(compress(txwr.Receipt.Bloom.Bytes())),
            txwr.Receipt.Status,
            txwr.Transaction.Type(),
            compress(accessListRLP),
            trimPrefix(txwr.Transaction.GasFeeCap().Bytes()),
            trimPrefix(txwr.Transaction.GasTipCap().Bytes()),
          ))
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
        wg.Add(1)
        if _, err := dbtx.Exec(strings.Join(statements, " ; ")); err != nil {
          dbtx.Rollback()
          stats := db.Stats()
          log.Printf("WARN: Failed to insert logs: %v", err.Error())
          log.Printf("SQLite Pool - Open: %v InUse: %v Idle: %v", stats.OpenConnections, stats.InUse, stats.Idle)
          wg.Done()
          continue BLOCKLOOP
        }
        // log.Printf("Spent %v on %v inserts", time.Since(istart), len(statements))
        // cstart := time.Now()
        if err := dbtx.Commit(); err != nil {
          stats := db.Stats()
          log.Printf("WARN: Failed to insert logs: %v", err.Error())
          log.Printf("SQLite Pool - Open: %v InUse: %v Idle: %v", stats.OpenConnections, stats.InUse, stats.Idle)
          wg.Done()
          continue BLOCKLOOP
        }
        wg.Done()
        processed = true
        completionFeed.Send(chainEvent.Block.Hash)
        // log.Printf("Spent %v on commit", time.Since(cstart))
        log.Printf("Committed Block %v (%#x) in %v (age %v)", uint64(chainEvent.Block.Number.ToInt().Int64()), chainEvent.Block.Hash.Bytes(), time.Since(start), time.Since(time.Unix(int64(chainEvent.Block.Timestamp), 0)))
        db.QueryRow("SELECT count(*) FROM mempool.transactions;").Scan(&txCount)
        break
      }
    }
  }
}
