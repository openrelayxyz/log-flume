package indexer

import (
  "strings"
  "context"
  "math/big"
  "github.com/openrelayxyz/flume/flumeserver/datafeed"
  "database/sql"
  "github.com/ethereum/go-ethereum/core/types"
  "github.com/ethereum/go-ethereum/common"
  "github.com/ethereum/go-ethereum/common/math"
  "github.com/ethereum/go-ethereum/rlp"
	log "github.com/inconshreveable/log15"
  "time"
  "sync"
)

func legacy_indexer(db *sql.DB, eip155Block, homesteadBlock uint64, mut *sync.RWMutex, chainEvent *datafeed.ChainEvent, processed bool) error {
      start := time.Now()
      BLOCKLOOP:
      for {
        dbtx, err := db.BeginTx(context.Background(), nil)
        if err != nil { log.Error("Error creating a transaction:", "Error:", err.Error())}
        if err := chainEvent.Commit(dbtx); err != nil {
          dbtx.Rollback()
          stats := db.Stats()
          log.Warn("Failed to commit chainEvent:", "chanevent", err.Error())
          log.Info("SQLite Pool - Open:", "InUse:", stats.OpenConnections, "idle:", stats.InUse, stats.Idle)
          continue BLOCKLOOP
        }
        dstart := time.Now()
        deleteRes, err := dbtx.Exec("DELETE FROM blocks WHERE number >= ?;", chainEvent.Block.Number.ToInt().Int64())
        if err != nil {
          dbtx.Rollback()
          stats := db.Stats()
          log.Warn("Failed to cleanup reorged transactions", "error:", err.Error())
          log.Info("SQLite Pool", "Open:", stats.OpenConnections, "In use:", stats.InUse, "Idle:", stats.Idle)
          continue BLOCKLOOP
        }
        if count, _ := deleteRes.RowsAffected(); count > 0 {
          log.Info("Deleted %v records for blocks >= %v", "count:", count, "Blocks >=:", chainEvent.Block.Number.ToInt().Int64())
        }
        log.Debug("Deleting reorged data", "time spent", time.Since(dstart))
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
              log.Warn("Failed to derive sender", "Error:", err.Error())
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
            log.Warn("Failed to derive sender.", "Sender is nill", sender)
            log.Info("SQLite Pool:", "Open:", stats.OpenConnections, "In use:", stats.InUse, "Idle:", stats.Idle)
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
        mut.Lock()
        if _, err := dbtx.Exec(strings.Join(statements, " ; ")); err != nil {
          dbtx.Rollback()
          stats := db.Stats()
          log.Warn("Failed to insert logs", "Error:", err.Error())
          log.Info("SQLite Pool:", "Open:", stats.OpenConnections, "In use:", stats.InUse, "Idle:", stats.Idle)
          mut.Unlock()
          continue BLOCKLOOP
        }
        // log.Printf("Spent %v on %v inserts", time.Since(istart), len(statements))
        // cstart := time.Now()
        if err := dbtx.Commit(); err != nil {
          stats := db.Stats()
          log.Warn("Failed to insert logs.", "Error:", err.Error())
          log.Info("SQLite Pool:", "Open:", stats.OpenConnections, "In Use:", stats.InUse, "Idle:", stats.Idle)
          mut.Unlock()
          continue BLOCKLOOP
        }
        mut.Unlock()
        processed = true
        // completionFeed.Send(chainEvent.Block.Hash)
        // log.Printf("Spent %v on commit", time.Since(cstart))
        log.Info("Committed Block:", "Number:", uint64(chainEvent.Block.Number.ToInt().Int64()), "Hash:", chainEvent.Block.Hash.Bytes(), "Time:", time.Since(start), time.Since(time.Unix(int64(chainEvent.Block.Timestamp), 0)))
        break
      }
			return nil
    }
