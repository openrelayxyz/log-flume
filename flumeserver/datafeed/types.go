package datafeed

import (
  "database/sql"
  "github.com/ethereum/go-ethereum/core/types"
  "github.com/ethereum/go-ethereum/common"
  "github.com/ethereum/go-ethereum/event"
)

type receiptMeta struct {
  contractAddress common.Address
  cumulativeGasUsed uint64
  gasUsed uint64
  logsBloom types.Bloom
  status uint64
}

type ChainEvent struct {
  Block *types.Block
  Commit func(*sql.Tx) error
  receiptMeta map[common.Hash]*receiptMeta
  logs map[common.Hash][]*types.Log
}

func (ce *ChainEvent) Logs() []*types.Log {
  logs := []*types.Log{}
  for _, tx := range ce.Block.Transactions() {
    logs = append(logs, ce.logs[tx.Hash()]...)
  }
  return logs
}

type TxWithReceipt struct {
  Transaction *types.Transaction
  Receipt *types.Receipt
}

func (ce *ChainEvent) TxWithReceipts() []*TxWithReceipt {
  txs := ce.Transactions()
  receipts := ce.Receipts()
  results := make([]*TxWithReceipt, len(txs))
  for i := range txs {
    results[i] = &TxWithReceipt{Transaction: txs[i], Receipt: receipts[i]}
  }
  return results
}

func (ce *ChainEvent) Receipts() []*types.Receipt {
  receipts := make([]*types.Receipt, len(ce.Block.Transactions()))
  for i, tx := range ce.Block.Transactions() {
    meta := ce.receiptMeta[tx.Hash()]
    receipts[i] = &types.Receipt{
      Status: meta.status,
      CumulativeGasUsed: meta.cumulativeGasUsed,
      Bloom: meta.logsBloom,
      Logs: ce.logs[tx.Hash()],
      TxHash: tx.Hash(),
      ContractAddress: meta.contractAddress,
      GasUsed: meta.gasUsed,
      BlockHash: ce.Block.Hash(),
      BlockNumber: ce.Block.Number(),
      TransactionIndex: uint(i),
    }
  }
  return receipts
}

func (ce *ChainEvent) Transactions() types.Transactions {
  return ce.Block.Transactions()
}

type DataFeed interface{
  Close()
  Subscribe(chan *ChainEvent) event.Subscription
  Ready() <-chan struct{}
}

type NullDataFeed struct {}


func (*NullDataFeed) Close() {}
func (*NullDataFeed) Subscribe(chan *ChainEvent) event.Subscription { return nil }
func (*NullDataFeed) Ready() <-chan struct{} { return make(chan struct{}) }
