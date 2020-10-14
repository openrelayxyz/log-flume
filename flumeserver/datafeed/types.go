package datafeed

import (
  "database/sql"
  // "math/big"
  "github.com/ethereum/go-ethereum/core/types"
  "github.com/ethereum/go-ethereum/common"
  "github.com/ethereum/go-ethereum/common/hexutil"
  "github.com/ethereum/go-ethereum/event"
)

type receiptMeta struct {
  contractAddress common.Address
  cumulativeGasUsed uint64
  gasUsed uint64
  logsBloom types.Bloom
  status uint64
}

// In websockets.go, load this via query
// In kafka.go, load this from the chainEvent
type miniBlock struct {
  Difficulty hexutil.Big  `json:"difficulty"`
  ExtraData []byte `json:"extraData"`
  GasLimit hexutil.Uint64 `json:"gasLimit"`
  GasUsed hexutil.Uint64 `json:"gasUsed"`
  Hash  common.Hash `json:"hash"`
  LogsBloom hexutil.Bytes  `json:"logsBloom"`
  Coinbase common.Address  `json:"miner"`
  MixHash common.Hash `json:"mixHash"`
  Nonce hexutil.Uint64 `json:"nonce"`
  Number hexutil.Big  `json:"number"`
  ParentHash common.Hash  `json:"parentHash"`
  ReceiptRoot common.Hash  `json:"receiptsRoot"`
  Sha3Uncles common.Hash  `json:"sha3Uncles"`
  Size hexutil.Uint64  `json:"size"`
  StateRoot common.Hash  `json:"stateRoot"`
  Timestamp hexutil.Uint64  `json:"timestamp"`
  TotalDifficulty hexutil.Big `json:"totalDifficulty"`
  Transactions []*types.Transaction `json:"transactions"`
  TransactionsRoot common.Hash `json:"transactionsRoot"`
  Uncles []common.Hash  `json:"uncles"`
}

// TODO: Save the fields off the block that we actually need.
type ChainEvent struct {
  Block *miniBlock
  Commit func(*sql.Tx) error
  receiptMeta map[common.Hash]*receiptMeta
  logs map[common.Hash][]*types.Log
}

func (ce *ChainEvent) Logs() []*types.Log {
  logs := []*types.Log{}
  for _, tx := range ce.Block.Transactions {
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
  receipts := make([]*types.Receipt, len(ce.Block.Transactions))
  for i, tx := range ce.Block.Transactions {
    meta := ce.receiptMeta[tx.Hash()]
    receipts[i] = &types.Receipt{
      Status: meta.status,
      CumulativeGasUsed: meta.cumulativeGasUsed,
      Bloom: meta.logsBloom,
      Logs: ce.logs[tx.Hash()],
      TxHash: tx.Hash(),
      ContractAddress: meta.contractAddress,
      GasUsed: meta.gasUsed,
      BlockHash: ce.Block.Hash,
      BlockNumber: ce.Block.Number.ToInt(),
      TransactionIndex: uint(i),
    }
  }
  return receipts
}

func (ce *ChainEvent) Transactions() types.Transactions {
  return ce.Block.Transactions
}

type DataFeed interface{
  Close()
  Subscribe(chan *ChainEvent) event.Subscription
  Ready() <-chan struct{}
}

type NullDataFeed struct {}


func (*NullDataFeed) Close() {}
func (*NullDataFeed) Subscribe(chan *ChainEvent) event.Subscription { return &NullSubscription{} }
func (*NullDataFeed) Ready() <-chan struct{} {
  ch := make(chan struct{}, 1)
  ch <- struct{}{}
  return ch
}


type NullSubscription struct {}

func (s *NullSubscription) Err() <-chan error {
  return make(chan error)
}
func (s *NullSubscription) Unsubscribe() {}
