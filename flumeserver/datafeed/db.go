package datafeed

import (
  "database/sql"
  "github.com/ethereum/go-ethereum/core/types"
  "github.com/ethereum/go-ethereum/ethdb"
  "github.com/ethereum/go-ethereum/event"
  "github.com/ethereum/go-ethereum/core/rawdb"
  "github.com/ethereum/go-ethereum/common"
  "github.com/ethereum/go-ethereum/common/hexutil"
  "log"
)

// type DataFeed interface{
//   Close()
//   Subscribe(chan *ChainEvent) event.Subscription
//   Ready() <-chan struct{}
// }

type dbDataFeed struct {
  db ethdb.Database
  feed event.Feed
  startingBlock uint64
  started bool
  ready chan struct{}
}

func (feed *dbDataFeed) Close()  {}
func (feed *dbDataFeed) Subscribe(ch chan *ChainEvent) event.Subscription {
  if !feed.started {
    go feed.subscribe()
    feed.started = true
  }
  sub := feed.feed.Subscribe(ch)
  return sub
}
func (feed *dbDataFeed) Ready() <-chan struct{} {
  return feed.ready
}

func (feed *dbDataFeed) subscribe() {
  genesisHash := rawdb.ReadCanonicalHash(feed.db, 0)
  if genesisHash == (common.Hash{}) {
    log.Fatalf("Genesis hash not available")
  }
  chainConfig := rawdb.ReadChainConfig(feed.db, genesisHash)
  if chainConfig == nil {
    log.Fatalf("Chain config could not be loaded")
  }

  OrderedProcessor(feed.startingBlock, 10, func(n uint64, ch chan<- interface{}, quit func()) {
    h := rawdb.ReadCanonicalHash(feed.db, n)
    if h == (common.Hash{}) {
      quit()
      return
    }
    block := rawdb.ReadBlock(feed.db, h, n)
    if block == nil {
      quit()
      return
    }
    receipts := rawdb.ReadReceipts(feed.db, h, n, chainConfig)
    td := rawdb.ReadTd(feed.db, h, n)
    logs := make(map[common.Hash][]*types.Log)
    if receipts != nil {
      // Receipts will be nil if the list is empty, so this is not an error condition
      for _, receipt := range receipts {
        logs[receipt.TxHash] = receipt.Logs
      }
    }
    ce := &ChainEvent{
      Block: &miniBlock{
        Difficulty: hexutil.Big(*block.Difficulty()),
        ExtraData: block.Extra(),
        GasLimit: hexutil.Uint64(block.GasLimit()),
        GasUsed: hexutil.Uint64(block.GasUsed()),
        Hash: block.Hash(),
        LogsBloom: block.Bloom().Bytes(),
        Coinbase: block.Coinbase(),
        MixHash: block.MixDigest(),
        Nonce: types.EncodeNonce(block.Nonce()),
        Number: hexutil.Big(*block.Number()),
        ParentHash: block.ParentHash(),
        ReceiptRoot: block.ReceiptHash(),
        Sha3Uncles: block.UncleHash(),
        Size: hexutil.Uint64(block.Size()),
        StateRoot: block.Root(),
        Timestamp: hexutil.Uint64(block.Time()),
        TotalDifficulty: hexutil.Big(*td),
        Transactions: block.Transactions(),
        TransactionsRoot: block.TxHash(),
        Uncles: make([]common.Hash, len(block.Uncles())),
      },
      logs: logs,
      receiptMeta: make(map[common.Hash]*receiptMeta),
      Commit: func(*sql.Tx) (error) { return nil },
    }
    for _, receipt := range receipts {
      ce.receiptMeta[receipt.TxHash] = &receiptMeta{
        contractAddress: receipt.ContractAddress,
        cumulativeGasUsed: receipt.CumulativeGasUsed,
        gasUsed: receipt.GasUsed,
        logsBloom: receipt.Bloom,
        status: receipt.Status,
      }
    }
    ch <- ce
  }, func(ce interface{}) {
    feed.feed.Send(ce)
  })
  feed.ready <- struct{}{}
}
