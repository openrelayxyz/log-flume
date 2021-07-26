package datafeed

import (
  "context"
  "database/sql"
  "fmt"
  "math/big"
  "github.com/ethereum/go-ethereum/ethclient"
  "github.com/ethereum/go-ethereum/common"
  "github.com/ethereum/go-ethereum/common/hexutil"
  "github.com/ethereum/go-ethereum/core/types"
  "github.com/ethereum/go-ethereum/event"
  "github.com/ethereum/go-ethereum/rpc"
  "log"

  "sync"
  "sync/atomic"
)

// TODO:  On startup, ethwsfeed should pull from the last stored block to the
// most recent block, then switch over to a subscription.

// Each block that comes in from the subscription, check that the parent hash
// of the new block matches the last block, or that the new block has a lower
// block number than the last processed block. If neither is the case, find the
// common ancestor and remit intermediate blocks.

// For each block that needs to be emitted (whether discovered by the feed or
// pulling intermediate blocks), query for the full block (including
// transactions), then query for the transaction receipt of each transaction.

// Emit a message containing block information, transactions, and transaction
// receipts.

type ethWSFeed struct {
  urlStr string
  conn *ethclient.Client
  rpcConn *rpc.Client
  lastBlockHash common.Hash
  lastBlockNumber *big.Int
  lastBlockTime *atomic.Value
  ready chan struct{}
  db *sql.DB
  feed event.Feed
  quit chan struct{}
  started bool
}

func NewETHWSFeed(urlStr string, db *sql.DB) (DataFeed, error) {
  var resumeBlock int64
  var blockHash []byte
  db.QueryRowContext(context.Background(), "SELECT max(number) FROM blocks;").Scan(&resumeBlock)
  db.QueryRowContext(context.Background(), "SELECT DISTINCT blockHash FROM blocks WHERE blockNumber = ?;", resumeBlock).Scan(&blockHash)
  conn, err := ethclient.Dial(urlStr)
  if err != nil { return nil, err }
  rpcConn, err := rpc.Dial(urlStr)
  if err != nil { return nil, err }
  feed := &ethWSFeed{
    urlStr: urlStr,
    lastBlockTime: &atomic.Value{},
    ready: make(chan struct{}),
    quit: make(chan struct{}),
    lastBlockNumber: big.NewInt(resumeBlock),
    lastBlockHash: common.BytesToHash(blockHash),
    conn: conn,
    rpcConn: rpcConn,
    db: db,
  }
  return feed, nil
}

func (feed *ethWSFeed) Close() {
  feed.quit <- struct{}{}
}

func (feed *ethWSFeed) getFromHeader(header *types.Header) (*ChainEvent, error) {
  ce := &ChainEvent{
    Block: &miniBlock{},
    receiptMeta: make(map[common.Hash]*receiptMeta),
    logs: make( map[common.Hash][]*types.Log),
    Commit: func(*sql.Tx) (error) { return nil },
  }
	err := feed.rpcConn.CallContext(context.Background(), &ce.Block, "eth_getBlockByNumber", hexutil.Big(*header.Number), true)
	if err != nil {
    log.Printf("Error getting block by number")
		return nil, err
	}
  // ce.Block, err = feed.rpcConn.BlockByNumber(context.Background(), header.Number)
  if err != nil { return nil, err }
  txhash := make(chan common.Hash, 10)
  txreceipts := make(chan *types.Receipt, 10)
  errch := make(chan error)
  go func() {
    for _, tx := range ce.Block.Transactions {
      txhash <- tx.Hash()
    }
    close(txhash)
  }()
  var wg sync.WaitGroup
  for i := 0; i < 10; i++ {
    wg.Add(1)
    go func(wg *sync.WaitGroup) {
      for hash := range txhash {
        receipt, err := feed.conn.TransactionReceipt(context.Background(), hash)
        if err != nil {
          log.Printf("Receipt: %v, Error: %v", receipt, err)
          errch <- err
          break
        }
        txreceipts <- receipt
      }
      wg.Done()
    }(&wg)
  }
  go func() {
    // Wait until all threads are done processing, then clean up.
    wg.Wait()
    errch <- nil
    close(txreceipts)
  }()
  TXRECEIPTS:
  for {
    select {
    case receipt := <-txreceipts:
      if receipt == nil {
        break TXRECEIPTS
      }
      ce.receiptMeta[receipt.TxHash] = &receiptMeta{
        contractAddress: receipt.ContractAddress,
        cumulativeGasUsed: receipt.CumulativeGasUsed,
        gasUsed: receipt.GasUsed,
        logsBloom: receipt.Bloom,
        status: receipt.Status,
      }
      ce.logs[receipt.TxHash] = receipt.Logs
    case err := <- errch:
      if err == nil { break }
      return nil, err
    }
  }
  return ce, nil
}

// Connection re-establishes itself, but subscriptions may not. We'll need to
// watch the subscription error channels and have them reconstruct themselves.
func (feed *ethWSFeed) subscribe() {
  log.Printf("Setting up websocket subscription %v", feed.urlStr)
  header, err := feed.conn.HeaderByNumber(context.Background(), feed.lastBlockNumber)
  if err != nil { panic(err.Error()) }
  tx, err := feed.db.BeginTx(context.Background(), nil)
  if err != nil { panic(err.Error()) }
  rolledBack := 0
  for feed.lastBlockNumber.Cmp(big.NewInt(0)) > 0 && header.Hash() != feed.lastBlockHash {
    tx.Exec("DELETE FROM blocks WHERE hash = ?;", feed.lastBlockHash)
    rolledBack++
    if rolledBack > 1000 {
      panic("Cannot find matching blockhash between DB and node")
    }
    feed.lastBlockNumber.Sub(feed.lastBlockNumber, big.NewInt(1))
    var blockHash []byte
    feed.db.QueryRowContext(context.Background(), "SELECT DISTINCT hash FROM blocks WHERE number = ?;", feed.lastBlockNumber.Int64()).Scan(&blockHash)
    feed.lastBlockHash = common.BytesToHash(blockHash)
    header, err = feed.conn.HeaderByNumber(context.Background(), feed.lastBlockNumber)
    if err != nil { panic(err.Error()) }
  }
  if rolledBack > 0 {
    tx.Commit() // Roll back until the database matches the server
  } else {
    tx.Rollback()
  }
  OrderedProcessor(header.Number.Uint64(), 10, func(number uint64, ch chan<- interface{}, quit func()) {
    i := 0
    for ; i < 3; i++ {
      header, err := feed.conn.HeaderByNumber(context.Background(), big.NewInt(int64(number + 1)))
      if header != nil {
        j := 0
        for ; j < 3; j++{
          if ce, err := feed.getFromHeader(header); err == nil {
            ch <- ce
            return
          } else {
            log.Printf("Error getting header: %v", err.Error())
          }
        }
        if j == 3 {
          quit()
          panic(fmt.Sprintf("Failed to get chain event from headers for block %v: %v", header.Number.Uint64(), err))
        }
      }
    }
    if i == 3 {
      quit()
      // panic("Failed to get header")
    }
  }, func(ce interface{}) {
    feed.feed.Send(ce)
    feed.lastBlockHash = ce.(*ChainEvent).Block.Hash
    feed.lastBlockNumber = ce.(*ChainEvent).Block.Number.ToInt()
  })
  // At this point we've emitted all chain events from where the DB left off up
  // through the latest the node has. Now we switch over to subscriptions.

  feed.ready <- struct{}{}
  subch := make(chan *types.Header)

  for {
    sub, err := feed.conn.SubscribeNewHead(context.Background(), subch)
    if err != nil { panic(err.Error()) }
    READ_LOOP:
    for {
      select {
      case header := <-subch :
        if header.ParentHash != feed.lastBlockHash && header.Number.Cmp(feed.lastBlockNumber) > 0 {
          // This block is not a child of the last block, and not a sibling or
          // uncle. Something is wrong.
          panic("Blocks missing from feed")
        }
        i := 0
        for ; i < 3; i++{
          // Try 3 times to emit the header
          if ce, err := feed.getFromHeader(header); err == nil {
            go feed.feed.Send(ce)
            break
          }
        }
        if i == 3 { panic("Could not emit header") }
        feed.lastBlockHash = header.Hash()
        feed.lastBlockNumber = header.Number
      case <-sub.Err():
        break READ_LOOP
      case <-feed.quit:
        return
      }
    }
  }
}

func (feed *ethWSFeed) Ready() <-chan struct{} {
  return feed.ready
}

func (feed *ethWSFeed) Subscribe(ch chan *ChainEvent) event.Subscription {
  sub := feed.feed.Subscribe(ch)
  if !feed.started {
    go feed.subscribe()
    feed.started = true
  }
  return sub
}
