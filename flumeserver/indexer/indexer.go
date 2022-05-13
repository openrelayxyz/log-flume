package indexer

import (
  "fmt"
  "strings"
  "bytes"
  "context"
	"math/big"
  "github.com/openrelayxyz/flume/flumeserver/datafeed"
  "github.com/openrelayxyz/flume/flumeserver/txfeed"
  "database/sql"
  "github.com/ethereum/go-ethereum/core/types"
  "github.com/ethereum/go-ethereum/common"
  "github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/openrelayxyz/cardinal-streams/transports"
	"github.com/openrelayxyz/cardinal-streams/delivery"
  "log"
  "time"
  "github.com/klauspost/compress/zlib"
  "sync"
	"strconv"
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
		case *common.Address:
			if value == nil {
				preparedParams[i] = "NULL"
				continue
			}
			b := trimPrefix(value.Bytes())
			if len(b) == 0 {
				preparedParams[i] = "NULL"
			} else {
				preparedParams[i] = fmt.Sprintf("X'%x'", b)
			}
		case *big.Int:
			if value == nil {
				preparedParams[i] = "NULL"
				continue
			}
      b := trimPrefix(value.Bytes())
      if len(b) == 0 {
        preparedParams[i] = "NULL"
      } else {
        preparedParams[i] = fmt.Sprintf("X'%x'", b)
      }
    case bytesable:
			if value == nil {
				preparedParams[i] = "NULL"
				continue
			}
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

func ProcessDataFeed(feed datafeed.DataFeed, csConsumer transports.Consumer, txFeed *txfeed.TxFeed, db *sql.DB, quit <-chan struct{}, eip155Block, homesteadBlock uint64, mut *sync.RWMutex, mempoolSlots int, indexers []Indexer) {
	log.Printf("Processing data feed")
  txCh := make(chan *types.Transaction, 200)
  txSub := txFeed.Subscribe(txCh)
  ch := make(chan *datafeed.ChainEvent, 10)
  sub := feed.Subscribe(ch)
  csCh := make(chan *delivery.ChainUpdate, 10)
  if csConsumer != nil {
    csSub := csConsumer.Subscribe(csCh)
    defer csSub.Unsubscribe()
  }
  processed := false
  pruneTicker := time.NewTicker(5 * time.Second)
  txCount := 0
  txDedup := make(map[common.Hash]struct{})
  defer sub.Unsubscribe()
  defer txSub.Unsubscribe()
	db.Exec("DELETE FROM mempool.transactions WHERE 1;")
	for {
		select {
	case <-quit:
		if !processed {
			panic("Shutting down without processing any blocks")
			//try not to panic, pass back into main and do proper cleanup
		}
		log.Printf("Shutting down index process")
		return
	case <- pruneTicker.C:
		mempool_indexer1(db, mempoolSlots, processed, txCount, txDedup)
	case tx := <-txCh:
		mempool_indexer2(db, mempoolSlots, processed, txCount, txDedup, tx)
	case chainEvent := <- ch:
		lg := legacy_indexer(db, eip155Block, homesteadBlock, mut, chainEvent, processed)
		if lg != nil { fmt.Println(lg) }
	case chainUpdate := <-csCh:
      //UPDATELOOP:
      var lastBatch *delivery.PendingBatch
      for {
        statements := []string{}
        for _, pb := range chainUpdate.Added() {
          for _, indexer := range indexers {
            s, err := indexer.Index(pb)
            if err != nil {
              log.Printf("Error computing updates")
              continue
            }
            statements  = append(statements, s...)
          }
          lastBatch = pb

					resumption := pb.Resumption()
					tokens := strings.Split(resumption, ";")
					for _, token := range tokens {
						parts := strings.Split(token, "=")
						source, offsetS := parts[0], parts[1]
						parts = strings.Split(source, ":")
						topic, partitionS := parts[0], parts[1]
						offset, err := strconv.Atoi(offsetS)
						if err != nil {
							log.Printf(err.Error())
							continue
						}
						partition, err := strconv.Atoi(partitionS)
						if err != nil {
							log.Printf( err.Error())
							continue
						}

						statements = append(statements, applyParameters(
		          ("INSERT OR REPLACE INTO cardinal_offsets(offset, partition, topic) VALUES (?, ?, ?)"),
		          offset, partition, topic,
		        ))
				}

        mut.Lock()
        start := time.Now()
        dbtx, err := db.BeginTx(context.Background(), nil)

        if err != nil { log.Fatalf("Error creating a transaction: %v", err.Error())}
        if _, err := dbtx.Exec(strings.Join(statements, " ; ")); err != nil {
          dbtx.Rollback()
          stats := db.Stats()
          log.Printf("WARN: Failed to insert logs: %v", err.Error())
          log.Printf("SQLite Pool - Open: %v InUse: %v Idle: %v", stats.OpenConnections, stats.InUse, stats.Idle)
          mut.Unlock()
          continue
        }
        // log.Printf("Spent %v on %v inserts", time.Since(istart), len(statements))
        // cstart := time.Now()
        if err := dbtx.Commit(); err != nil {
          stats := db.Stats()
          log.Printf("WARN: Failed to insert logs: %v", err.Error())
          log.Printf("SQLite Pool - Open: %v InUse: %v Idle: %v", stats.OpenConnections, stats.InUse, stats.Idle)
          mut.Unlock()
          continue
        }
        mut.Unlock()
        processed = true
        // completionFeed.Send(chainEvent.Block.Hash)
        // log.Printf("Spent %v on commit", time.Since(cstart))
        log.Printf("Committed Block %v (%#x) in %v (age ??)", uint64(lastBatch.Number), lastBatch.Hash.Bytes(), time.Since(start)) // TODO: Figure out a simple way to get age
      }
		//TODO: checkhere for processed
}
}
}
}
