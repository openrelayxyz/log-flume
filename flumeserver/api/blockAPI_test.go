package api

import (
	// "reflect"
	"testing"
	"context"
	"fmt"
	"bytes"
	// "cmp"

	// "os"
	// "github.com/openrelayxyz/cardinal-rpc/transports"
	// "github.com/openrelayxyz/flume/flumeserver/txfeed"
	// "github.com/openrelayxyz/flume/flumeserver/datafeed"
	// "github.com/openrelayxyz/flume/flumeserver/indexer"
	// "github.com/openrelayxyz/flume/flumeserver/migrations"
	// "github.com/openrelayxyz/flume/flumeserver/notify"
	// "github.com/openrelayxyz/flume/flumeserver/api"
	// gethLog "github.com/ethereum/go-ethereum/log"
	// "github.com/ethereum/go-ethereum/event"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/rpc"
	// "github.com/google/go-cmp/cmp"
	// "net/http"'https://2ebb6d9d2bbb42a3a9b1cf4d129e9609.eth.rpc.rivet.cloud/', json=d).json()
	"path/filepath"
	"encoding/json"
	// "flag"
	// "fmt"
	// "time"
	// "log"
	"io"
	"compress/gzip"
	"io/ioutil"
	"github.com/mattn/go-sqlite3"
	_ "net/http/pprof"
	"database/sql"
	"sync"
)

var register sync.Once

func connectToDatabase() (*sql.DB, error) {
  sqlitePath := "../../testdata.sqlite"

  mempoolDb := filepath.Join(filepath.Dir(sqlitePath), "mempool.sqlite")

  register.Do(func () {sql.Register("sqlite3_hooked",
    &sqlite3.SQLiteDriver{
      ConnectHook: func(conn *sqlite3.SQLiteConn) error {
        conn.Exec(fmt.Sprintf("ATTACH DATABASE '%v' AS 'mempool'; PRAGMA mempool.journal_mode = WAL ; PRAGMA mempool.synchronous = OFF ;", mempoolDb), nil)
        return nil
      },
  })})

  logsdb, err := sql.Open("sqlite3_hooked", fmt.Sprintf("file:%v?_sync=0&_journal_mode=WAL&_foreign_keys=on", sqlitePath))
	//we should add migrations process
	if err != nil {
		return nil, err
	}
	return logsdb, nil
}

func jsonDecompress() ([]map[string]json.RawMessage, error) {
	file, _ := ioutil.ReadFile("data2.json.gz")
  r, err := gzip.NewReader(bytes.NewReader(file))
	if err != nil {return nil, err }
  raw, _ := ioutil.ReadAll(r)
  if err == io.EOF || err == io.ErrUnexpectedEOF {
    return nil, err
  }

	var blocksMap []map[string]json.RawMessage
	json.Unmarshal(raw, &blocksMap)

  return blocksMap, nil
}

func receiptsDecompress() ([]map[string]json.RawMessage, error) {
	file, _ := ioutil.ReadFile("receipts.json.gz")
  r, err := gzip.NewReader(bytes.NewReader(file))
	if err != nil {return nil, err }
  raw, _ := ioutil.ReadAll(r)
  if err == io.EOF || err == io.ErrUnexpectedEOF {
    return nil, err
  }

	var receiptsMap []map[string]json.RawMessage
	json.Unmarshal(raw, &receiptsMap)

  return receiptsMap, nil
}

func getBlockNumbers() []rpc.BlockNumber {
	blocksMap, _ := jsonDecompress()
	result := []rpc.BlockNumber{}
	for _, block := range blocksMap {
		var x rpc.BlockNumber
		json.Unmarshal(block["number"], &x)
		result = append(result, x)
	}
	return result
}

func getBlockHashes() []common.Hash {
	blocksMap, _ := jsonDecompress()
	result := []common.Hash{}
	for _, block := range blocksMap {
		var x common.Hash
		json.Unmarshal(block["hash"], &x)
		result = append(result, x)
	}
	return result
}

func TestBlockNumber(t *testing.T) {
	db, err := connectToDatabase()
	if err != nil {
		t.Fatal(err.Error())
	}
	defer db.Close()
	b := NewBlockAPI(db, 1)
	expectedResult, _ := hexutil.DecodeUint64("0xd59f80")
	test , err:= b.BlockNumber(context.Background())
	if err != nil {
		t.Fatalf(err.Error())
	}
	if test != hexutil.Uint64(expectedResult) {
		t.Fatalf("BlockNumber() result not accurate")
	}
}
//
func TestBlockAPI(t *testing.T) {
		db, err := connectToDatabase()
		if err != nil {
			t.Fatal(err.Error())
		}
		defer db.Close()
		b := NewBlockAPI(db, 1)
		blocksMap, _ := jsonDecompress()
		// blockNumbers := getBlockNumbers()
		for i, block := range blocksMap {
			var n rpc.BlockNumber
			json.Unmarshal(block["number"], &n)
			actual, err := b.GetBlockByNumber(context.Background(), n, true)
					if err != nil {
						t.Fatal(err.Error())
					}
					for k, v := range actual {
						if k == "transactions" {
							continue
						} else {
						data, err := json.Marshal(v)
						if err != nil {
							t.Errorf("nope %v", k)
						}
						if !bytes.Equal(data, block[k]) {
							var x interface{}
							var y interface{}
							json.Unmarshal(block[k], &x)
							json.Unmarshal(data, &y)
							t.Fatalf("block values are different %v, %v, %v '%v'", i, len(blocksMap), string(data), string(block[k]))
					}
				}
			}
		}
}
// func TestBlockAPI(t *testing.T) {
// 		db, err := connectToDatabase()
// 		if err != nil {
// 			t.Fatal(err.Error())
// 		}
// 		defer db.Close()
// 		b := NewBlockAPI(db, 1)
// 		var blocksMap []map[string]json.RawMessage
// 		// raw, _ := jsonDecompress("data.json.gz")
// 		raw, _ := testingJson("data.json")
// 		json.Unmarshal(raw, &blocksMap)
// 		for i, block := range blockNumbers[4:] {
// 			t.Run(fmt.Sprintf("GetBlockByNumber %v", i), func(t *testing.T){
// 				actual, err := b.GetBlockByNumber(context.Background(), block, true)
// 					if err != nil {
// 						t.Fatal(err.Error())
// 					}
// 					for k, v := range actual {
// 						data, err := json.Marshal(v)
// 						if err != nil {
// 							t.Errorf("nope %v", k)
// 						}
// 						if !bytes.Equal(data, blocksMap[i + 4][k]) {
// 							t.Fatalf("not equal %v", k)
// 					}
// 			}
// 			})
// 			t.Run("GetBlockTransactionCountByNumber", func(t *testing.T){
// 				actual, err := b.GetBlockTransactionCountByNumber(context.Background(), block)
// 					if err != nil {
// 						t.Fatal(err.Error())
// 					}
// 					var txSlice []*rpcTransaction
// 					json.Unmarshal(blocksMap[i + 4]["transactions"], &txSlice)
// 					if actual != hexutil.Uint64(len(txSlice)) {
// 						t.Fatalf("transaction count by block %v %v", actual, hexutil.Uint64(len(txSlice)))
// 					}
// 			})
// 			t.Run("GetUncleCountByBlockNumber", func(t *testing.T){
// 				actual, err := b.GetUncleCountByBlockNumber(context.Background(), block)
// 					if err != nil {
// 						t.Fatal(err.Error())
// 					}
// 					var uncleSlice []common.Hash
// 					json.Unmarshal(blocksMap[i + 4]["uncles"], &uncleSlice)
// 					if actual != hexutil.Uint64(len(uncleSlice)) {
// 						t.Fatalf("uncle count by block %v %v", actual, hexutil.Uint64(len(uncleSlice)))
// 					}
// 			})
// 	}
// 	for i, hash := range blockHashes[4:] {
// 		t.Run("GetBlockByHash", func(t *testing.T){
// 			actual, err := b.GetBlockByHash(context.Background(), common.HexToHash(hash), true)
// 				if err != nil {
// 					t.Fatal(err.Error())
// 				}
// 				for k, v := range actual {
// 					data, err := json.Marshal(v)
// 					if err != nil {
// 						t.Errorf("nope %v", k)
// 					}
// 					if !bytes.Equal(data, blocksMap[i + 4][k]) {
// 						t.Fatalf("not equal %v", k)
// 				}
// 		}
// 		})
// 		t.Run("GetBlockTransactionCountByHash", func(t *testing.T){
// 			actual, err := b.GetBlockTransactionCountByHash(context.Background(), common.HexToHash(hash))
// 				if err != nil {
// 					t.Fatal(err.Error())
// 				}
// 				var txSlice []*rpcTransaction
// 				json.Unmarshal(blocksMap[i + 4]["transactions"], &txSlice)
// 				if actual != hexutil.Uint64(len(txSlice)) {
// 					t.Fatalf("transaction count by hash %v %v", actual, hexutil.Uint64(len(txSlice)))
// 				}
// 		})
// 		t.Run("GetUncleCountByBlockHash", func(t *testing.T){
// 			actual, err := b.GetUncleCountByBlockHash(context.Background(), common.HexToHash(hash))
// 				if err != nil {
// 					t.Fatal(err.Error())
// 				}
// 				var uncleSlice []common.Hash
// 				json.Unmarshal(blocksMap[i + 4]["uncles"], &uncleSlice)
// 				if actual != hexutil.Uint64(len(uncleSlice)) {
// 					t.Fatalf("uncle count by hash %v %v", actual, hexutil.Uint64(len(uncleSlice)))
// 				}
// 		})
// }
// }
