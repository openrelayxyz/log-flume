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
	// "github.com/ethereum/go-ethereum/common/hexutil"
	// "github.com/ethereum/go-ethereum/rpc"
	// "github.com/google/go-cmp/cmp"
	// "net/http"
	"path/filepath"
	"encoding/json"
	// "flag"
	// "fmt"
	// "time"
	// "log"
	// "io/ioutil"
	"github.com/mattn/go-sqlite3"
	_ "net/http/pprof"
	"database/sql"
	"sync"
)

var farewell string = "goodbuy horses"

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


//TODO: make unique test files for each module


// func TestBlockNumber(t *testing.T) {
// 	db, err := connectToDatabase()
// 	if err != nil {
// 		t.Fatal(err.Error())
// 	}
// 	defer db.Close()
// 	b := NewBlockAPI(db, 1)
// 	expectedResult, _ := hexutil.DecodeUint64("0xd59f80")
// 	test , err:= b.BlockNumber(context.Background())
// 	if err != nil {
// 		t.Fatalf(err.Error())
// 	}
// 	if test != hexutil.Uint64(expectedResult) {
// 		t.Fatalf("BlockNumber() result not accurate")
// 	}
// }
//
// func TestBlockAPI(t *testing.T) {
// 		db, err := connectToDatabase()
// 		if err != nil {
// 			t.Fatal(err.Error())
// 		}
// 		defer db.Close()
// 		b := NewBlockAPI(db, 1)
// 		var blocksMap []map[string]json.RawMessage
// 		raw, _ := jsonDecompress("data.json.gz")
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

func getTransactionsForTesting(blocksMap []map[string]json.RawMessage) []map[string]json.RawMessage {
	result := []map[string]json.RawMessage{}
	for i, _ := range blockNumbers[4:] {
		txns := []map[string]json.RawMessage{}
		json.Unmarshal(blocksMap[i + 4]["transactions"], &txns)
		result = append(result, txns...)
	}
	return result
}

func getTransactionHashes(blocksMap []map[string]json.RawMessage) []common.Hash {
	result := []common.Hash{}
	for i, _ := range blockNumbers[4:] {
		txnLevel := []map[string]interface{}{}
		json.Unmarshal(blocksMap[i + 4]["transactions"], &txnLevel)
		if len(txnLevel) > 0 {
			for _, tx := range txnLevel {
				result = append(result, common.HexToHash(tx["hash"].(string)))
			}
		}
	}
	return result
}

func TestTransactionAPI(t *testing.T) {
		db, err := connectToDatabase()
		if err != nil {
			t.Fatal(err.Error())
		}
		defer db.Close()
		tx := NewTransactionAPI(db, 1)
		var blocksMap []map[string]json.RawMessage
		raw, _ := jsonDecompress("data.json.gz")
		json.Unmarshal(raw, &blocksMap)
		transactions := getTransactionsForTesting(blocksMap)
		hashes := getTransactionHashes(blocksMap)

		if len(transactions) != len(hashes) {
			t.Fatalf("lists of different lengths")
		}
		for i, hash := range hashes {
			actual, _ := tx.GetTransactionByHash(context.Background(), hash)
			for k, v := range actual {
								data, err := json.Marshal(v)
								if err != nil {t.Errorf("json marshalling %v", k)}
								if !bytes.Equal(data, transactions[i][k]) {
									var x interface{}
									json.Unmarshal(transactions[i][k], &x)
									t.Fatalf("values are different %v %v %v '%v' %v", i, k, v, x, hash)
							}
					}
		}

}
		// for i, hash := range hashes {
		// 	t.Run("GetTransactionByHash", func(t *testing.T){
		// 		actual, _ := tx.GetTransactionByHash(context.Background(), hash)
		//
		// 		if actual.Hash != transactions[i].Hash {
		// 			t.Fatalf("transaction hash error %v %v %v %v", actual.BlockNumber, actual.TransactionIndex, transactions[i].BlockNumber, transactions[i].TransactionIndex)
		// 		}
		// 	})
// }
	// 		t.Run("GetBlockTransactionCountByNumber", func(t *testing.T){
	// 			actual, err := b.GetBlockTransactionCountByNumber(context.Background(), block)
	// 				if err != nil {
	// 					t.Fatal(err.Error())
	// 				}
	// 				var txSlice []*rpcTransaction
	// 				json.Unmarshal(blocksMap[i + 4]["transactions"], &txSlice)
	// 				if actual != hexutil.Uint64(len(txSlice)) {
	// 					t.Fatalf("transaction count by block %v %v", actual, hexutil.Uint64(len(txSlice)))
	// 				}
	// 		})
	// 		t.Run("GetUncleCountByBlockNumber", func(T *testing.T){
	// 			actual, err := b.GetUncleCountByBlockNumber(context.Background(), block)
	// 				if err != nil {
	// 					t.Fatal(err.Error())
	// 				}
	// 				var uncleSlice []common.Hash
	// 				json.Unmarshal(blocksMap[i + 4]["uncles"], &uncleSlice)
	// 				if actual != hexutil.Uint64(len(uncleSlice)) {
	// 					t.Fatalf("uncle count by block %v %v", actual, hexutil.Uint64(len(uncleSlice)))
	// 				}
	// 		})
	// }
	// for i, hash := range blockHashes[4:] {
	// 	t.Run("GetBlockByHash", func(T *testing.T){
	// 		actual, err := b.GetBlockByHash(context.Background(), common.HexToHash(hash), true)
	// 			if err != nil {
	// 				t.Fatal(err.Error())
	// 			}
	// 			for k, v := range actual {
	// 				data, err := json.Marshal(v)
	// 				if err != nil {
	// 					t.Errorf("nope %v", k)
	// 				}
	// 				if !bytes.Equal(data, blocksMap[i + 4][k]) {
	// 					t.Fatalf("not equal %v", k)
	// 			}
	// 	}
	// 	})
	// 	t.Run("GetBlockTransactionCountByHash", func(T *testing.T){
	// 		actual, err := b.GetBlockTransactionCountByHash(context.Background(), common.HexToHash(hash))
	// 			if err != nil {
	// 				t.Fatal(err.Error())
	// 			}
	// 			var txSlice []*rpcTransaction
	// 			json.Unmarshal(blocksMap[i + 4]["transactions"], &txSlice)
	// 			if actual != hexutil.Uint64(len(txSlice)) {
	// 				t.Fatalf("transaction count by hash %v %v", actual, hexutil.Uint64(len(txSlice)))
	// 			}
	// 	})
	// 	t.Run("GetUncleCountByBlockHash", func(T *testing.T){
	// 		actual, err := b.GetUncleCountByBlockHash(context.Background(), common.HexToHash(hash))
	// 			if err != nil {
	// 				t.Fatal(err.Error())
	// 			}
	// 			var uncleSlice []common.Hash
	// 			json.Unmarshal(blocksMap[i + 4]["uncles"], &uncleSlice)
	// 			if actual != hexutil.Uint64(len(uncleSlice)) {
	// 				t.Fatalf("uncle count by hash %v %v", actual, hexutil.Uint64(len(uncleSlice)))
	// 			}
	// 	})



// t.Errorf("%v %v", reflect.TypeOf(actual["gasUsed"]), reflect.TypeOf(flume))






// func TestGetBlockByNumber(t *testing.T) {
// 	db, err := connectToDatabase()
// 	if err != nil {
// 		t.Fatal(err.Error())
// 	}
// 	defer db.Close()
// 	b := NewBlockAPI(db, 1)
// 	// actual , err := b.GetBlockByHash(context.Background(), hash, true)
// 	// if err != nil {
// 	// 	t.Fatal(err.Error())
// 	// }
// 	// actualBytes, _ := json.Marshal(actual)
// 	// if string(actualBytes) != gBBNExpectedResults[0] {
// 	// 	t.Fatal(string(actualBytes), gBBNExpectedResults[0])
// 	// }
// 	for i, block := range blockNumbers {
//     t.Run("GetBlockByNumber", func(T *testing.T) {
// 			actual , err := b.GetBlockByNumber(context.Background(), block, true)
// 			if err != nil {
// 				t.Fatal(err.Error())
// 			}
// 			actualBytes, _ := json.Marshal(actual)
//       if string(actualBytes) != gBBNExpectedResults[i] {
//         t.Errorf("Error with block %v", i)
//       }
//     })
//   }
// 	for i, hash := range blockHashes {
// 		t.Run("GetBlockByHash", func(T *testing.T) {
// 			actual , err := b.GetBlockByHash(context.Background(), common.HexToHash(hash), true)
// 			if err != nil {
// 				t.Fatal(err.Error())
// 			}
// 			actualBytes, _ := json.Marshal(actual)
// 			if string(actualBytes) != gBBNExpectedResults[i] {
// 				t.Errorf("Error with block %v", i)
// 			}
// 		})
// 	}
// }
// func TestGetBlockTransactionCount(t *testing.T) {
// 	db, err := connectToDatabase()
// 	if err != nil {
// 		t.Fatal(err.Error())
// 	}
// 	defer db.Close()
// 	b := NewBlockAPI(db, 1)
// 	for i, block := range blockNumbers {
//     t.Run("GetBlockTransactionCountByNumber", func(T *testing.T) {
// 			actual , err := b.GetBlockTransactionCountByNumber(context.Background(), block)
// 			if err != nil {
// 				t.Fatal(err.Error())
// 			}
//       if actual != txCountResults[i] {
//         t.Errorf("Error with block %v", i)
//       }
//     })
//   }
// 	for i, hash := range blockHashes {
//     t.Run("GetBlockTransactionCountByHash", func(T *testing.T) {
// 			actual , err := b.GetBlockTransactionCountByHash(context.Background(), common.HexToHash(hash))
// 			if err != nil {
// 				t.Fatal(err.Error())
// 			}
//       if actual != txCountResults[i] {
//         t.Errorf("Error with block %v", i)
//       }
//     })
//   }
// }
//
// func TestGetUncleCount(t *testing.T) {
// 	db, err := connectToDatabase()
// 	if err != nil {
// 		t.Fatal(err.Error())
// 	}
// 	defer db.Close()
// 	b := NewBlockAPI(db, 1)
// 	for i, block := range blockNumbers {
//     t.Run("GetUncleCountByBlockNumber", func(T *testing.T) {
// 			actual , err := b.GetUncleCountByBlockNumber(context.Background(), block)
// 			if err != nil {
// 				t.Fatal(err.Error())
// 			}
//       if actual != uncleCountResults[i] {
//         t.Errorf("Error with block %v", i)
//       }
//     })
//   }
// 	for i, hash := range blockHashes {
//     t.Run("GetUncleCountByBlockHash", func(T *testing.T) {
// 			actual , err := b.GetUncleCountByBlockHash(context.Background(), common.HexToHash(hash))
// 			if err != nil {
// 				t.Fatal(err.Error())
// 			}
//       if actual != uncleCountResults[i] {
//         t.Errorf("Error with block %v", i)
//       }
//     })
//   }
// }
//
// func TestGetTransactions(t *testing.T) {
// 	db, err := connectToDatabase()
// 	if err != nil {
// 		t.Fatal(err.Error())
// 	}
// 	defer db.Close()
// 	tx := NewTransactionAPI(db, 1)
// 	// b := NewBlockAPI(db, 1)
//
// 	for i, block := range blockNumbers {
//     t.Run("GetTransactionByBlockNumberAndIndex", func(T *testing.T) {
// 			blockMap := Block{}
// 			json.Unmarshal([]byte(gBBNExpectedResults[i]), &blockMap)
// 			txns := blockMap.Transactions
// 			for j, _ := range txns {
// 				actual , err := tx.GetTransactionByBlockNumberAndIndex(context.Background(), block, hexutil.Uint64(j))
// 				if err != nil {
// 					t.Fatal(err.Error())
// 			}
// 			if actual != txns[j] {
// 				t.Errorf("Error with block %v %v %v %v", actual, txns[j], block, j)
// 			}
//       }
//     })
//   }
//
// 	// for i, hash := range blockHashes {
//   //   t.Run("GetUncleCountByBlockHash", func(T *testing.T) {
// 	// 		actual , err := b.GetUncleCountByBlockHash(context.Background(), common.HexToHash(hash))
// 	// 		if err != nil {
// 	// 			t.Fatal(err.Error())
// 	// 		}
//   //     if actual != uncleCountResults[i] {
//   //       t.Errorf("Error with block %v", i)
//   //     }
//   //   })
//   // }
// }
//
// // GetTransactionByBlockNumberAndIndex
//
// func TestGasPrice(t *testing.T) {
// 	db, err := connectToDatabase()
// 	if err != nil {
// 		t.Fatal(err.Error())
// 	}
// 	defer db.Close()
// 	g := NewGasAPI(db, 1)
// 	expectedResult  := "0x1f47a69b13"
// 	test , err:= g.GasPrice(context.Background())
// 	if err != nil {
// 		t.Fatalf(err.Error())
// 	}
// 	if test != expectedResult {
// 		t.Fatalf("GasPrice() result not accurate")
// 	}
// }

// func TestMaxPriorityFeePerGas(t *testing.T) {
// 	db, err := connectToDatabase()
// 	if err != nil {
// 		t.Fatal(err.Error())
// 	}
// 	defer db.Close()
// 	g := NewGasAPI(db, 1)
// 	expectedResult  := "0xa84b504a"
// 	test , err:= g.MaxPriorityFeePerGas(context.Background())
// 	if err != nil {
// 		t.Fatalf(err.Error())
// 	}
// 	if test != expectedResult {
// 		t.Fatalf("GasPrice() result not accurate")
// 	}
// }
